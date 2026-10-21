"""Bounded classifier migration, independent of forward cursors and scans.

All callers hold the normal publisher lease. Only plugin state is writable.
Evidence and discovery progress commit together; no alternate delivery path.
"""

import json

from .blockers import CLASSIFIER_VERSION, BlockerEvidenceError, is_blocker
from .store import BindingError

CHUNK_SIZE = 256
TASKS_PER_PASS = 32


class BlockerRecovery:
    def __init__(self, store, board):
        self.store, self.board = store, board
        self.db = store._conn
        self.key = (board, CLASSIFIER_VERSION)

    def discover(self, native, path, activation, highwater, report):
        stat = path.stat()
        identity = json.dumps([str(path), stat.st_dev, stat.st_ino])
        cursor = self.store.get_cursor(self.board)
        if highwater < max(cursor, activation):
            raise BindingError("native event IDs rewound; blocker recovery frozen")
        native.execute("BEGIN")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            migration = self.db.execute(
                "SELECT * FROM blocker_migration WHERE board=? AND version=?", self.key
            ).fetchone()
            if migration is None:
                end = max(activation, min(cursor, highwater))
                self.db.execute(
                    "INSERT INTO blocker_migration VALUES (?,?,?,?,?,?)",
                    (*self.key, identity, end, activation, int(end == activation)),
                )
                position, end, done = activation, end, end == activation
            else:
                if migration["identity"] != identity or highwater < migration["highwater"]:
                    raise BindingError("native database identity changed; blocker recovery frozen")
                position, end, done = (
                    migration["position"],
                    migration["highwater"],
                    migration["done"],
                )
            if not done:
                rows = native.execute(
                    "SELECT * FROM task_events WHERE id>? AND id<=? ORDER BY id LIMIT ?",
                    (position, end, CHUNK_SIZE),
                ).fetchall()
                for row in rows:
                    if row["kind"] != "gave_up":
                        continue
                    state, detail = "pending", ""
                    try:
                        is_blocker(native, row)
                    except BlockerEvidenceError as exc:
                        state, detail = "quarantined", str(exc)
                        report.errors.append(
                            f"{row['task_id']}: blocker recovery quarantined: {exc}"
                        )
                    post = self.store.get_post(self.board, row["task_id"])
                    replay = row["id"] - 1
                    if post and post["thread_id"]:
                        replay = max(replay, post["last_event_id"])
                    self.db.execute(
                        "INSERT OR IGNORE INTO blocker_recovery VALUES (?,?,?,?,?,?,?,?,?)",
                        (
                            *self.key,
                            row["task_id"],
                            row["id"],
                            row["kind"],
                            row["payload"],
                            replay,
                            state,
                            detail,
                        ),
                    )
                position = rows[-1]["id"] if rows else end
                done = position >= end or len(rows) < CHUNK_SIZE
                self.db.execute(
                    "UPDATE blocker_migration SET position=?, done=? WHERE board=? AND version=?",
                    (position, int(done), *self.key),
                )
            self.db.execute("COMMIT")
            return bool(done)
        except BaseException:
            self.db.execute("ROLLBACK")
            raise
        finally:
            native.rollback()

    def pending(self, now=0):
        return self.db.execute(
            "SELECT r.task_id, MIN(r.replay_position) AS position FROM blocker_recovery r "
            "LEFT JOIN posts p ON p.board=r.board AND p.task_id=r.task_id "
            "WHERE r.board=? AND r.version=? AND r.state IN ('pending','deferred') "
            "AND COALESCE(p.backoff_until,0)<=? AND NOT EXISTS "
            "(SELECT 1 FROM blocker_recovery q WHERE q.board=r.board AND q.version=r.version "
            "AND q.task_id=r.task_id AND q.state='quarantined') GROUP BY r.task_id "
            "ORDER BY MIN(evidence_id) LIMIT ?",
            (*self.key, now, TASKS_PER_PASS),
        ).fetchall()

    def held_tasks(self):
        return {
            r[0]
            for r in self.db.execute(
                "SELECT DISTINCT task_id FROM blocker_recovery WHERE board=? AND version=? "
                "AND state IN ('pending', 'deferred', 'quarantined')",
                self.key,
            )
        }

    def quarantined(self):
        """Operator-visible quarantined obligations (D1/D2)."""
        return self.db.execute(
            "SELECT task_id, detail FROM blocker_recovery WHERE board=? AND version=? "
            "AND state='quarantined' ORDER BY evidence_id",
            self.key,
        ).fetchall()

    def quarantine_forward(self, task_id, evidence_id, kind, payload, detail):
        """Durably quarantine a contradictory/malformed forward-path blocker.

        A poison event in the first-block eligibility filter must not pin the
        board cursor or re-report the same error every pass (F2): record it
        here so `held_tasks` keeps the task out of the forward batch and the
        cursor can advance past the event.
        """
        self.db.execute(
            "INSERT INTO blocker_recovery VALUES (?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(board,version,task_id,evidence_id) DO UPDATE SET "
            "state='quarantined', detail=excluded.detail "
            "WHERE blocker_recovery.state IN ('pending','deferred')",
            (
                *self.key,
                task_id,
                evidence_id,
                kind,
                payload,
                evidence_id - 1,
                "quarantined",
                detail,
            ),
        )

    def defer(self, task_id, events):
        """Retain exact held originals before ordinary cursor progress.

        INSERT OR IGNORE preserves quarantine/suppression and completed receipts.
        A crash before cursor advance can repeat this bounded write harmlessly.
        Deferred rows are runnable only when the whole task has no quarantine.
        """
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.executemany(
                "INSERT OR IGNORE INTO blocker_recovery VALUES (?,?,?,?,?,?,?,?,?)",
                [
                    (
                        *self.key,
                        task_id,
                        r["id"],
                        r["kind"],
                        r["payload"],
                        r["id"] - 1,
                        "deferred",
                        "",
                    )
                    for r in events
                ],
            )
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def reconcile(self, task_id, detail="manual reconciliation"):
        """Explicit operator action: release a quarantined obligation (D2).

        Transitions the obligation to `suppressed` (row retained for audit) and
        advances the scan watermark past its evidence so the forward path never
        re-raises on it. Never auto-rearmed: outcome() only ever transitions
        `pending`/`deferred` rows, and nothing here clears an unknown create outcome or
        tombstone.
        """
        self.db.execute("BEGIN IMMEDIATE")
        try:
            row = self.db.execute(
                "SELECT MAX(evidence_id) FROM blocker_recovery WHERE board=? AND version=? "
                "AND task_id=? AND state='quarantined'",
                (*self.key, task_id),
            ).fetchone()
            if row[0] is None:
                self.db.execute("COMMIT")
                return
            self.db.execute(
                "UPDATE blocker_recovery SET state='suppressed', detail=? WHERE board=? "
                "AND version=? AND task_id=? AND state='quarantined'",
                (detail, *self.key, task_id),
            )
            self.store.checkpoint_scan(self.board, task_id, row[0])
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def outcome(self, task_id, state, detail=""):
        self.db.execute(
            "UPDATE blocker_recovery SET state=?, detail=? WHERE board=? AND version=? "
            "AND task_id=? AND state IN ('pending','deferred')",
            (state, detail, *self.key, task_id),
        )

    def checkpoint(self, task_id, position):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.execute(
                "UPDATE blocker_recovery SET replay_position=MAX(replay_position,?) "
                "WHERE board=? AND version=? AND task_id=? AND state IN ('pending','deferred')",
                (position, *self.key, task_id),
            )
            self.store.set_task_position(self.board, task_id, last_event_id=position)
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def replay(self, consumer, native, now, report):
        for obligation in self.pending(now):
            task_id, position = obligation["task_id"], obligation["position"]
            if not consumer._lease_guard():
                report.warnings.append("lease lost before blocker recovery")
                return
            # Revalidate retained originals, not a synthetic blocker or old block.
            originals = self.db.execute(
                "SELECT * FROM blocker_recovery WHERE board=? AND version=? AND task_id=? "
                "AND state='pending'",
                (*self.key, task_id),
            ).fetchall()
            try:
                for original in originals:
                    evidence = native.execute(
                        "SELECT * FROM task_events WHERE id=?", (original["evidence_id"],)
                    ).fetchone()
                    if (
                        evidence is None
                        or evidence["task_id"] != task_id
                        or evidence["kind"] != original["kind"]
                        or evidence["payload"] != original["payload"]
                    ):
                        raise BlockerEvidenceError("missing or changed retained blocker event")
                    is_blocker(native, evidence)
                if native.execute("SELECT 1 FROM tasks WHERE id=?", (task_id,)).fetchone() is None:
                    raise BlockerEvidenceError("missing retained task")
            except BlockerEvidenceError as exc:
                self.outcome(task_id, "quarantined", str(exc))
                report.errors.append(f"{task_id}: blocker recovery quarantined: {exc}")
                continue
            post = self.store.get_post(self.board, task_id)
            if post and post["thread_id"] and post["last_event_id"] > position:
                # Only an actually newer durable receipt may advance recovery.
                # Never write a replay start into the post before normal guards.
                position = post["last_event_id"]
                self.checkpoint(task_id, position)
            events = native.execute(
                "SELECT * FROM task_events WHERE task_id=? AND id>? ORDER BY id LIMIT ?",
                (task_id, position, CHUNK_SIZE),
            ).fetchall()
            end = events[-1]["id"] if events else position
            # Revalidate at most one chunk of retained held originals, including
            # missing tail evidence. Never replay a manually suppressed poison.
            retained = self.db.execute(
                "SELECT * FROM blocker_recovery WHERE board=? AND version=? "
                "AND task_id=? AND state='deferred' AND evidence_id>? "
                "ORDER BY evidence_id LIMIT ?",
                (*self.key, task_id, position, CHUNK_SIZE),
            ).fetchall()
            by_id = {r["id"]: r for r in events}
            if any(
                (r["evidence_id"] <= end or len(events) < CHUNK_SIZE)
                and (
                    r["evidence_id"] not in by_id
                    or by_id[r["evidence_id"]]["kind"] != r["kind"]
                    or by_id[r["evidence_id"]]["payload"] != r["payload"]
                )
                for r in retained
            ):
                self.outcome(task_id, "quarantined", "missing or changed retained forward event")
                report.errors.append(f"{task_id}: missing or changed retained forward event")
                continue
            suppressed = {
                r[0]
                for r in self.db.execute(
                    "SELECT evidence_id FROM blocker_recovery WHERE board=? AND version=? "
                    "AND task_id=? AND state='suppressed' AND evidence_id>? AND evidence_id<=?",
                    (*self.key, task_id, position, end),
                )
            }
            raw_count = len(events)
            events = [r for r in events if r["id"] not in suppressed]
            try:
                consumer._process_task(
                    native, task_id, events, end, now, report, recovery_start=position
                )
            except Exception as exc:
                report.errors.append(f"{task_id}: blocker recovery failed: {exc!r}")
                continue
            post = self.store.get_post(self.board, task_id)
            if post and post["state"] in ("tombstone", "dead_letter"):
                self.outcome(task_id, "suppressed", post["state"])
            elif post and post["pending_create_at"] is not None:
                self.outcome(task_id, "quarantined", "unknown create outcome; reconcile manually")
            else:
                current = self.db.execute(
                    "SELECT MIN(replay_position) FROM blocker_recovery WHERE board=? "
                    "AND version=? AND task_id=? AND state IN ('pending','deferred')",
                    (*self.key, task_id),
                ).fetchone()[0]
                if (
                    current is not None
                    and current >= end
                    and raw_count < CHUNK_SIZE
                    and ((post and post["thread_id"]) or not originals)
                ):
                    self.outcome(task_id, "complete")
