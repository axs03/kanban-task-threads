"""Installed-native producers -> real plugin consumer, isolated and offline.

Run with installed Hermes Python. Never spawn workers or deliver externally.
"""

import hashlib
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))


def deny(event, args):
    if event in {
        "socket.connect",
        "socket.bind",
        "socket.getaddrinfo",
        "socket.sendto",
        "socket.sendmsg",
        "socket.gethostbyname",
        "socket.gethostbyaddr",
        "subprocess.Popen",
        "os.system",
        "os.kill",
        "os.killpg",
    }:
        raise AssertionError("Unexpected external effect: " + event)


sys.addaudithook(deny)
from conftest import FakeTransport
from hermes_cli import kanban_db as kb
from hermes_cli import kanban_db_dispatch as dispatch

from kanban_task_threads.consumer import Consumer
from kanban_task_threads.store import StateStore


class NativeProducers(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=os.environ["HOME"])
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.path = self.root / "native.db"
        kb.init_db(self.path)
        self.db = sqlite3.connect(self.path)
        self.db.row_factory = sqlite3.Row
        self.addCleanup(self.db.close)
        self.store = StateStore(self.root / "state.db")
        self.addCleanup(self.store._conn.close)
        self.transport = FakeTransport()
        self.consumer = Consumer(
            self.path, self.store, self.transport, thread_creation="first_blocked"
        )
        self.consumer.run_once(now=100)
        for name in ("_fire_task_hook", "_fire_kanban_lifecycle_hook", "notify_task_updated"):
            hook = patch.object(kb, name)
            hook.start()
            self.addCleanup(hook.stop)

    def task(self, **kwargs):
        return kb.create_task(self.db, title="synthetic", workspace_kind="scratch", **kwargs)

    def kinds(self, tid):
        return [
            row[0]
            for row in self.db.execute(
                "SELECT kind FROM task_events WHERE task_id=? ORDER BY id", (tid,)
            )
        ]

    def status(self, tid):
        return self.db.execute("SELECT status FROM tasks WHERE id=?", (tid,)).fetchone()[0]

    def consume(self, expected):
        report = self.consumer.run_once(now=101)
        self.assertEqual(report.errors, [])
        self.assertEqual(set(report.opened), set(expected))
        self.consumer.run_once(now=102)
        self.assertEqual(len(self.transport.ops("open_thread")), len(expected))

    def test_initial_park_and_rapid_complete(self):
        tid = self.task(initial_status="blocked")
        self.assertIn("blocked", self.kinds(tid))
        self.assertEqual(self.status(tid), "blocked")
        self.assertTrue(kb.complete_task(self.db, tid, result="synthetic done"))
        self.consume([tid])
        self.assertEqual(self.status(tid), "done")

    def test_regular_block_dependency_rekind_and_in_place(self):
        typed, untyped, rekind, classified = [self.task() for _ in range(4)]
        self.assertTrue(kb.block_task(self.db, typed, kind="needs_input", reason="decision"))
        self.assertTrue(kb.block_task(self.db, untyped, reason="untyped"))
        self.assertTrue(kb.block_task(self.db, rekind, kind="dependency", reason="no parent"))
        dispatch._record_task_failure(
            self.db, classified, "spawn failed", outcome="spawn_failed", force_trip=True
        )
        self.assertTrue(
            kb.block_task(self.db, classified, kind="needs_input", reason="classification")
        )
        self.assertFalse(kb.block_task(self.db, classified, kind="needs_input"))
        self.assertFalse(kb.block_task(self.db, untyped, kind="needs_input", expected_run_id=999))
        self.consume([typed, untyped, rekind, classified])

    def test_dependency_wait_and_loop_escalation_do_not_provision(self):
        parent = self.task()
        child = self.task(parents=[parent])
        self.assertEqual(self.status(child), "todo")
        self.assertIn("dependency_wait", self.kinds(child))
        loop = self.task()
        for _ in range(10):
            if self.status(loop) == "triage":
                break
            kb.block_task(self.db, loop, kind="transient", reason="repeat")
            if self.status(loop) == "blocked":
                kb.unblock_task(self.db, loop)
        self.assertEqual(self.status(loop), "triage")
        self.assertIn("block_loop_detected", self.kinds(loop))
        # Previous true blockers DO qualify, while triage itself does not.
        self.consume([loop])
        self.assertIsNone(self.store.get_post("default", child))

    def test_shared_failure_writer_all_outcomes_force_overrides_and_review(self):
        expected = []
        for outcome in ("spawn_failed", "timed_out", "reclaimed", "crashed"):
            for phase in ("ready", "review"):
                for policy in ("threshold", "force", "task_override", "infrastructure", "below"):
                    override = (
                        99
                        if policy in ("force", "below")
                        else (1 if policy == "task_override" else None)
                    )
                    tid = self.task(max_retries=override)
                    if phase == "review":
                        with kb.write_txn(self.db):
                            self.db.execute("UPDATE tasks SET status='review' WHERE id=?", (tid,))
                            kb._append_event(self.db, tid, "review_requested", {})
                    trip = dispatch._record_task_failure(
                        self.db,
                        tid,
                        "synthetic " + outcome,
                        outcome=outcome,
                        failure_limit=99 if policy in ("below", "task_override") else 1,
                        force_trip=policy == "force",
                        infrastructure=policy == "infrastructure",
                        release_claim=False,
                        end_run=False,
                    )
                    should_trip = policy not in ("infrastructure", "below")
                    self.assertEqual(trip, should_trip)
                    self.assertEqual(self.status(tid), "blocked" if should_trip else phase)
                    self.assertEqual("gave_up" in self.kinds(tid), should_trip)
                    if should_trip:
                        expected.append(tid)
        self.consume(expected)

    def test_native_crash_accounting_systemic_override_protocol_and_terminal(self):
        expected = []
        details = []
        for override in (None, None, 99):
            tid = self.task(max_retries=override)
            dead = SimpleNamespace(
                error_text="identical synthetic failure",
                protocol_violation=False,
                terminal_provider=False,
            )
            details.append((tid, 999999, "synthetic", dead))
            if override is None:
                expected.append(tid)
        self.assertEqual(set(dispatch._account_crashes(self.db, details)), set(expected))
        terminal = self.task(max_retries=99)
        dead = SimpleNamespace(
            error_text="synthetic terminal failure",
            protocol_violation=False,
            terminal_provider=True,
        )
        self.assertEqual(
            dispatch._account_crashes(self.db, [(terminal, 999999, "synthetic", dead)]), [terminal]
        )
        expected.append(terminal)
        for count in (1, 2):
            tid = self.task(max_retries=2)
            for _ in range(count):
                self.assertIsNotNone(kb.claim_task(self.db, tid, claimer="synthetic"))
                with kb.write_txn(self.db):
                    kb._end_run(
                        self.db,
                        tid,
                        outcome="crashed",
                        status="crashed",
                        error="synthetic protocol violation",
                        metadata={"protocol_violation": True},
                    )
                    self.db.execute(
                        "UPDATE tasks SET status='ready', claim_lock=NULL, "
                        "claim_expires=NULL WHERE id=?",
                        (tid,),
                    )
                    kb._append_event(self.db, tid, "protocol_violation", {})
            dead = SimpleNamespace(
                error_text="synthetic protocol", protocol_violation=True, terminal_provider=False
            )
            result = dispatch._account_crashes(self.db, [(tid, 999999, "synthetic", dead)])
            self.assertEqual(result, [tid] if count == 2 else [])
            if result:
                expected.append(tid)
        self.consume(expected)

    def test_native_rowcount_race_emits_gave_up_without_blocking_fail_closed(self):
        tid = self.task()
        self.assertTrue(kb.complete_task(self.db, tid, result="finished before accounting"))
        self.assertTrue(
            dispatch._record_task_failure(
                self.db, tid, "late crash accounting", outcome="crashed", force_trip=True
            )
        )
        self.assertEqual(self.status(tid), "done")
        self.assertIn("gave_up", self.kinds(tid))
        report = self.consumer.run_once(now=101)
        self.assertIn("contradictory", str(report.errors))
        self.assertEqual(self.transport.calls, [])


if __name__ == "__main__":
    for module in (kb, dispatch):
        path = Path(module.__file__)
        print(
            json.dumps(
                {
                    "native_source": str(path),
                    "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                }
            ),
            flush=True,
        )
    unittest.main(verbosity=2)
