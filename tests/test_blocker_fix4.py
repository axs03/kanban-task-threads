"""Permanent fix4 behavior regressions from independent acceptance probes."""

import json
import sqlite3

import pytest
from conftest import add_event, insert_task
from test_blocker_coverage import breaker_payload
from test_blocker_recovery import ledger
from test_first_blocked import setup

from kanban_task_threads.consumer import Consumer
from kanban_task_threads.recovery import BlockerRecovery
from kanban_task_threads.store import StateStore


def test_quarantined_task_is_not_replayed_via_another_pending_obligation(tmp_path):
    board, store, transport, consumer = setup(tmp_path)
    insert_task(board, "t", status="blocked")
    add_event(board, "t", "completed")  # 1
    add_event(board, "t", "gave_up", breaker_payload())  # 2 contradictory
    add_event(board, "t", "status", {"status": "ready"})  # 3 valid new phase
    add_event(board, "t", "gave_up", breaker_payload())  # 4 genuine later blocker
    store.activate("nib", 1, [])
    store.advance_cursor("nib", old=0, new=4)
    store.checkpoint_scan("nib", "t", 4)
    report = consumer.run_once(now=100)
    observation = dict(
        errors=report.errors,
        ledger=ledger(store),
        held=sorted(BlockerRecovery(store, "nib").held_tasks()),
        opens=len(transport.ops("open_thread")),
        replies=transport.message_types,
    )
    print(json.dumps(observation))
    assert "contradictory" in str(report.errors)
    assert not transport.calls, (
        "An unresolved quarantine must wait for explicit reconciliation, "
        "even with another pending obligation"
    )


def test_backoff_front_of_queue_does_not_starve_other_recovery_tasks(tmp_path):
    board, store, transport, consumer = setup(tmp_path)
    for i in range(33):
        tid = f"t{i:02}"
        insert_task(board, tid, status="blocked")
        add_event(board, tid, "gave_up", breaker_payload())
        if i < 32:
            store.begin_create("nib", tid, now=1)
            store.clear_pending("nib", tid)
            store.set_backoff("nib", tid, until=1000000)
    store.activate("nib", 0, [])
    store.advance_cursor("nib", old=0, new=33)
    for i in range(33):
        store.checkpoint_scan("nib", f"t{i:02}", 33)
    for now in range(100, 105):
        consumer.run_once(now=now)
    print(
        json.dumps(
            dict(
                opens=len(transport.ops("open_thread")),
                last_post=store.get_post("nib", "t32"),
                ledger=ledger(store),
            )
        )
    )
    assert store.get_post("nib", "t32"), (
        "A runnable task behind 32 backed-off tasks must receive service "
        "without waiting for unrelated cooldowns"
    )
    for i in range(32):
        post = store.get_post("nib", f"t{i:02}")
        assert post["thread_id"] is None and post["backoff_until"] == 1000000
    assert sum(r["state"] == "pending" for r in ledger(store)) == 32
    store, consumer = restart(tmp_path, store, transport)
    consumer.run_once(now=105)
    assert len(transport.ops("open_thread")) == 1
    consumer.run_once(now=1000000)
    assert len(transport.ops("open_thread")) == 33
    assert all(r["state"] == "complete" for r in ledger(store))


def test_quarantine_does_not_pin_cursor_on_subsequent_held_only_events(tmp_path):
    board, store, transport, consumer = setup(tmp_path)
    insert_task(board, "t")
    consumer.run_once(now=100)
    add_event(board, "t", "completed")
    add_event(board, "t", "gave_up", breaker_payload())
    consumer.run_once(now=101)
    assert store.get_cursor("nib") == 2
    add_event(board, "t", "edited", {"title": "later held-task update"})
    for now in range(102, 105):
        consumer.run_once(now=now)
    print(
        json.dumps(
            dict(
                cursor=store.get_cursor("nib"),
                ledger=ledger(store),
                transport_calls=len(transport.calls),
            )
        )
    )
    assert store.get_cursor("nib") == 3, (
        "Durably held events should not pin the global cursor and be fetched on every empty pass"
    )


def test_held_task_valid_later_block_is_not_lost_when_another_task_advances_cursor(tmp_path):
    board, store, transport, consumer = setup(tmp_path)
    insert_task(board, "t")
    insert_task(board, "other")
    consumer.run_once(now=100)
    add_event(board, "t", "completed")
    add_event(board, "t", "gave_up", breaker_payload())
    consumer.run_once(now=101)
    assert store.get_cursor("nib") == 2
    add_event(board, "t", "status", {"status": "ready"})
    add_event(board, "t", "gave_up", breaker_payload(error="valid after poison"))
    add_event(board, "other", "edited")
    consumer.run_once(now=102)
    assert store.get_cursor("nib") == 5
    BlockerRecovery(store, "nib").reconcile("t")
    consumer.run_once(now=103)
    consumer.run_once(now=104)
    print(
        json.dumps(
            dict(
                cursor=store.get_cursor("nib"),
                scan=store.scan_position("nib", "t"),
                ledger=ledger(store),
                opens=len(transport.ops("open_thread")),
            )
        )
    )
    assert len(transport.ops("open_thread")) == 1, (
        "Suppressing poison event 2 must not discard genuine later blocker 4 received while held"
    )
    assert transport.message_types == ["gave_up"]


def poison(root):
    board, store, transport, consumer = setup(root)
    insert_task(board, "poison")
    consumer.run_once(now=100)
    add_event(board, "poison", "completed", {})
    add_event(board, "poison", "gave_up", breaker_payload(error="token=RAW_LOCAL_SYNTHETIC"))
    first = consumer.run_once(now=101)
    assert len(first.errors) == 1 and "RAW_LOCAL_SYNTHETIC" not in str(first.errors)
    assert not transport.calls and store.get_cursor("nib") == 2
    return board, store, transport, consumer


def test_held_only_forward_activity_must_not_accumulate_repeated_full_rescans(
    tmp_path, monkeypatch
):
    board, store, transport, consumer = poison(tmp_path)
    actual_fetches = []
    original_connect = sqlite3.connect

    class TrackingCursor(sqlite3.Cursor):
        def execute(self, sql, parameters=()):
            self.forward = sql.startswith(
                "SELECT id, task_id, kind, payload, created_at FROM task_events WHERE id > ?"
            )
            return super().execute(sql, parameters)

        def fetchall(self):
            rows = super().fetchall()
            if self.forward:
                actual_fetches.append(len(rows))
            return rows

    class TrackingConnection(sqlite3.Connection):
        def execute(self, sql, parameters=()):
            return self.cursor(factory=TrackingCursor).execute(sql, parameters)

    def traced_connect(*args, **kwargs):
        return original_connect(*args, factory=TrackingConnection, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", traced_connect)
    # Legitimate later event activity for the held task, without another task
    # happening to rescue the global cursor. Only synthetic SQLite rows.
    for i in range(300):
        add_event(board, "poison", "edited", {"title": f"synthetic {i}"})
    highwater = board.execute("SELECT MAX(id) FROM task_events").fetchone()[0]
    observations = []
    for now in (102, 103, 104):
        cursor = store.get_cursor("nib")
        fetched = board.execute(
            "SELECT COUNT(*) FROM task_events WHERE id>?", (cursor,)
        ).fetchone()[0]
        report = consumer.run_once(now=now)
        observations.append(
            dict(
                cursor_before=cursor,
                repeated_forward_rows=fetched,
                cursor_after=store.get_cursor("nib"),
                errors=report.errors,
            )
        )
    print(
        json.dumps(
            dict(
                highwater=highwater,
                passes=observations,
                actual_forward_fetchall_rows=actual_fetches,
            )
        )
    )
    assert not transport.calls and len(BlockerRecovery(store, "nib").quarantined()) == 1
    assert store.get_cursor("nib") == highwater or actual_fetches[1:] == [0, 0], observations


def restart(tmp_path, store, transport):
    store._conn.close()
    again = StateStore(tmp_path / "state.db")
    return again, Consumer(
        tmp_path / "board.db", again, transport, board="nib", thread_creation="first_blocked"
    )


def test_deferred_receipt_wins_over_stale_recovery_position(tmp_path):
    board, store, transport, consumer = setup(tmp_path)
    insert_task(board, "t", status="blocked")
    add_event(board, "t", "blocked", {"kind": "human", "reason": "original"})
    consumer.run_once(now=100)
    add_event(board, "t", "completed")
    consumer.run_once(now=101)
    assert transport.message_types == ["completed"]
    # Crash-window ordering: normal delivery receipt durable, recovery ledger
    # still carries its pre-delivery checkpoint. Restart must finish, not replay.
    board.row_factory = sqlite3.Row
    event = board.execute("SELECT * FROM task_events WHERE id=2").fetchone()
    BlockerRecovery(store, "nib").defer("t", [event])
    store, consumer = restart(tmp_path, store, transport)
    for now in (102, 103):
        assert consumer.run_once(now=now).errors == []
    assert transport.message_types == ["completed"]
    assert not BlockerRecovery(store, "nib").held_tasks()


def test_held_only_cursor_commit_requires_current_lease(tmp_path, monkeypatch):
    board, store, transport, consumer = poison(tmp_path)
    add_event(board, "poison", "edited")
    original = BlockerRecovery.defer

    def lose_lease(rec, task_id, events):
        original(rec, task_id, events)
        assert store.acquire_lease("consume:nib", "replacement", now=10000, ttl=60)

    monkeypatch.setattr(BlockerRecovery, "defer", lose_lease)
    report = consumer.run_once(now=102)
    assert store.get_cursor("nib") == 2
    assert report.warnings
    assert not transport.calls
    # Retained evidence is durable even though ordinary progress was fenced.
    assert (
        store._conn.execute(
            "SELECT COUNT(*) FROM blocker_recovery WHERE evidence_id=3 AND state='deferred'"
        ).fetchone()[0]
        == 1
    )


def test_mapped_continuation_preserves_update_and_suppressed_tail_across_restart(tmp_path):
    board, store, transport, consumer = setup(tmp_path)
    insert_task(board, "t", status="blocked")
    add_event(board, "t", "blocked", {"kind": "human"})
    consumer.run_once(now=100)
    add_event(board, "t", "completed")
    add_event(board, "t", "gave_up", breaker_payload())  # contradictory tail
    board.row_factory = sqlite3.Row
    poison_row = board.execute("SELECT * FROM task_events WHERE id=3").fetchone()
    rec = BlockerRecovery(store, "nib")
    rec.quarantine_forward("t", 3, poison_row["kind"], poison_row["payload"], "manual hold")
    consumer.run_once(now=101)
    assert store.get_cursor("nib") == 3
    assert transport.message_types == []
    rec.reconcile("t")
    store, consumer = restart(tmp_path, store, transport)
    for now in (102, 103, 104):
        assert consumer.run_once(now=now).errors == []
    assert transport.message_types == ["completed"]
    assert len(transport.ops("open_thread")) == 1
    assert not BlockerRecovery(store, "nib").held_tasks()
    assert store.get_post("nib", "t")["last_event_id"] == 3


def test_mixed_discovery_chunks_restart_remain_held_until_manual_release(tmp_path, monkeypatch):
    import kanban_task_threads.recovery as recovery

    monkeypatch.setattr(recovery, "CHUNK_SIZE", 2)
    board, store, transport, consumer = setup(tmp_path)
    insert_task(board, "t")
    for kind, payload in [
        ("completed", {}),
        ("gave_up", breaker_payload()),
        ("status", {"status": "ready"}),
        ("gave_up", breaker_payload(error="new generation")),
    ]:
        add_event(board, "t", kind, payload)
    store.activate("nib", 1, [])
    store.advance_cursor("nib", old=0, new=4)
    store.checkpoint_scan("nib", "t", 4)
    assert consumer.run_once(now=100).errors
    store, consumer = restart(tmp_path, store, transport)
    for now in range(101, 105):
        consumer.run_once(now=now)
    assert not transport.calls
    assert [r["state"] for r in ledger(store)] == ["quarantined", "pending"]
    BlockerRecovery(store, "nib").reconcile("t")
    for now in range(105, 109):
        assert consumer.run_once(now=now).errors == []
    assert transport.message_types == ["gave_up"]
    assert store.get_cursor("nib") == store.scan_position("nib", "t") == 4


@pytest.mark.parametrize("guard", ["tombstone", "dead_letter", "pending", "backoff", "destination"])
def test_released_continuation_retains_normal_guards_across_restart(tmp_path, guard):
    board, store, transport, consumer = poison(tmp_path)
    add_event(board, "poison", "status", {"status": "ready"})
    add_event(board, "poison", "gave_up", breaker_payload())
    consumer.run_once(now=102)
    BlockerRecovery(store, "nib").reconcile("poison")
    store.begin_create("nib", "poison", now=1)
    if guard in ("tombstone", "dead_letter"):
        getattr(store, "mark_" + guard)("nib", "poison", "manual")
    elif guard == "backoff":
        store.clear_pending("nib", "poison")
        store.set_backoff("nib", "poison", until=200)
    elif guard == "destination":
        store.complete_create("nib", "poison", thread_id="old", message_id="m", destination="old")
    before = store.get_post("nib", "poison").copy()
    store, consumer = restart(tmp_path, store, transport)
    if guard == "destination":
        consumer._destination = "new"
    consumer.run_once(now=103)
    consumer.run_once(now=104)
    assert not transport.calls
    assert store.get_post("nib", "poison") == before
    states = {r["state"] for r in ledger(store) if r["evidence_id"] in (3, 4)}
    assert states == (
        {"suppressed"}
        if guard in ("tombstone", "dead_letter")
        else {"quarantined"}
        if guard == "pending"
        else {"deferred"}
    )
    if guard == "backoff":
        consumer.run_once(now=201)
        assert len(transport.ops("open_thread")) == 1


@pytest.mark.parametrize("mapped", [False, True])
def test_large_held_continuation_is_lossless_bounded_and_restart_safe(tmp_path, mapped):
    board, store, transport, consumer = setup(tmp_path)
    insert_task(board, "t", status="blocked" if mapped else "ready")
    if mapped:
        add_event(board, "t", "blocked", {"kind": "human"})
    consumer.run_once(now=100)
    add_event(board, "t", "completed")
    add_event(board, "t", "gave_up", breaker_payload())
    poison_id = board.execute("SELECT MAX(id) FROM task_events").fetchone()[0]
    if mapped:
        rec = BlockerRecovery(store, "nib")
        row = board.execute("SELECT payload FROM task_events WHERE id=?", (poison_id,)).fetchone()
        rec.quarantine_forward("t", poison_id, "gave_up", row[0], "contradictory")
    consumer.run_once(now=101)
    for i in range(300):
        add_event(board, "t", "edited", {"title": str(i)})
    add_event(board, "t", "status", {"status": "ready"})
    add_event(board, "t", "gave_up", breaker_payload(error="latest generation"))
    add_event(board, "t", "completed")
    high = board.execute("SELECT MAX(id) FROM task_events").fetchone()[0]
    consumer.run_once(now=102)
    assert store.get_cursor("nib") == poison_id + 256
    store, consumer = restart(tmp_path, store, transport)
    for now in (103, 104):
        consumer.run_once(now=now)
    assert store.get_cursor("nib") == high
    assert not transport.message_types
    retained = [r for r in ledger(store) if r["state"] == "deferred"]
    assert len(retained) >= 303
    BlockerRecovery(store, "nib").reconcile("t")
    store, consumer = restart(tmp_path, store, transport)
    for now in range(105, 110):
        assert consumer.run_once(now=now).errors == []
    assert len(transport.ops("open_thread")) == 1
    assert transport.message_types == (["completed"] if mapped else []) + ["gave_up", "completed"]
    assert not BlockerRecovery(store, "nib").held_tasks()
    assert store.get_cursor("nib") == high


@pytest.mark.parametrize("change", ["missing", "changed"])
def test_held_evidence_is_revalidated_before_release_delivery(tmp_path, change):
    board, store, transport, consumer = poison(tmp_path)
    add_event(board, "poison", "status", {"status": "ready"})
    add_event(board, "poison", "gave_up", breaker_payload())
    add_event(board, "other", "edited")  # keep native highwater after deletion
    consumer.run_once(now=102)
    BlockerRecovery(store, "nib").reconcile("poison")
    if change == "missing":
        board.execute("DELETE FROM task_events WHERE id=4")
    else:
        board.execute("UPDATE task_events SET payload='{}' WHERE id=4")
    board.commit()
    assert consumer.run_once(now=103).errors
    assert not transport.calls
    assert BlockerRecovery(store, "nib").quarantined()


def test_continuation_without_blocker_never_creates_and_completes(tmp_path):
    board, store, transport, consumer = poison(tmp_path)
    for i in range(260):
        add_event(board, "poison", "edited", {"title": str(i)})
    consumer.run_once(now=102)
    consumer.run_once(now=103)
    BlockerRecovery(store, "nib").reconcile("poison")
    for now in range(104, 108):
        assert consumer.run_once(now=now).errors == []
    assert not transport.calls
    assert store.get_post("nib", "poison") is None
    assert not BlockerRecovery(store, "nib").held_tasks()


def test_same_batch_poison_does_not_discard_valid_later_block(tmp_path):
    board, store, transport, consumer = setup(tmp_path)
    insert_task(board, "t")
    consumer.run_once(now=100)
    for kind, payload in [
        ("completed", {}),
        ("gave_up", breaker_payload()),
        ("status", {"status": "ready"}),
        ("gave_up", breaker_payload()),
    ]:
        add_event(board, "t", kind, payload)
    consumer.run_once(now=101)
    assert not transport.calls
    BlockerRecovery(store, "nib").reconcile("t")
    consumer.run_once(now=102)
    consumer.run_once(now=103)
    assert transport.message_types == ["gave_up"]
    assert len(transport.ops("open_thread")) == 1


def test_deferred_new_poison_quarantines_again_without_losing_valid_tail(tmp_path):
    board, store, transport, consumer = poison(tmp_path)
    add_event(board, "poison", "gave_up", {})  # independently malformed retained evidence
    add_event(board, "poison", "status", {"status": "ready"})
    add_event(board, "poison", "gave_up", breaker_payload())
    consumer.run_once(now=102)
    BlockerRecovery(store, "nib").reconcile("poison")
    assert consumer.run_once(now=103).errors
    assert not transport.calls
    assert [
        r["evidence_id"]
        for r in store._conn.execute("SELECT * FROM blocker_recovery WHERE state='quarantined'")
    ] == [3]
    assert consumer.run_once(now=104).errors == []
    BlockerRecovery(store, "nib").reconcile("poison")
    consumer.run_once(now=105)
    assert transport.message_types == ["gave_up"]
