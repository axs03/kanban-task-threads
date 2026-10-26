"""Process-crash boundaries: durable state only, memory is lost each step.

Each boundary is a distinct process-kill point simulated by a fresh StateStore
plus a forced raise at the exact durable step; the next pass must recover from
what was committed, never from what was lost.
"""

from unittest import mock

import pytest
from conftest import add_event
from test_blocker_recovery import incident, ledger

import kanban_task_threads.recovery as recovery
from kanban_task_threads.consumer import Consumer, LeaseLostError
from kanban_task_threads.recovery import BlockerRecovery
from kanban_task_threads.store import StateStore


def reopen(tmp_path, transport):
    store = StateStore(tmp_path / "state.db")
    return store, Consumer(
        tmp_path / "board.db", store, transport, board="nib", thread_creation="first_blocked"
    )


def test_kill_during_discovery_rolls_back_and_rediscovers_once(tmp_path, monkeypatch):
    board, store, transport, consumer = incident(tmp_path)
    real = recovery.is_blocker
    calls = {"n": 0}

    def die_once(conn, row):
        calls["n"] += 1
        if row["kind"] == "gave_up" and calls["n"] == 1:
            # Discovery inserts the queue row then dies before its commit; the
            # whole transaction rolls back so nothing is half-queued.
            raise RuntimeError("simulated kill inside discovery transaction")
        return real(conn, row)

    monkeypatch.setattr(recovery, "is_blocker", die_once)
    with pytest.raises(RuntimeError, match="simulated kill"):
        consumer.run_once(now=100)
    assert not transport.calls
    assert store._conn.execute("SELECT COUNT(*) FROM blocker_recovery").fetchone()[0] == 0
    store._conn.close()
    store, consumer = reopen(tmp_path, transport)
    consumer.run_once(now=101)
    assert len(transport.ops("open_thread")) == 1
    assert ledger(store)[0]["state"] == "complete"
    assert store.scan_position("nib", "t") == 703


def test_kill_before_remote_create_is_a_known_no_send_retry(tmp_path):
    board, store, transport, consumer = incident(tmp_path)

    def no_send(*, title, card):
        raise LeaseLostError("deterministic no-send: died before any HTTP request")

    with mock.patch.object(transport, "open_thread", no_send):
        assert consumer.run_once(now=100).errors == []
    assert store.get_post("nib", "t")["pending_create_at"] is None
    assert ledger(store)[0]["state"] == "pending"
    consumer.run_once(now=101)
    assert len(transport.ops("open_thread")) == 1
    assert ledger(store)[0]["state"] == "complete"


def test_kill_after_remote_create_before_local_receipt_is_quarantined(tmp_path):
    board, store, transport, consumer = incident(tmp_path)
    transport.queue_error("open_thread", RuntimeError("response lost after remote create"))
    assert consumer.run_once(now=100).errors
    assert store.get_post("nib", "t")["pending_create_at"] == 100
    assert ledger(store)[0]["state"] == "quarantined"
    # No auto-rearm: the pending create marker is never cleared by recovery,
    # and the obligation is quarantined, not silently dropped or retried.
    assert consumer.run_once(now=101).errors == []
    assert len(transport.ops("open_thread")) == 1
    assert store.get_post("nib", "t")["pending_create_at"] == 100


def test_acknowledged_reply_window_is_not_replayed_on_restart(tmp_path):
    board, store, transport, consumer = incident(tmp_path)
    add_event(board, "t", "commented", {"author": "owner"})
    store.advance_cursor("nib", old=703, new=704)
    store.checkpoint_scan("nib", "t", 704)
    consumer.run_once(now=100)
    assert len(transport.ops("open_thread")) == 1
    assert transport.message_types == ["gave_up", "commented"]
    assert ledger(store)[0]["state"] == "complete"
    store._conn.close()
    store, consumer = reopen(tmp_path, transport)
    consumer.run_once(now=101)
    assert transport.message_types == ["gave_up", "commented"]
    assert len(transport.ops("open_thread")) == 1


def test_recovery_never_auto_rearms_quarantined_or_suppressed(tmp_path):
    board, store, transport, consumer = incident(tmp_path)
    consumer.run_once(now=100)  # completes the obligation
    store._conn.execute("UPDATE blocker_recovery SET state='quarantined' WHERE evidence_id=703")
    store._conn.commit()
    store._conn.close()
    store, consumer = reopen(tmp_path, transport)
    consumer.run_once(now=101)
    consumer.run_once(now=102)
    assert len(transport.ops("open_thread")) == 1
    assert store._conn.execute("SELECT COUNT(*) FROM blocker_recovery").fetchone()[0] == 1
    assert BlockerRecovery(store, "nib").held_tasks() == {"t"}
