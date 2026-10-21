"""Independent replay progress must not reset a board or its threadless scans."""

import json

import pytest
from conftest import add_event, insert_task
from test_blocker_coverage import breaker_payload
from test_first_blocked import setup

from kanban_task_threads.consumer import Consumer
from kanban_task_threads.store import StateStore
from kanban_task_threads.transport import TransportError


def ledger(store):
    return [
        dict(row)
        for row in store._conn.execute("SELECT * FROM blocker_recovery ORDER BY evidence_id")
    ]


@pytest.mark.parametrize("guard", ["tombstone", "dead_letter", "pending", "backoff", "destination"])
def test_recovery_uses_existing_delivery_guards_without_rearming(tmp_path, guard):
    board, store, transport, consumer = incident(tmp_path)
    store.begin_create("nib", "t", now=1)
    if guard in ("tombstone", "dead_letter"):
        getattr(store, "mark_" + guard)("nib", "t", "manual")
    elif guard == "backoff":
        store.clear_pending("nib", "t")
        store.set_backoff("nib", "t", until=110)
    elif guard == "destination":
        store.complete_create("nib", "t", thread_id="existing", message_id="m", destination="old")
        consumer._destination = "new"
    before = store.get_post("nib", "t").copy()
    consumer.run_once(now=100)
    assert not transport.calls
    assert store.get_post("nib", "t") == before
    if guard in ("tombstone", "dead_letter"):
        assert ledger(store)[0]["state"] == "suppressed"
    elif guard == "pending":
        assert ledger(store)[0]["state"] == "quarantined"
    else:
        assert ledger(store)[0]["state"] == "pending"
    if guard == "backoff":
        consumer.run_once(now=111)
        assert len(transport.ops("open_thread")) == 1


def test_unknown_remote_create_is_quarantined_across_restart(tmp_path):
    board, store, transport, consumer = incident(tmp_path)
    transport.queue_error("open_thread", RuntimeError("lost response after remote create"))
    assert consumer.run_once(now=100).errors
    assert ledger(store)[0]["state"] == "quarantined"
    assert store.get_post("nib", "t")["pending_create_at"] == 100
    consumer.run_once(now=101)
    store._conn.close()
    again = StateStore(tmp_path / "state.db")
    Consumer(
        tmp_path / "board.db", again, transport, board="nib", thread_creation="first_blocked"
    ).run_once(now=102)
    assert len(transport.ops("open_thread")) == 1


def test_known_retry_and_reply_failure_resume_on_empty_forward_pass(tmp_path):
    board, store, transport, consumer = incident(tmp_path)
    transport.queue_error("open_thread", TransportError(429, {"retry_after": 2}))
    consumer.run_once(now=100)
    assert ledger(store)[0]["state"] == "pending"
    consumer.run_once(now=101)
    assert len(transport.ops("open_thread")) == 1
    transport.queue_error("append", TransportError(503, {}))
    consumer.run_once(now=103)
    assert ledger(store)[0]["state"] == "pending"
    consumer.run_once(now=105)
    assert ledger(store)[0]["state"] == "complete"
    assert len(transport.ops("open_thread")) == 2
    assert store.scan_position("nib", "t") == 703


def test_migration_chunks_restart_and_forward_boundary_preserve_order(tmp_path, monkeypatch):
    import kanban_task_threads.recovery as recovery

    monkeypatch.setattr(recovery, "CHUNK_SIZE", 2)
    board, store, transport, consumer = incident(tmp_path)
    add_event(board, "t", "unblocked")
    add_event(board, "t", "commented", {"author": "owner"})
    add_event(board, "t", "gave_up", breaker_payload(error="second"))
    add_event(board, "t", "completed")
    store.advance_cursor("nib", old=703, new=707)
    store.checkpoint_scan("nib", "t", 707)
    consumer.run_once(now=100)
    assert not transport.calls  # discovery not complete; no forward leap
    assert ledger(store)  # queue is already durable before discovery finishes
    assert not store._conn.execute("SELECT done FROM blocker_migration").fetchone()[0]
    store._conn.close()
    again = StateStore(tmp_path / "state.db")
    consumer = Consumer(
        tmp_path / "board.db", again, transport, board="nib", thread_creation="first_blocked"
    )
    add_event(board, "t", "unblocked")  # arrives beyond frozen migration snapshot
    for now in range(101, 109):
        consumer.run_once(now=now)
    assert transport.message_types == [
        "gave_up",
        "unblocked",
        "commented",
        "gave_up",
        "completed",
        "unblocked",
    ]
    assert len(transport.ops("open_thread")) == 1
    assert again.get_cursor("nib") == 708
    assert again.scan_position("nib", "t") == 707


@pytest.mark.parametrize("missing", ["task", "event", "changed_event"])
def test_retained_recovery_evidence_loss_is_quarantined(tmp_path, missing, monkeypatch):
    board, store, transport, consumer = incident(tmp_path)
    transport.queue_error("open_thread", TransportError(429, {"retry_after": 2}))
    consumer.run_once(now=100)
    if missing == "task":
        board.execute("DELETE FROM tasks")
    elif missing == "event":
        # Keep a later ID so the rewind guard does not mask missing evidence.
        add_event(board, "other", "edited")
        board.execute("DELETE FROM task_events WHERE id=703")
    else:
        board.execute("UPDATE task_events SET payload='{}' WHERE id=703")
    board.commit()
    assert consumer.run_once(now=103).errors
    assert ledger(store)[0]["state"] == "quarantined"
    assert len(transport.ops("open_thread")) == 1


def test_replaced_database_identity_is_not_inherited_on_restart(tmp_path):
    import shutil

    board, store, transport, consumer = incident(tmp_path)
    consumer.run_once(now=100)
    replacement = tmp_path / "replacement.db"
    shutil.copyfile(tmp_path / "board.db", replacement)
    replacement.replace(tmp_path / "board.db")
    from kanban_task_threads.store import BindingError

    with pytest.raises(BindingError, match="identity"):
        consumer.run_once(now=101)
    assert len(transport.ops("open_thread")) == 1


def test_imported_kind_alone_does_not_qualify_but_activation_bootstraps_blocked(tmp_path):
    board, store, transport, consumer = setup(tmp_path)
    insert_task(board, "imported", status="blocked")
    add_event(board, "imported", "imported", {"board": "src", "parked": True})
    insert_task(board, "imported_resolved", status="ready")
    add_event(board, "imported_resolved", "imported", {"board": "src", "parked": True})
    consumer.run_once(now=100)
    # Eventless currently-blocked row reaches first-block via activation bootstrap;
    # the imported event itself is never a blocker trigger.
    assert store.get_post("nib", "imported")["thread_id"]
    assert store.get_post("nib", "imported_resolved") is None
    assert not [m for m in transport.message_types if m == "imported"]


def test_legacy_spawn_auto_blocked_is_not_a_blocker_alias(tmp_path):
    # C:988-995 renames this legacy kind to gave_up below the cursor; we do not
    # promise an undocumented raw-legacy alias — unknown kinds stay repaint-only.
    board, store, transport, consumer = setup(tmp_path)
    insert_task(board, "t")
    consumer.run_once(now=100)
    add_event(board, "t", "spawn_auto_blocked", {"error": "legacy"})
    consumer.run_once(now=101)
    assert not transport.ops("open_thread")
    assert store.get_post("nib", "t") is None


def incident(tmp_path, status="blocked"):
    board, store, transport, consumer = setup(tmp_path)
    insert_task(board, "t", status=status)
    for event_id, kind, payload in [
        (580, "blocked", {"reason": "obsolete"}),
        (595, "unblocked", {}),
        (703, "gave_up", breaker_payload(error="latest worker failure")),
    ]:
        board.execute(
            "INSERT INTO task_events(id,task_id,kind,payload,created_at) VALUES(?, 't', ?, ?, 1)",
            (event_id, kind, json.dumps(payload)),
        )
    board.commit()
    store.activate("nib", 638, [])
    store.advance_cursor("nib", old=0, new=703)
    store.checkpoint_scan("nib", "t", 703)
    return board, store, transport, consumer


@pytest.mark.parametrize("status", ["blocked", "done", "ready", "archived"])
def test_already_scanned_postactivation_miss_replays_original_trigger_once(tmp_path, status):
    board, store, transport, consumer = incident(tmp_path, status)
    report = consumer.run_once(now=100)
    assert report.errors == []
    assert len(transport.ops("open_thread")) == 1
    assert transport.message_types == ["gave_up"]
    assert store.get_cursor("nib") == 703
    assert store.scan_position("nib", "t") == 703
    assert store.activate("nib", 999, []) == 638
    assert store.get_post("nib", "t")["last_event_id"] == 703
    store._conn.close()
    again = StateStore(tmp_path / "state.db")
    consumer = Consumer(
        tmp_path / "board.db", again, transport, board="nib", thread_creation="first_blocked"
    )
    consumer.run_once(now=101)
    consumer.run_once(now=102)
    assert len(transport.ops("open_thread")) == 1
    assert transport.message_types == ["gave_up"]


def test_recovery_preserves_interleaved_updates_from_earliest_miss(tmp_path):
    board, store, transport, consumer = incident(tmp_path)
    add_event(board, "t", "unblocked")
    add_event(board, "t", "commented", {"author": "owner"})
    add_event(board, "t", "gave_up", breaker_payload(error="second failure"))
    store.advance_cursor("nib", old=703, new=706)
    store.checkpoint_scan("nib", "t", 706)
    consumer.run_once(now=100)
    assert transport.message_types == ["gave_up", "unblocked", "commented", "gave_up"]
    assert len(transport.ops("open_thread")) == 1
    assert store.scan_position("nib", "t") == 706
