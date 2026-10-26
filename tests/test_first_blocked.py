"""First-block activation uses durable events, not today's task status."""

import sqlite3

from conftest import FakeTransport, add_event, insert_task, make_board

from kanban_task_threads.consumer import Consumer
from kanban_task_threads.store import StateStore


def setup(tmp_path):
    path = tmp_path / "board.db"
    board = make_board(path)
    store = StateStore(tmp_path / "state.db")
    transport = FakeTransport()
    consumer = Consumer(path, store, transport, board="nib", thread_creation="first_blocked")
    return board, store, transport, consumer


def test_never_blocked_stays_threadless_then_durable_block_opens_once(tmp_path):
    board, store, transport, consumer = setup(tmp_path)
    insert_task(board, "t_same", title="same title", assignee="worker-a")
    add_event(board, "t_same", "created")
    consumer.run_once(now=100)
    assert not transport.calls
    assert store.get_cursor("nib") == 1
    add_event(board, "t_same", "assigned", {"assignee": "worker-b"})
    add_event(board, "t_same", "blocked", {"reason": "needs a decision", "actor": "worker-b"})
    add_event(board, "t_same", "unblocked", {"actor": "owner"})
    # Already resumed before polling: the durable block must still open a thread.
    consumer.run_once(now=101)
    assert len(transport.ops("open_thread")) == 1
    assert "nib:t_same" in transport.ops("open_thread")[0][1]
    assert transport.message_types == ["blocked", "unblocked"]
    ref = store.get_post("nib", "t_same")["thread_id"]
    consumer.run_once(now=102)
    add_event(board, "t_same", "blocked")
    consumer.run_once(now=103)
    assert len(transport.ops("open_thread")) == 1
    assert store.get_post("nib", "t_same")["thread_id"] == ref


def test_activation_bootstraps_current_blocked_not_resolved_history(tmp_path):
    board, store, transport, consumer = setup(tmp_path)
    for task, status in [("old", "ready"), ("current", "blocked")]:
        insert_task(board, task, status=status)
        add_event(board, task, "created")
        add_event(board, task, "blocked")
    consumer.run_once(now=100)
    assert store.get_post("nib", "old") is None
    assert store.get_post("nib", "current")["thread_id"]
    assert not transport.ops("append")  # no preactivation history flood
    assert store.get_cursor("nib") == 4


def test_existing_blocked_mapping_discharges_bootstrap_without_recreating(tmp_path):
    board, store, transport, consumer = setup(tmp_path)
    insert_task(board, "mapped", status="blocked")
    store.begin_create("nib", "mapped", now=1)
    store.complete_create(
        "nib", "mapped", thread_id="existing", message_id="starter", destination=""
    )
    consumer.run_once(now=100)
    assert not transport.ops("open_thread")
    assert store.bootstrap_tasks("nib") == []


def test_bootstrap_obligation_survives_failed_create_and_restart(tmp_path):
    from kanban_task_threads.transport import TransportError

    board, store, transport, consumer = setup(tmp_path)
    insert_task(board, "current", status="blocked")  # even without a task event
    transport.queue_error("open_thread", TransportError(429, {"retry_after": 2}))
    consumer.run_once(now=100)
    board.execute("UPDATE tasks SET status='ready'")
    board.commit()
    store._conn.close()
    reopened = StateStore(tmp_path / "state.db")
    again = Consumer(
        tmp_path / "board.db", reopened, transport, board="nib", thread_creation="first_blocked"
    )
    again.run_once(now=103)
    assert reopened.get_post("nib", "current")["thread_id"]
    assert len(transport.ops("open_thread")) == 2
    again.run_once(now=104)
    assert len(transport.ops("open_thread")) == 2
    assert (
        sqlite3.connect(tmp_path / "state.db").execute("PRAGMA integrity_check").fetchone()[0]
        == "ok"
    )


def test_append_mentions_only_when_requested():
    from kanban_task_threads.transport import DiscordTransport

    calls = []

    def http(method, url, body, headers=None):
        calls.append(body)
        return 200, {"id": "1", "channel_id": "2"}

    from kanban_task_threads.transport import ThreadRef

    t = DiscordTransport(http, "https://discord.com/api/webhooks/1/x", mention_user_id="42")
    ref = ThreadRef(thread_id="2", message_id="3")
    t.append(ref, content="plain")
    t.append(ref, content="alert", mention=True)
    assert calls[0]["allowed_mentions"] == {"parse": []}
    assert calls[1]["content"].startswith("<@42> ")
    assert calls[1]["allowed_mentions"] == {"parse": [], "users": ["42"]}


def test_routing_warns_about_unconfigured_boards(tmp_path):
    import logging
    from types import SimpleNamespace

    from kanban_task_threads.routing import _warn_unconfigured_boards

    (tmp_path / "a").mkdir()
    (tmp_path / "a" / "kanban.db").touch()
    (tmp_path / "b").mkdir()
    (tmp_path / "b" / "kanban.db").touch()
    seen = []
    logger = SimpleNamespace(warning=lambda fmt, *a: seen.append(fmt % a))
    db = SimpleNamespace(boards_root=lambda: tmp_path)
    _warn_unconfigured_boards([SimpleNamespace(board="a"), SimpleNamespace(board="default")], db, logger)
    assert len(seen) == 1 and "b" in seen[0] and "a," not in seen[0]
