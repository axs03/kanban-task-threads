"""Correctness repair regressions (independent review R1/R2).

Pin the two delivery-loss defects the correctness review reproduced: a
bootstrap create retry that skipped intervening unblock/comment updates, and
a fenced create (lease lost before any HTTP request) that was quarantined as
if a create had been sent. No network; public fixtures only.
"""

from conftest import FakeTransport, add_event, insert_task, make_board
from test_multiboard import rig as multiboard_rig

from kanban_task_threads.consumer import Consumer
from kanban_task_threads.store import StateStore
from kanban_task_threads.transport import TransportError

rig = multiboard_rig


def _setup(tmp_path):
    path = tmp_path / "board.db"
    board = make_board(path)
    store = StateStore(tmp_path / "state.db")
    transport = FakeTransport()
    consumer = Consumer(path, store, transport, board="nib", thread_creation="first_blocked")
    return board, store, transport, consumer


def test_bootstrap_retry_reblock_retains_intervening_updates(tmp_path):
    board, store, transport, consumer = _setup(tmp_path)
    insert_task(board, "bootstrap", status="blocked")
    add_event(board, "bootstrap", "blocked")  # snapshot block (event 1)
    transport.queue_error("open_thread", TransportError(429, {"retry_after": 2}))
    consumer.run_once(now=100)
    assert store.bootstrap_tasks("nib") == ["bootstrap"]
    add_event(board, "bootstrap", "unblocked")
    add_event(board, "bootstrap", "commented", {"author": "owner"})
    add_event(board, "bootstrap", "blocked", {"reason": "again"})
    add_event(board, "bootstrap", "unblocked")
    consumer.run_once(now=103)
    # The create retried once; every post-activation update survived the
    # reblock instead of being skipped forward to the later block.
    assert len(transport.ops("open_thread")) == 2
    assert transport.message_types == ["unblocked", "commented", "blocked", "unblocked"]


def test_known_unsent_create_after_lease_loss_retries_without_operator(rig, monkeypatch):
    boards, paths, settings, http, build = rig
    settings["boards"] = [settings["boards"][0]]
    coordinator = build()
    try:
        coordinator.run_once(now=100)
        insert_task(boards["default"], "new-block", status="blocked")
        add_event(boards["default"], "new-block", "blocked")
        store = coordinator.consumers[0]._store
        original_begin = store.begin_create

        def lose_lease(board, task_id, *, now):
            original_begin(board, task_id, now=now)
            token = store.acquire_lease("consume:default", "successor", now=200, ttl=60)
            assert token is not None

        monkeypatch.setattr(store, "begin_create", lose_lease)
        coordinator.run_once(now=101)
        assert not http.creates(), "the guarded HTTP boundary must block the request"
        monkeypatch.setattr(store, "begin_create", original_begin)
        again = coordinator.run_once(now=300)
        assert len(http.creates()) == 1, "a no-send refusal must recover, not quarantine"
        assert not again.errors
        assert store.get_post("default", "new-block")["pending_create_at"] is None
    finally:
        coordinator.close()
