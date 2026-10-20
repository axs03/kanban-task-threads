"""Crash/race acceptance with real SQLite and the production delivery boundary."""

import importlib
import multiprocessing
import sqlite3
import time

from conftest import add_event, insert_task
from test_first_blocked import setup
from test_multiboard import rig as multiboard_rig

rig = multiboard_rig


def test_activation_race_persists_obligation_before_unblock(tmp_path, monkeypatch):
    board, store, transport, consumer = setup(tmp_path)
    board.execute("PRAGMA journal_mode=WAL")
    insert_task(board, "blocked-at-snapshot", status="blocked")
    insert_task(board, "future", status="ready")
    activate = store.activate

    def racing_activate(board_name, watermark, blocked):
        # A concurrent writer resolves one card and blocks/resolves another
        # after the read snapshot, before activation's durable commit.
        other = sqlite3.connect(tmp_path / "board.db")
        other.execute("UPDATE tasks SET status='ready' WHERE id='blocked-at-snapshot'")
        other.commit()
        add_event(other, "blocked-at-snapshot", "unblocked")
        add_event(other, "future", "blocked")
        add_event(other, "future", "unblocked")
        other.close()
        return activate(board_name, watermark, blocked)

    monkeypatch.setattr(store, "activate", racing_activate)
    consumer.run_once(now=100)
    assert len(transport.ops("open_thread")) == 2
    assert store.get_post("nib", "blocked-at-snapshot")["thread_id"]
    assert store.get_post("nib", "future")["thread_id"]
    assert store._conn.execute("SELECT watermark FROM activation").fetchone()[0] == 0
    assert store.get_cursor("nib") == 3


def test_same_board_two_processes_use_fenced_leases_without_duplicate_creates(rig):
    boards, paths, settings, http, build = rig
    for board in boards.values():
        insert_task(board, "t_same", status="blocked")
        add_event(board, "t_same", "blocked")
    context = multiprocessing.get_context("fork")
    count = context.Value("i", 0)
    ready = context.Barrier(2)

    def worker():
        original_http = http

        def remote(method, url, body, headers=None):
            if method == "POST" and body and "thread_name" in body:
                with count.get_lock():
                    count.value += 1
                    number = count.value
                time.sleep(0.05)
                return 200, {"id": str(number), "channel_id": str(number + 100)}
            return original_http(method, url, body, headers)

        importlib.import_module("ktt_entry.kanban_task_threads.transport").urllib_http = remote
        coordinator = build()
        ready.wait(timeout=10)
        coordinator.run_once(now=100)
        coordinator.close()

    processes = [context.Process(target=worker) for _ in range(2)]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=20)
        if process.is_alive():
            process.kill()
            process.join()
        assert process.exitcode == 0
    assert count.value == 3


def test_process_death_after_remote_create_before_local_commit_is_quarantined(rig, tmp_path):
    import os

    boards, paths, settings, http, build = rig
    settings["boards"] = [settings["boards"][0]]
    insert_task(boards["default"], "t_same", status="blocked")
    add_event(boards["default"], "t_same", "blocked")
    context = multiprocessing.get_context("fork")
    effect = tmp_path / "fake-remote-effects.txt"

    def worker():
        def remote(method, url, body, headers=None):
            if method == "POST" and body and "thread_name" in body:
                with effect.open("a") as stream:
                    stream.write("accepted default:t_same\n")
                    stream.flush()
                    os.fsync(stream.fileno())
                os._exit(23)  # no response or local commit, not a fabricated failure
            return http(method, url, body, headers)

        importlib.import_module("ktt_entry.kanban_task_threads.transport").urllib_http = remote
        build().run_once(now=100)

    process = context.Process(target=worker)
    process.start()
    process.join(timeout=15)
    if process.is_alive():
        process.kill()
        process.join()
    assert process.exitcode == 23
    assert effect.read_text().splitlines() == ["accepted default:t_same"]
    again = build()
    report = again.run_once(now=200)  # expired prior holder's lease
    assert any("unknown" in error for error in report.errors)
    assert not http.creates()
    state = again.consumers[0]._store
    assert state.get_post("default", "t_same")["pending_create_at"] == 100
    # Activation intentionally skipped the preactivation blocked event. The
    # durable bootstrap obligation and pending attempt, not event replay, own it.
    assert state.get_cursor("default") == 1
    again.close()


def test_binding_change_after_create_prevents_reply_and_dirty_effects(rig, monkeypatch):
    boards, paths, settings, http, build = rig
    coordinator = build()
    coordinator.run_once(now=100)
    insert_task(boards["default"], "t_same", status="blocked")
    add_event(boards["default"], "t_same", "blocked")
    original = http.__class__.__call__

    def sabotage(self, method, url, body, headers=None):
        result = original(self, method, url, body, headers)
        if method == "POST" and body and "thread_name" in body:
            coordinator.consumers[0]._store._conn.execute(
                "UPDATE board_binding SET destination='discord:webhook:123@999'"
            )
        return result

    monkeypatch.setattr(http.__class__, "__call__", sabotage)
    coordinator.run_once(now=101)
    assert len(http.creates()) == 1
    assert not any(c[0] == "POST" and "thread_id=" in c[1] for c in http.calls)
    before = len(http.calls)
    coordinator.run_once(now=102)
    assert len(http.calls) == before
    coordinator.close()
