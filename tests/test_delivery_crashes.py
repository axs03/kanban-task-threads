"""Record accepted effects around process termination; no HTTP server or sockets."""

import importlib
import multiprocessing
import os

from conftest import add_event, insert_task
from test_multiboard import rig as multiboard_rig

rig = multiboard_rig


def test_reply_acceptance_before_process_death_replays_at_least_once(rig, tmp_path, monkeypatch):
    boards, paths, settings, http, build = rig
    settings["boards"] = [settings["boards"][0]]
    insert_task(boards["default"], "t_same", status="blocked")
    add_event(boards["default"], "t_same", "blocked")
    initial = build()
    initial.run_once(now=100)
    initial.close()
    assert len(http.creates()) == 1
    add_event(boards["default"], "t_same", "unblocked", {"actor": "worker-other-profile"})
    remote_log = tmp_path / "accepted-replies.txt"

    def record():
        with remote_log.open("a") as stream:
            stream.write("accepted default:t_same unblocked\n")
            stream.flush()
            os.fsync(stream.fileno())

    def worker():
        def remote(method, url, body, headers=None):
            if method == "POST" and "thread_id=" in url:
                record()
                os._exit(24)
            return http(method, url, body, headers)

        importlib.import_module("ktt_entry.kanban_task_threads.transport").urllib_http = remote
        build().run_once(now=101)

    process = multiprocessing.get_context("fork").Process(target=worker)
    process.start()
    process.join(timeout=15)
    if process.is_alive():
        process.kill()
        process.join()
    assert process.exitcode == 24
    assert len(remote_log.read_text().splitlines()) == 1
    original = http.__class__.__call__

    def replay(self, method, url, body, headers=None):
        if method == "POST" and "thread_id=" in url:
            record()
        return original(self, method, url, body, headers)

    monkeypatch.setattr(http.__class__, "__call__", replay)
    restarted = build()
    report = restarted.run_once(now=200)
    assert not report.errors
    assert len(remote_log.read_text().splitlines()) == 2
    assert len(http.creates()) == 1
    assert restarted.consumers[0]._store.get_post("default", "t_same")["last_event_id"] == 2
    restarted.close()


def test_lease_takeover_between_http_operations_fences_remaining_effects(rig, monkeypatch):
    boards, paths, settings, http, build = rig
    settings["boards"] = [settings["boards"][0]]
    coordinator = build()
    coordinator.run_once(now=100)
    insert_task(boards["default"], "t_same", status="blocked")
    add_event(boards["default"], "t_same", "blocked")
    state = coordinator.consumers[0]._store
    original = http.__class__.__call__
    stolen = []

    def takeover(self, method, url, body, headers=None):
        result = original(self, method, url, body, headers)
        if method == "POST" and body and "thread_name" in body:
            stolen.append(state.acquire_lease("consume:default", "successor", now=200, ttl=60))
        return result

    monkeypatch.setattr(http.__class__, "__call__", takeover)
    report = coordinator.run_once(now=101)
    assert stolen[0] is not None
    assert len(http.creates()) == 1
    assert not any(c[0] == "POST" and "thread_id=" in c[1] for c in http.calls)
    assert report.warnings
    # Once the successor's lease expires, resume this same known mapping.
    monkeypatch.setattr(http.__class__, "__call__", original)
    coordinator.run_once(now=300)
    assert len(http.creates()) == 1
    assert any(c[0] == "POST" and "thread_id=" in c[1] for c in http.calls)
    coordinator.close()
