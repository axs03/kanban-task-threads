"""Maintenance failures must not erase activation or affect another route."""

from conftest import add_event, insert_task
from test_multiboard import rig as multiboard_rig

rig = multiboard_rig


def test_failed_forum_setup_does_not_lose_initial_block_obligation(rig, monkeypatch):
    boards, paths, settings, http, build = rig
    settings["boards"][0]["bot_token_secret"] = "BOT"
    insert_task(boards["default"], "t_same", status="blocked")
    add_event(boards["default"], "t_same", "blocked")
    original = http.__class__.__call__
    channel_reads = 0

    def flaky_setup(self, method, url, body, headers=None):
        nonlocal channel_reads
        if method == "GET" and "/channels/" in url:
            channel_reads += 1
            if channel_reads == 2:
                return 403, {"message": "no setup permission yet"}
        return original(self, method, url, body, headers)

    monkeypatch.setattr(http.__class__, "__call__", flaky_setup)
    coordinator = build()
    report = coordinator.run_once(now=100)
    assert report.errors
    assert not http.creates()
    boards["default"].execute("UPDATE tasks SET status='ready'")
    boards["default"].commit()
    add_event(boards["default"], "t_same", "unblocked")
    coordinator.run_once(now=101)
    assert len(http.creates()) == 1
    assert "default:t_same" in http.creates()[0][2]["thread_name"]
    coordinator.close()


def test_missing_thread_is_parked_without_cross_board_replacement(rig):
    boards, paths, settings, http, build = rig
    for board in boards.values():
        insert_task(board, "t_same", status="blocked")
        add_event(board, "t_same", "blocked")
    coordinator = build()
    coordinator.run_once(now=100)
    assert len(http.creates()) == 3
    for board in boards.values():
        add_event(board, "t_same", "unblocked")
    http.fail = (404, {"code": 10003})
    report = coordinator.run_once(now=101)
    assert len(report.errors) == 1
    states = [
        consumer._store.get_post(consumer._board, "t_same")["state"]
        for consumer in coordinator.consumers
    ]
    assert sorted(states) == ["live", "live", "tombstone"]
    for board in boards.values():
        add_event(board, "t_same", "blocked")
    coordinator.run_once(now=102)
    assert len(http.creates()) == 3
    coordinator.close()
