"""Resume an actual pinned v0.4.1 state file, and read it with the old code again."""

import subprocess
import types
from pathlib import Path

from conftest import add_event, insert_task
from test_multiboard import rig as multiboard_rig

rig = multiboard_rig
BASE = "467284665eb18360e9c32cd8fe4ff76690a66231"


def test_actual_v041_state_adoption_preserves_positions_and_old_reader(rig, tmp_path):
    boards, paths, settings, http, build = rig
    root = Path(__file__).resolve().parents[1]
    source = subprocess.run(
        ["git", "show", f"{BASE}:kanban_task_threads/store.py"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    old = types.ModuleType("pinned_v041_store")
    exec(compile(source, "pinned-v041-store.py", "exec"), old.__dict__)
    states = {}
    for board_name, board in boards.items():
        insert_task(board, "t_same", assignee="worker-any-profile")
        add_event(board, "t_same", "created")
        state_path = tmp_path / f"kanban/plugins/kanban-task-threads/{board_name}.db"
        legacy = old.StateStore(state_path)
        legacy.begin_create(board_name, "t_same", now=1)
        legacy.complete_create(
            board_name,
            "t_same",
            thread_id=f"old-{board_name}",
            message_id=f"starter-{board_name}",
            destination="discord:webhook:123@777",
        )
        legacy.set_task_position(board_name, "t_same", last_event_id=1)
        legacy.advance_cursor(board_name, old=0, new=1)
        assert not legacy._conn.execute(
            "SELECT 1 FROM sqlite_master WHERE name='board_binding'"
        ).fetchone()
        legacy._conn.close()
        states[board_name] = state_path
    coordinator = build()
    report = coordinator.run_once(now=100)
    assert not report.errors
    assert not http.creates()
    assert len(http.calls) == 3  # read-only builder preflights only
    for board in boards.values():
        add_event(board, "t_same", "unblocked", {"actor": "named-worker"})
    report = coordinator.run_once(now=101)
    assert not report.errors
    assert report.replied == 3
    assert not http.creates()
    for board_name in boards:
        assert any(f"thread_id=old-{board_name}" in request[1] for request in http.calls)
    coordinator.close()
    for board_name, path in states.items():
        reader = old.StateStore(path)
        assert reader.get_cursor(board_name) == 2
        post = reader.get_post(board_name, "t_same")
        assert post["thread_id"] == f"old-{board_name}"
        assert post["message_id"] == f"starter-{board_name}"
        assert post["last_event_id"] == 2
        reader._conn.close()
