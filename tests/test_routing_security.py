"""Delivery-boundary failures are scoped and credential-free."""

import importlib
import sys

import pytest
from conftest import add_event, insert_task
from test_multiboard import rig as multiboard_rig

rig = multiboard_rig


def test_missing_profile_scope_defers_instead_of_disabling_registry(rig):
    boards, paths, settings, http, build = rig
    sys.modules["agent.secret_scope"].get_secret = lambda name: None
    sys.modules["agent.secret_scope"].current_secret_scope = lambda: None
    retryable = importlib.import_module("ktt_entry.kanban_task_threads.runtime").RetryableStartup
    with pytest.raises(retryable):
        build()


@pytest.mark.parametrize(
    "failure", [(400, {"code": 40067}), (500, {}), OSError("secret synthetic-secret-sentinel")]
)
def test_poison_create_never_reroutes_or_blindly_retries(rig, failure):
    boards, paths, settings, http, build = rig
    coordinator = build()
    coordinator.run_once(now=100)
    for board in boards.values():
        insert_task(board, "t_same", status="blocked")
        add_event(board, "t_same", "blocked")
    http.fail = failure
    first = coordinator.run_once(now=101)
    assert len(http.creates()) == 3
    assert first.errors
    second = coordinator.run_once(now=1000)
    assert len(http.creates()) == 3
    assert "synthetic-secret-sentinel" not in repr(first) + repr(second)
    coordinator.close()


def test_transport_error_body_cannot_leak_token_fragment_to_state(rig, tmp_path):
    boards, paths, settings, http, build = rig
    coordinator = build()
    coordinator.run_once(now=100)
    insert_task(boards["default"], "t_same", status="blocked")
    add_event(boards["default"], "t_same", "blocked")
    http.fail = (
        400,
        {"message": "synthetic-secret-sentinel synthetic-bot-sentinel", "code": 40067},
    )
    report = coordinator.run_once(now=101)
    assert "synthetic-secret-sentinel" not in repr(report)
    state = coordinator.consumers[0]._store
    assert "synthetic-secret-sentinel" not in state.get_post("default", "t_same")["detail"]
    coordinator.close()


def test_webhook_resolving_to_another_webhook_id_is_invalid(rig, tmp_path, monkeypatch):
    boards, paths, settings, http, build = rig
    original = http.__class__.__call__

    def inconsistent(self, method, url, body, headers=None):
        if method == "GET":
            return 200, {"id": "999", "channel_id": "777"}
        return original(self, method, url, body, headers)

    monkeypatch.setattr(http.__class__, "__call__", inconsistent)
    assert build() is None
    assert not (tmp_path / "kanban/plugins").exists()


def test_supplied_secret_scope_failure_is_sanitized_retryable(rig):
    boards, paths, settings, http, build = rig

    def unavailable(name):
        raise RuntimeError("synthetic-secret-sentinel")

    sys.modules["agent.secret_scope"].get_secret = unavailable
    retryable = importlib.import_module("ktt_entry.kanban_task_threads.runtime").RetryableStartup
    with pytest.raises(retryable) as error:
        build()
    assert "synthetic-secret-sentinel" not in str(error.value)
    assert not http.calls


def test_bot_preflight_must_establish_actual_forum_type(rig, monkeypatch, tmp_path):
    boards, paths, settings, http, build = rig
    settings["boards"][0]["bot_token_secret"] = "BOT"
    original = http.__class__.__call__

    def bad_type(self, method, url, body, headers=None):
        if method == "GET" and "/channels/" in url:
            return 200, {"id": "777", "type": 0}
        return original(self, method, url, body, headers)

    monkeypatch.setattr(http.__class__, "__call__", bad_type)
    assert build() is None
    assert not (tmp_path / "kanban/plugins").exists()
