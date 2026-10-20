"""Additional fail-closed schema and path boundary cases."""

import sys

import pytest
from test_multiboard import rig as multiboard_rig

rig = multiboard_rig


@pytest.mark.parametrize(
    "change",
    [
        {"board": ""},
        {"board": " nib"},
        {"board": "unknown"},
        {"forum_channel_id": 777},
        {"forum_channel_id": "0"},
        {"forum_channel_id": "777\n"},
        {"webhook_secret": None},
        {"bot_token_secret": None},
        {"discord_applied_tag_ids": None},
        {"discord_applied_tag_ids": ["1", "1"]},
        {"dashboard_url": []},
        {"dashboard_url": "https://user:password@example.invalid"},
    ],
)
def test_malformed_entry_rejects_entire_registry(rig, tmp_path, change):
    boards, paths, settings, http, build = rig
    settings["boards"][-1].update(change)
    assert build() is None
    assert not http.calls
    assert not (tmp_path / "kanban/plugins").exists()


@pytest.mark.parametrize(
    "conflict",
    [
        {"board": "default"},
        {"discord_applied_tag_ids": ["1"]},
        {"thread_creation": None},
        {"thread_creation": True},
        {"thread_creation": "FIRST_BLOCKED"},
    ],
)
def test_global_route_conflicts_do_not_select_legacy(rig, conflict):
    boards, paths, settings, http, build = rig
    settings.update(conflict)
    assert build() is None
    assert not http.calls


def test_invalid_native_schema_has_no_preflight_or_state_side_effect(rig, tmp_path):
    boards, paths, settings, http, build = rig
    boards["nib"].execute("DROP TABLE task_events")
    boards["nib"].commit()
    assert build() is None
    assert not http.calls
    assert not (tmp_path / "kanban/plugins").exists()


@pytest.mark.parametrize("suffix", ["-wal", "-shm", "-journal"])
def test_state_sidecar_alias_cannot_write_into_native_board(rig, tmp_path, suffix):
    boards, paths, settings, http, build = rig
    state = tmp_path / "kanban/plugins/kanban-task-threads"
    state.mkdir(parents=True)
    (state / ("nib.db" + suffix)).symlink_to(paths["default"])
    assert build() is None
    assert not http.calls


def test_separate_explicit_destinations_remain_supported(rig, monkeypatch):
    boards, paths, settings, http, build = rig
    scope = sys.modules["agent.secret_scope"]
    original_secret = scope.get_secret
    settings["boards"][-1].update(forum_channel_id="888", webhook_secret="NIB_HOOK")
    scope.get_secret = lambda name: (
        "https://discord.com/api/webhooks/456/other-synthetic-token"
        if name == "NIB_HOOK"
        else original_secret(name)
    )
    original_http = http.__class__.__call__

    def separate(self, method, url, body, headers=None):
        if method == "GET" and "/456/" in url:
            self.calls.append((method, url, body, headers))
            return 200, {"id": "456", "channel_id": "888"}
        return original_http(self, method, url, body, headers)

    monkeypatch.setattr(http.__class__, "__call__", separate)
    coordinator = build()
    assert coordinator is not None
    coordinator.run_once(now=100)
    assert [c._destination for c in coordinator.consumers] == [
        "discord:webhook:123@777",
        "discord:webhook:123@777",
        "discord:webhook:456@888",
    ]
    coordinator.close()
