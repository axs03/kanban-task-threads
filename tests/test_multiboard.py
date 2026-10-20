"""Offline acceptance through the real builder, coordinator and Discord transport."""

import importlib
import sqlite3
import sys

import pytest
from conftest import add_event, insert_task, make_board
from test_build_consumer import install_fake_hermes
from test_register import ConfiguredCtx, load_entry_point

BOARDS = ("default", "fstream", "nib")
WEBHOOK = "https://discord.com/api/webhooks/123/synthetic-secret-sentinel"


class CannedDiscord:
    def __init__(self):
        self.calls = []
        self.count = 1000
        self.fail = None
        self.forum = "777"

    def __call__(self, method, url, body, headers=None):
        self.calls.append((method, url, body, headers))
        if self.fail and method == "POST":
            failure, self.fail = self.fail, None
            if isinstance(failure, Exception):
                raise failure
            return failure
        if method == "GET" and "/webhooks/" in url:
            return 200, {"id": "123", "channel_id": self.forum, "name": "synthetic"}
        if method == "GET" and "/channels/" in url:
            return 200, {"id": self.forum, "type": 15, "available_tags": []}
        if method == "PATCH" and body and "available_tags" in body:
            return 200, {
                "id": self.forum,
                "type": 15,
                "available_tags": [
                    dict(tag, id=str(20 + i)) for i, tag in enumerate(body["available_tags"])
                ],
            }
        self.count += 1
        return 200, {"id": str(self.count), "channel_id": str(self.count + 10000)}

    def creates(self):
        return [c for c in self.calls if c[0] == "POST" and c[2] and "thread_name" in c[2]]


@pytest.fixture
def rig(monkeypatch, tmp_path):
    install_fake_hermes(
        monkeypatch,
        tmp_path,
        {
            "HOOK": WEBHOOK,
            "HOOK_ALIAS": WEBHOOK,
            "BOT": "synthetic-bot-sentinel",
            "KANBAN_TASK_THREADS_WEBHOOK_URL": WEBHOOK,
            "KANBAN_TASK_THREADS_BOT_TOKEN": "ambient-must-not-inherit",
        },
    )
    db = sys.modules["hermes_cli.kanban_db"]
    paths = {
        b: tmp_path / ("kanban.db" if b == "default" else f"kanban/boards/{b}/kanban.db")
        for b in BOARDS
    }
    boards = {}
    for b, path in paths.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        boards[b] = make_board(path)
    db.kanban_db_path = lambda board=None: paths[board]
    db.board_exists = lambda board: board in paths
    db._explicit_board_intent_pinned = lambda: False
    module = load_entry_point()
    http = CannedDiscord()
    transport = importlib.import_module("ktt_entry.kanban_task_threads.transport")
    monkeypatch.setattr(transport, "urllib_http", http)
    routes = [
        {
            "board": b,
            "forum_channel_id": "777",
            "webhook_secret": "HOOK" if b != "nib" else "HOOK_ALIAS",
        }
        for b in BOARDS
    ]
    settings = {"boards": routes, "thread_creation": "first_blocked", "avatars_enabled": False}

    def build():
        return module._build_consumer(ConfiguredCtx("default", settings))

    return boards, paths, settings, http, build


def test_shared_forum_three_real_boards_colliding_ids_restart(rig, tmp_path):
    boards, paths, settings, http, build = rig
    for board in boards.values():
        insert_task(board, "t_same", assignee="other-profile")
        add_event(board, "t_same", "created")
    coordinator = build()
    assert coordinator is not None
    report = coordinator.run_once(now=100)
    assert not report.errors
    assert not http.creates()
    for board in boards.values():
        add_event(board, "t_same", "blocked", {"actor": "worker-profile", "reason": "decision"})
        add_event(board, "t_same", "unblocked", {"actor": "owner"})
    report = coordinator.run_once(now=101)
    assert not report.errors
    assert len(http.creates()) == 3
    names = [c[2]["thread_name"] for c in http.creates()]
    assert all(any(f"{b}:t_same" in n for n in names) for b in BOARDS)
    states = {}
    for b in BOARDS:
        path = tmp_path / f"kanban/plugins/kanban-task-threads/{b}.db"
        with sqlite3.connect(path) as conn:
            states[b] = conn.execute(
                "SELECT thread_id, last_event_id FROM posts WHERE board=?", (b,)
            ).fetchone()
            assert (
                conn.execute("SELECT last_event_id FROM cursor WHERE board=?", (b,)).fetchone()[0]
                == 3
            )
        assert states[b][1] == 3
    assert len({row[0] for row in states.values()}) == 3
    coordinator.close()
    again = build()
    assert again is not None
    again.run_once(now=102)
    for board in boards.values():
        add_event(board, "t_same", "blocked", {"actor": "new-assignee"})
    again.run_once(now=103)
    assert len(http.creates()) == 3
    count = len(http.calls)
    again.run_once(now=104)
    assert len(http.calls) == count
    again.close()


@pytest.mark.parametrize("value", [None, [], {}, "", True, ["default"], [{"board": "default"}]])
def test_explicit_invalid_boards_never_falls_back(rig, tmp_path, value):
    boards, paths, settings, http, build = rig
    settings["boards"] = value
    assert build() is None
    assert not http.calls
    assert not (tmp_path / "kanban/plugins").exists()


@pytest.mark.parametrize(
    "change",
    [
        {"board": "NIB"},
        {"board": "../nib"},
        {"board": True},
        {"forum_channel_id": True},
        {"forum_channel_id": " 777"},
        {"webhook_secret": WEBHOOK},
        {"webhook_secret": "${HOOK}"},
        {"webhook_secret": "MISSING"},
        {"bot_token_secret": "MISSING"},
        {"extra": "ignored?"},
        {"discord_applied_tag_ids": [True]},
    ],
)
def test_bad_route_fails_closed_without_partial_activation(rig, tmp_path, change):
    boards, paths, settings, http, build = rig
    settings["boards"][-1].update(change)
    assert build() is None
    assert not any(c[0] != "GET" for c in http.calls)
    assert not (tmp_path / "kanban/plugins").exists()


def test_duplicate_board_is_invalid_but_shared_endpoint_is_valid(rig, tmp_path):
    boards, paths, settings, http, build = rig
    settings["boards"].append(dict(settings["boards"][0]))
    assert build() is None
    assert not http.calls


def test_mismatch_discovered_forum_prevents_all_state_writes(rig, tmp_path):
    boards, paths, settings, http, build = rig
    settings["boards"][-1]["forum_channel_id"] = "999"
    assert build() is None
    assert not (tmp_path / "kanban/plugins").exists()


def test_board_failure_does_not_starve_next_route(rig):
    boards, paths, settings, http, build = rig
    coordinator = build()
    coordinator.run_once(now=100)
    for board in boards.values():
        insert_task(board, "t_same", status="blocked")
        add_event(board, "t_same", "blocked")
    http.fail = (429, {"retry_after": 2})
    coordinator.run_once(now=101)
    assert len(http.creates()) == 3  # first failed, other two succeeded
    coordinator.run_once(now=104)
    assert len(http.creates()) == 4
    coordinator.close()


def test_secrets_not_in_error_logs_or_state(rig, tmp_path, caplog):
    boards, paths, settings, http, build = rig

    def explode(*args, **kwargs):
        raise OSError(WEBHOOK + " synthetic-bot-sentinel")

    importlib.import_module("ktt_entry.kanban_task_threads.transport").urllib_http = explode
    retryable = importlib.import_module("ktt_entry.kanban_task_threads.runtime").RetryableStartup
    with pytest.raises(retryable) as error:
        build()
    text = str(error.value) + caplog.text
    assert "synthetic-secret-sentinel" not in text
    assert "synthetic-bot-sentinel" not in text


@pytest.mark.parametrize(
    "alias", ["symlink", "hardlink", "resolver", "state-native", "state-state"]
)
def test_database_aliases_are_rejected_before_preflight(rig, tmp_path, alias):
    import os

    boards, paths, settings, http, build = rig
    if alias in ("symlink", "hardlink"):
        boards["nib"].close()
        paths["nib"].unlink()
        if alias == "symlink":
            paths["nib"].symlink_to(paths["default"])
        else:
            os.link(paths["default"], paths["nib"])
    elif alias == "resolver":
        sys.modules["hermes_cli.kanban_db"].kanban_db_path = lambda board: paths["default"]
    else:
        root = tmp_path / "kanban/plugins/kanban-task-threads"
        root.mkdir(parents=True)
        if alias == "state-native":
            os.link(paths["default"], root / "nib.db")
        else:
            (root / "default.db").touch()
            os.link(root / "default.db", root / "nib.db")
    assert build() is None
    assert not http.calls


def test_replaced_native_file_freezes_only_its_board(rig):
    boards, paths, settings, http, build = rig
    coordinator = build()
    coordinator.run_once(now=100)
    boards["default"].close()
    paths["default"].rename(paths["default"].with_suffix(".old"))
    boards["default"] = make_board(paths["default"])
    for board in boards.values():
        insert_task(board, "t_same", status="blocked")
        add_event(board, "t_same", "blocked")
    coordinator.run_once(now=101)
    assert len(http.creates()) == 2
    assert not any("default:t_same" in c[2]["thread_name"] for c in http.creates())
    coordinator.close()


@pytest.mark.parametrize(
    "foreign",
    ["post", "cursor", "binding", "pending", "unknown-destination", "changed-destination"],
)
def test_state_ownership_and_legacy_uncertainty_freeze_all_effect_paths(rig, tmp_path, foreign):
    boards, paths, settings, http, build = rig
    for board in boards.values():
        insert_task(board, "t_same", status="blocked")
        add_event(board, "t_same", "blocked")
    coordinator = build()
    state = coordinator.consumers[0]._store
    if foreign == "post":
        state.begin_create("other", "t_same", now=1)
    elif foreign == "cursor":
        state.advance_cursor("other", old=0, new=9)
    elif foreign == "binding":
        state._conn.execute(
            "CREATE TABLE IF NOT EXISTS board_binding("
            "board TEXT PRIMARY KEY, destination TEXT NOT NULL)"
        )
        state._conn.execute("INSERT INTO board_binding VALUES ('other', 'discord:webhook:123@777')")
    else:
        state.begin_create("default", "t_same", now=1)
        if foreign != "pending":
            state.complete_create(
                "default",
                "t_same",
                thread_id="existing",
                message_id="starter",
                destination="" if foreign == "unknown-destination" else "discord:webhook:123@999",
            )
            state.set_card_dirty("default", "t_same", True)
    report = coordinator.run_once(now=100)
    assert report.errors
    assert not any("default:t_same" in c[2]["thread_name"] for c in http.creates())
    assert len(http.creates()) == 2
    assert not any("thread_id=existing" in c[1] for c in http.calls)
    assert state.get_cursor("default") == 0
    coordinator.close()


def test_consistent_legacy_mapping_resumes_same_thread_cursor(rig, tmp_path):
    boards, paths, settings, http, build = rig
    for board in boards.values():
        insert_task(board, "t_same")
        add_event(board, "t_same", "created")
    coordinator = build()
    state = coordinator.consumers[0]._store
    state.begin_create("default", "t_same", now=1)
    state.complete_create(
        "default",
        "t_same",
        thread_id="legacy-thread",
        message_id="legacy-starter",
        destination="discord:webhook:123@777",
    )
    state.set_task_position("default", "t_same", last_event_id=1)
    state.advance_cursor("default", old=0, new=1)
    add_event(boards["default"], "t_same", "blocked", {"actor": "a-worker"})
    coordinator.run_once(now=100)
    assert not http.creates()
    assert any("thread_id=legacy-thread" in call[1] and call[0] == "POST" for call in http.calls)
    assert state.get_post("default", "t_same")["thread_id"] == "legacy-thread"
    assert state.get_cursor("default") == 2
    assert (
        state._conn.execute("SELECT board, destination FROM board_binding").fetchall()[0][0]
        == "default"
    )
    coordinator.close()


@pytest.mark.parametrize("key", ["HERMES_KANBAN_TASK", "HERMES_DELEGATED_CHILD_CONTEXT"])
def test_fenced_context_does_not_clear_isolation(rig, monkeypatch, key):
    boards, paths, settings, http, build = rig
    monkeypatch.setenv(key, "synthetic-fence")
    assert build() is None
    assert not http.calls


def test_destination_rotation_keeps_binding_but_route_change_freezes(rig):
    boards, paths, settings, http, build = rig
    coordinator = build()
    coordinator.run_once(now=100)
    coordinator.close()
    sys.modules["agent.secret_scope"].get_secret = lambda name: WEBHOOK.replace(
        "synthetic-secret-sentinel", "rotated"
    )
    again = build()
    again.run_once(now=101)
    again.close()
    for board in boards.values():
        insert_task(board, "t_same", status="blocked")
        add_event(board, "t_same", "blocked")
    for route in settings["boards"]:
        route["forum_channel_id"] = "888"
    http.forum = "888"
    changed = build()
    report = changed.run_once(now=102)
    assert len(report.errors) == 3
    assert all("frozen" in error for error in report.errors)
    assert not http.creates()
    changed.close()


def test_mixed_bot_and_webhook_routes_never_inherit_global_token(rig):
    boards, paths, settings, http, build = rig
    settings["boards"][0]["bot_token_secret"] = "BOT"
    coordinator = build()
    assert coordinator is not None
    assert "tags" in coordinator.consumers[0]._transport.capabilities()
    assert all("tags" not in c._transport.capabilities() for c in coordinator.consumers[1:])
    coordinator.run_once(now=100)
    assert all(
        (call[3] or {}).get("Authorization") in (None, "Bot synthetic-bot-sentinel")
        for call in http.calls
    )
    coordinator.close()


def test_shared_forum_setup_serialized_across_processes(rig, tmp_path, monkeypatch):
    import multiprocessing
    import time

    boards, paths, settings, http, build = rig
    context = multiprocessing.get_context("fork")
    active, maximum = context.Value("i", 0), context.Value("i", 0)
    barrier = context.Barrier(2)
    errors = context.Queue()

    # Separate boards own separate leases; setup must still serialize by forum.
    def worker(board):
        gets = 0

        def fake(method, url, body, headers=None):
            nonlocal gets
            if method == "GET" and "/channels/" in url:
                gets += 1
                if gets == 2:  # prepare_forum, after read-only builder preflight
                    with active.get_lock():
                        active.value += 1
                        maximum.value = max(maximum.value, active.value)
                    time.sleep(0.2)
                return 200, {"id": "777", "type": 15, "available_tags": []}
            if method == "PATCH" and body and "available_tags" in body:
                with active.get_lock():
                    active.value -= 1
                return 200, {
                    "id": "777",
                    "available_tags": [
                        dict(tag, id=str(i + 10)) for i, tag in enumerate(body["available_tags"])
                    ],
                }
            return http(method, url, body, headers)

        importlib.import_module("ktt_entry.kanban_task_threads.transport").urllib_http = fake
        route = dict(settings["boards"][BOARDS.index(board)], bot_token_secret="BOT")
        module = sys.modules["ktt_entry"]
        coordinator = module._build_consumer(
            ConfiguredCtx("default", dict(settings, boards=[route]))
        )
        barrier.wait(timeout=10)
        report = coordinator.run_once(now=100)
        errors.put(report.errors + report.warnings)
        coordinator.close()

    processes = [context.Process(target=worker, args=(board,)) for board in ("default", "nib")]
    for p in processes:
        p.start()
    for p in processes:
        p.join(timeout=20)
        if p.is_alive():
            p.kill()
            p.join()
        assert p.exitcode == 0
    assert errors.get(timeout=2) == []
    assert errors.get(timeout=2) == []
    assert maximum.value == 1
    assert active.value == 0


def test_register_no_hook_runtime_polls_three_boards_and_closes_state(rig, monkeypatch):
    import threading

    boards, paths, settings, http, build = rig
    for board in boards.values():
        insert_task(board, "t_same", status="blocked")
        add_event(board, "t_same", "blocked")
    module = sys.modules["ktt_entry"]
    built, posted = [], threading.Event()
    original_build = module._build_consumer

    def capture(ctx):
        coordinator = original_build(ctx)
        built.append(coordinator)
        return coordinator

    original = http.__class__.__call__

    def observe(self, *args, **kwargs):
        response = original(self, *args, **kwargs)
        if len(self.creates()) == 3:
            posted.set()
        return response

    monkeypatch.setattr(http.__class__, "__call__", observe)
    monkeypatch.setattr(module, "_build_consumer", capture)
    monkeypatch.setattr(module, "_refresh_profile_context", lambda context, **kw: context)
    ctx = ConfiguredCtx("default", dict(settings, poll_seconds=0.01, publisher_profile="default"))
    module.register(ctx)
    try:
        assert posted.wait(10)
    finally:
        for close in ctx.unload_callbacks:
            close()
    assert len(http.creates()) == 3
    assert len(built) == 1
    with pytest.raises(sqlite3.ProgrammingError):
        built[0].consumers[0]._store.get_cursor("default")


@pytest.mark.parametrize("phase", ["build", "pass"])
def test_unload_during_build_or_pass_closes_without_starting_more_boards(rig, phase):
    import threading

    boards, paths, settings, http, build = rig
    for board in boards.values():
        insert_task(board, "t_same", status="blocked")
        add_event(board, "t_same", "blocked")
    runtime_module = importlib.import_module("ktt_entry.kanban_task_threads.runtime")
    entered, release = threading.Event(), threading.Event()
    built = []

    def slow_build():
        coordinator = build()
        built.append(coordinator)
        if phase == "build":
            entered.set()
            assert release.wait(10)
        else:
            real = coordinator.consumers[0]._transport._http

            def slow_http(method, url, body, headers=None):
                if method == "POST" and body and "thread_name" in body:
                    entered.set()
                    assert release.wait(10)
                return real(method, url, body, headers=headers)

            coordinator.consumers[0]._transport._http = slow_http
        return coordinator

    runtime = runtime_module.Runtime(slow_build, poll_seconds=0.01)
    runtime.start()
    assert entered.wait(10)
    stop = threading.Thread(target=runtime.shutdown)
    stop.start()
    assert runtime._stopping.wait(2)
    release.set()
    stop.join(timeout=15)
    assert not stop.is_alive()
    assert not runtime.alive()
    assert len(http.creates()) == (0 if phase == "build" else 1)
    with pytest.raises(sqlite3.ProgrammingError):
        built[0].consumers[0]._store.get_cursor("default")
