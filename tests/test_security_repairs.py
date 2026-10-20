"""Security repair regressions (independent review R1-R4).

Pin the four security/isolation defects the security review reproduced:
bot Authorization leakage across redirects, SQLite URI path confusion,
forum-lock parent replacement, and reconcile sidecar aliasing. No network;
synthetic credentials and fixtures only.
"""

import hashlib
import importlib
import io
import multiprocessing
import os
import sqlite3
import sys
import urllib.request
from email.message import Message
from pathlib import Path
from urllib.response import addinfourl

import pytest
from conftest import add_event, insert_task, make_board
from test_multiboard import rig as multiboard_rig

import kanban_task_threads.transport as transport_mod

rig = multiboard_rig


# --- S-R1: bot Authorization must never follow a redirect ---------------------


def _redirect_responder(calls, destination):
    def respond(request):
        calls.append((request.full_url, request.get_header("Authorization")))
        headers = Message()
        if len(calls) == 1:
            headers["Location"] = destination
            response = addinfourl(io.BytesIO(b""), headers, request.full_url, 302)
            response.msg = "Found"
            return response
        response = addinfourl(io.BytesIO(b'{"id":"1"}'), headers, request.full_url, 200)
        response.msg = "OK"
        return response

    return respond


@pytest.mark.parametrize(
    "destination",
    [
        "https://collector.invalid/capture",  # cross-origin
        "http://discord.com/api/v10/channels/777",  # HTTPS -> HTTP downgrade
    ],
)
def test_authenticated_request_never_follows_redirect(monkeypatch, destination):
    calls = []
    respond = _redirect_responder(calls, destination)

    class SyntheticHTTPS(urllib.request.HTTPSHandler):
        def https_open(self, request):
            return respond(request)

    class SyntheticHTTP(urllib.request.HTTPHandler):
        def http_open(self, request):
            return respond(request)

    opener = urllib.request.build_opener(
        transport_mod._RejectRedirects(), SyntheticHTTPS(), SyntheticHTTP()
    )
    monkeypatch.setattr(transport_mod._NO_REDIRECT_OPENER, "open", opener.open)
    status, _ = transport_mod.urllib_http(
        "GET",
        "https://discord.com/api/v10/channels/777",
        None,
        headers={"Authorization": "Bot synthetic-bot"},
    )
    assert len(calls) == 1, "the redirect must never issue a second request"
    assert calls[0] == ("https://discord.com/api/v10/channels/777", "Bot synthetic-bot")
    assert status == 302, "the refused redirect surfaces as the 3xx response"


# --- S-R2: SQLite URI must open the validated native DB -----------------------


@pytest.mark.parametrize("separator", ["?", "#"])
def test_native_uri_opens_validated_database_not_decoy(rig, tmp_path, separator):
    _, _, settings, http, build = rig
    settings["boards"] = [settings["boards"][0]]
    root = tmp_path / ("native-root" + separator + "opaque")
    root.mkdir()
    legitimate = root / "kanban.db"
    make_board(legitimate).close()
    decoy = make_board(tmp_path / "native-root")
    insert_task(decoy, "foreign-card", title="CONFIDENTIAL-DECOY-NATIVE-BOARD", status="blocked")
    add_event(decoy, "foreign-card", "blocked")
    decoy.close()
    db = sys.modules["hermes_cli.kanban_db"]
    db.kanban_home = lambda: root
    db.kanban_db_path = lambda board=None: legitimate
    coordinator = build()
    assert coordinator is not None
    try:
        coordinator.run_once(now=100)
        assert not http.creates(), "the decoy prefix database must never be consumed"
        with sqlite3.connect(legitimate) as conn:
            assert conn.execute("SELECT count(*) FROM tasks").fetchone()[0] == 0
    finally:
        coordinator.close()


# --- S-R3: forum lock must not split across parent replacement ---------------


def test_forum_lock_pinned_parent_refuses_after_replacement(tmp_path):
    coordination = importlib.import_module("kanban_task_threads.coordination")
    root = tmp_path / "state-root"
    root.mkdir()
    lock = root / "forum-777.lock"
    pinned = (root.stat().st_dev, root.stat().st_ino)
    with coordination.forum_lock(lock, parent=pinned):
        root.rename(tmp_path / "old-root")
        with pytest.raises(RuntimeError, match="replaced|changed"):
            with coordination.forum_lock(lock, parent=pinned):
                pass  # a second holder must never enter a replaced parent


def test_forum_lock_no_simultaneous_holders_across_parent_replacement(tmp_path):
    coordination = importlib.import_module("kanban_task_threads.coordination")
    root = tmp_path / "state-root"
    root.mkdir()
    lock = root / "forum-777.lock"
    pinned = (root.stat().st_dev, root.stat().st_ino)
    context = multiprocessing.get_context("fork")
    outcome = context.Queue()

    def second():
        try:
            with coordination.forum_lock(lock, parent=pinned, timeout=1):
                outcome.put("entered")
        except Exception as exc:  # noqa: BLE001 — recording the refusal type
            outcome.put(("refused", type(exc).__name__))

    with coordination.forum_lock(lock, parent=pinned):
        root.rename(tmp_path / "old-root")
        process = context.Process(target=second)
        process.start()
        process.join(5)
        if process.is_alive():
            process.kill()
            process.join()
        assert process.exitcode == 0
        result = outcome.get(timeout=2)
        assert result != "entered", "a second holder must never enter after replacement"
        assert isinstance(result, tuple) and result[0] == "refused"


def test_shared_forum_lock_is_pinned_to_state_root(rig):
    boards, paths, settings, http, build = rig
    settings["boards"] = settings["boards"][:2]
    for route in settings["boards"]:
        route["bot_token_secret"] = "BOT"
    coordinator = build()
    assert coordinator is not None
    try:
        for consumer in coordinator.consumers:
            lock = consumer._forum_lock
            assert lock is not None
            lock_path = Path(lock.args[0])
            state_root = lock_path.parent
            assert state_root.name == "kanban-task-threads", "lock must not live in a subdir"
            assert lock.keywords.get("parent") == (
                state_root.stat().st_dev,
                state_root.stat().st_ino,
            ), "lock parent identity must be pinned to the state root"
    finally:
        coordinator.close()


# --- S-R4: reconcile must refuse aliased SQLite sidecars ----------------------


@pytest.mark.parametrize(
    "mode,suffix,kind",
    [
        ("WAL", "-wal", "hardlink"),
        ("WAL", "-wal", "symlink"),
        ("DELETE", "-journal", "hardlink"),
        ("DELETE", "-journal", "symlink"),
        ("WAL", "-shm", "hardlink"),
        ("WAL", "-shm", "symlink"),
    ],
)
def test_recreate_refuses_aliased_sidecar(tmp_path, mode, suffix, kind):
    from kanban_task_threads import reconcile
    from kanban_task_threads.store import StateStore

    state = tmp_path / "state.db"
    store = StateStore(state)
    store.begin_create("default", "same", now=1)
    store._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    store._conn.execute("PRAGMA journal_mode=" + mode)
    store._conn.close()
    native = tmp_path / "native.db"
    board = make_board(native)
    insert_task(board, "important-native-card", status="ready")
    board.close()
    before = hashlib.sha256(native.read_bytes()).hexdigest()
    sidecar = Path(str(state) + suffix)
    if kind == "hardlink":
        os.link(native, sidecar)
    else:
        sidecar.symlink_to(native)
    with pytest.raises(reconcile.ReconcileError):
        reconcile.recreate(state, "default", "same")
    after = hashlib.sha256(native.read_bytes()).hexdigest()
    assert before == after, "recreate must never mutate a native DB via a sidecar alias"
