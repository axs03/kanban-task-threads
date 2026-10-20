"""A kick accelerates the registered central publisher, not a worker's context."""

import contextvars
import sys
import threading

from conftest import add_event, insert_task
from test_multiboard import rig as multiboard_rig
from test_register import ConfiguredCtx

rig = multiboard_rig


def test_fenced_hook_cannot_replace_unfenced_registered_publisher_context(rig, monkeypatch):
    boards, paths, settings, http, build = rig
    for board in boards.values():
        insert_task(board, "t_same", status="blocked")
        add_event(board, "t_same", "blocked")
    fence = contextvars.ContextVar("synthetic-worker-fence", default=False)
    sys.modules["hermes_cli.kanban_db"]._explicit_board_intent_pinned = fence.get
    entry = sys.modules["ktt_entry"]
    monkeypatch.setattr(entry, "_refresh_profile_context", lambda context, **kwargs: context)
    context = ConfiguredCtx("default", dict(settings, poll_seconds=60))
    built = threading.Event()
    original = entry._build_consumer
    outcomes = []

    def observed(ctx):
        outcomes.append(original(ctx))
        built.set()
        return outcomes[-1]

    monkeypatch.setattr(entry, "_build_consumer", observed)
    entry.register(context)  # registration is deliberately not worker-fenced
    try:
        worker = contextvars.copy_context()
        worker.run(fence.set, True)
        worker.run(context.hooks["on_kanban_dispatch_tick"])
        assert built.wait(5)
        assert outcomes[0] is not None
        # Registration's identity/secret refresher still owns the build. The
        # hook's worker fence was not cleared or edited; it simply wasn't adopted.
        assert worker.run(fence.get) is True
    finally:
        for unload in context.unload_callbacks:
            unload()
