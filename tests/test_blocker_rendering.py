"""Latest blocked generation, safe failure detail, current-state rendering."""

from conftest import add_event
from test_blocker_coverage import breaker_payload
from test_blocker_recovery import incident

from kanban_task_threads.render import render_reply


def test_latest_breaker_replaces_old_reason_and_shows_untyped_current_block(tmp_path):
    board, store, transport, consumer = incident(tmp_path)
    consumer.run_once(now=100)
    card = transport.ops("open_thread")[0][2]
    assert "latest worker failure" in card.description
    assert "obsolete" not in card.description
    assert "latest worker failure" in transport.ops("append")[0][2]


def test_resolved_card_does_not_show_stale_block_kind_or_reason(tmp_path):
    board, store, transport, consumer = incident(tmp_path, "ready")
    board.execute("UPDATE tasks SET block_kind='needs_input'")
    board.commit()
    consumer.run_once(now=100)
    card = transport.ops("open_thread")[0][2]
    assert "waiting on:" not in card.description
    assert "latest worker failure" in transport.ops("append")[0][2]


def test_new_block_without_reason_must_not_revive_older_error(tmp_path):
    board, store, transport, consumer = incident(tmp_path)
    add_event(board, "t", "blocked", {"kind": "needs_input"})
    consumer.run_once(now=100)
    assert "latest worker failure" not in transport.ops("open_thread")[0][2].description


def test_automatic_error_detail_is_bounded_sanitized_and_credential_redacted():
    raw = "failure\x1b[31m @everyone https://discord.com/api/webhooks/123/private "
    raw += "api_key=SUPERSECRET bearer abc123 token: OTHERSECRET " + "x" * 1000
    reply = render_reply("gave_up", breaker_payload(error=raw, trigger_outcome="spawn_failed"))
    assert "spawn_failed" in reply
    assert "failure" in reply
    assert "SUPERSECRET" not in reply and "OTHERSECRET" not in reply and "abc123" not in reply
    assert "private" not in reply and "\x1b" not in reply and "@everyone" not in reply
    assert len(reply) <= 650
