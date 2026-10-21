"""R1: every supported credential label must consume its Bearer value in full."""

import pytest
from conftest import add_event, insert_task
from test_blocker_coverage import breaker_payload
from test_first_blocked import setup

from kanban_task_threads.render import failure_detail, render_reply

LABELS = (
    "apikey",
    "api_key",
    "api-key",
    "token",
    "password",
    "passwd",
    "secret",
    "authorization",
    "credential",
)


@pytest.mark.parametrize("label", LABELS)
@pytest.mark.parametrize("case", ["lower", "upper", "title"])
@pytest.mark.parametrize("separator", [": ", "=", " \t: \n"])
@pytest.mark.parametrize("surface", ["detail", "default_reply", "custom_error", "initial_card"])
def test_labelled_bearer_never_escapes_automatic_error_egress(
    tmp_path, label, case, separator, surface
):
    credential = (
        f"{getattr(label, case)()}{separator}{getattr('bearer', case)()}\tR1_SENTINEL;part,tail"
    )
    raw = f"worker spawn failed {credential} retry later @everyone <@123>"
    if surface == "detail":
        out = failure_detail(raw)
    elif surface in ("default_reply", "custom_error"):
        template = "{error}" if surface == "custom_error" else None
        out = render_reply("gave_up", breaker_payload(error=raw), template=template)
        if surface == "default_reply":
            assert "gave up" in out
    else:
        board, store, transport, consumer = setup(tmp_path)
        try:
            insert_task(board, "t", status="blocked")
            add_event(board, "t", "gave_up", breaker_payload(error=raw))
            report = consumer.run_once(now=100)
            assert report.errors == []
            cards = transport.ops("open_thread")
            assert len(cards) == 1
            out = cards[0][2].description
        finally:
            board.close()
            store._conn.close()
    assert "R1_SENTINEL" not in out
    assert "part,tail" not in out
    assert "worker spawn failed" in out and "retry later" in out
    assert "@everyone" not in out and "<@123>" not in out
    assert "[redacted]" in out


@pytest.mark.parametrize("label", LABELS)
@pytest.mark.parametrize("separator", [": ", " =\t"])
def test_plain_labelled_values_still_redact_without_losing_error_context(label, separator):
    out = failure_detail(f"failure {label}{separator}PLAIN_SENTINEL;part,tail retry later")
    assert out == "failure [redacted] retry later"


@pytest.mark.parametrize(
    "credential",
    [
        "Bearer BARE_SENTINEL",
        "sk_test_SYNTHETIC123",
        "sk_live_SYNTHETIC123",
        "sk-SYNTHETIC123",
        "https://example.invalid/private/SYNTHETIC123",
    ],
)
def test_standalone_bearer_urls_and_sk_keys_remain_redacted(credential):
    out = failure_detail(f"failure {credential} retry later")
    assert "SYNTHETIC123" not in out and "BARE_SENTINEL" not in out
    assert "example.invalid" not in out
    assert "failure" in out and "retry later" in out
