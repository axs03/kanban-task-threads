"""Independent adversarial acceptance assertions (all values synthetic).

Run only with run_review.py so parent and Python children inherit network guard.
No xfails: an actual credential leak fails the gate.
"""

import json

import pytest
from conftest import add_event, insert_task
from test_blocker_coverage import breaker_payload
from test_first_blocked import setup

from kanban_task_threads.render import failure_detail, render_reply

LABELS = [
    "token",
    "api_key",
    "secret",
    "authorization",
    "credential",
    "password",
    "passwd",
    "api-key",
    "apikey",
]
SENTINEL = "INDEPENDENT_SYNTHETIC_CREDENTIAL_729"


@pytest.mark.parametrize("label", LABELS)
@pytest.mark.parametrize("prefix", ["Bearer ", "BEARER\t", "Bearer\n", ""])
def test_r1_plain(label, prefix):
    raw = f"failed {label}: {prefix}{SENTINEL};suffix,tail retry"
    for rendered in [
        failure_detail(raw),
        render_reply("gave_up", breaker_payload(error=raw)),
        render_reply("gave_up", breaker_payload(error=raw), template="{error}|{reason}"),
    ]:
        assert SENTINEL not in rendered and "suffix" not in rendered, repr(rendered)
        assert "failed" in rendered and "retry" in rendered


@pytest.mark.parametrize(
    "raw",
    [
        f"failed Authorization: Bearer {SENTINEL} retry",
        f"failed token={SENTINEL};end,tail retry",
        f"failed sk_test_{SENTINEL} retry",
        f"failed sk-live-{SENTINEL.replace('_', '-')} retry",
        f"failed https://example.invalid/private/{SENTINEL} retry",
        f"failed token: \x1b[31mBearer\x1b[0m {SENTINEL} retry",
    ],
)
def test_f1_f7_urls_ansi(raw):
    rendered = failure_detail(raw)
    assert SENTINEL not in rendered and SENTINEL.replace("_", "-") not in rendered
    assert "example.invalid" not in rendered and len(rendered) <= 500


@pytest.mark.parametrize("control", ["\x00", "\x07", "\x08", "\x1b", "\u200b"])
@pytest.mark.parametrize(
    "surface", ["detail", "default_reply", "custom_error", "initial_card", "forward_reply"]
)
def test_control_normalization_does_not_strand_labelled_bearer(control, surface, tmp_path):
    # Native operational errors may contain controls; the sanitizer explicitly
    # strips controls, but must not first consume a partial Bearer prefix.
    raw = f"worker failed token: Bea{control}rer {SENTINEL} retry later"
    if surface == "detail":
        out = failure_detail(raw)
    elif surface in ("default_reply", "custom_error"):
        out = render_reply(
            "gave_up",
            breaker_payload(error=raw),
            template="{error}" if surface == "custom_error" else None,
        )
    else:
        board, store, transport, consumer = setup(tmp_path)
        try:
            insert_task(
                board, "synthetic", status="blocked" if surface == "initial_card" else "ready"
            )
            if surface == "forward_reply":
                consumer.run_once(now=100)
                board.execute("UPDATE tasks SET status='blocked' WHERE id='synthetic'")
                board.commit()
            add_event(board, "synthetic", "gave_up", breaker_payload(error=raw))
            report = consumer.run_once(now=101)
            assert report.errors == []
            if surface == "initial_card":
                out = transport.ops("open_thread")[0][2].description
            else:
                out = transport.ops("append")[0][2]
        finally:
            board.close()
            store._conn.close()
    print(
        json.dumps(dict(surface=surface, raw=raw, rendered=out, credential_leaked=SENTINEL in out))
    )
    assert SENTINEL not in out, repr(out)


def test_controls_bounds_mentions_and_safe_template_navigation():
    out = failure_detail(
        "plain\x00 text \x1b[31m red\x1b[0m @everyone <@123> `code` " + "x" * 10000
    )
    assert (
        len(out) <= 500
        and "\x00" not in out
        and "\x1b" not in out
        and "@" not in out
        and "`" not in out
    )
    for template in ["{error.__class__}", "{error[0]}", "{0}", "{error.__class__.__mro__}"]:
        out = render_reply("gave_up", breaker_payload(error="token=" + SENTINEL), template=template)
        assert SENTINEL not in out and "__mro__" not in out


@pytest.mark.parametrize(
    "raw",
    [
        "worker failed token: \x00Bearer SYNTHETIC_BEARER_918 retry",
        "worker failed token: \x07Bearer SYNTHETIC_BEARER_918 retry",
        "worker failed to\x00ken=SYNTHETIC_BEARER_918 retry",
        "worker failed h\x00ttps://example.invalid/SYNTHETIC_BEARER_918 retry",
        "worker failed https\x00://example.invalid/SYNTHETIC_BEARER_918 retry",
    ],
)
def test_strip_controls_before_redaction(raw):
    out = failure_detail(raw)
    print(json.dumps({"raw": raw, "rendered": out, "leak": "SYNTHETIC_BEARER_918" in out}))
    assert "SYNTHETIC_BEARER_918" not in out, repr(out)


@pytest.mark.parametrize("control", ["\x00", "\x07", "\x08", "\x1b", "\u200b"])
def test_normalized_label_url_and_whitespace_boundaries(control):
    for label in LABELS:
        for raw in (
            f"failed {label}: {control}Bearer {SENTINEL} retry",
            f"failed {label[:2]}{control}{label[2:]}={SENTINEL} retry",
            f"failed {label}:\tBearer\n{SENTINEL} retry",
        ):
            out = failure_detail(raw)
            assert SENTINEL not in out
            assert "failed" in out and "retry" in out
    for raw in (
        f"failed h{control}ttps://example.invalid/{SENTINEL} retry",
        f"failed https{control}://example.invalid/{SENTINEL} retry",
    ):
        for out in (
            failure_detail(raw),
            render_reply("gave_up", breaker_payload(error=raw)),
            render_reply("gave_up", breaker_payload(error=raw), template="{error}|{reason}"),
        ):
            assert SENTINEL not in out and "example.invalid" not in out
            assert "failed" in out and "retry" in out
