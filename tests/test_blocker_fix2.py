"""Review-fix regression tests (F1/F2/D1/D2/D3/D4/F4/F7).

Each test names the finding it closes. Written RED-first against the reviewed
working tree; implemented minimally afterwards.
"""

import sqlite3

import pytest
from conftest import add_event, insert_task
from test_blocker_coverage import breaker_payload
from test_first_blocked import setup

from kanban_task_threads.recovery import BlockerRecovery
from kanban_task_threads.render import failure_detail, render_reply
from kanban_task_threads.store import BindingError, StateStore

# --- F1 / F7: failure_detail redaction gaps --------------------------------


@pytest.mark.parametrize(
    "raw",
    [
        "authorization: Bearer eyJhbGciOiJIUzI1NiJ9.secret",
        "Authorization: Bearer abc123 rest",
        "authorization=Bearer sk_test_STRIPEKEY123",
        "Bearer abc123 rest",
        "api_key=SUPERSECRET;foo=bar",
        "token=TOKVAL,other=x",
        "credential=CRED1,CRED2 done",
        "sk_test_STRIPEKEY123",
        "sk_live_ABC123def",
    ],
)
def test_failure_detail_redacts_bearer_auth_stripe_and_punct_terminated(raw):
    out = failure_detail(raw)
    for secret in (
        "eyJhbGciOiJIUzI1NiJ9.secret",
        "abc123",
        "STRIPEKEY123",
        "ABC123def",
        "SUPERSECRET",
        "TOKVAL",
        "CRED1",
        "CRED2",
    ):
        assert secret not in out, (raw, out)


def test_failure_detail_still_redacts_existing_labels_and_urls():
    raw = "api_key=SUPERSECRET bearer abc123 token: OTHERSECRET https://x.example/p"
    out = failure_detail(raw)
    assert "SUPERSECRET" not in out and "abc123" not in out and "OTHERSECRET" not in out
    assert "x.example" not in out


# --- F2: forward-path contradictory gave_up must not pin the cursor ---------


def test_forward_contradictory_gave_up_quarantines_without_cursor_pin_or_spam(tmp_path):
    board, store, transport, consumer = setup(tmp_path)
    insert_task(board, "t")
    consumer.run_once(now=100)
    add_event(board, "t", "completed", {})
    add_event(board, "t", "gave_up", breaker_payload())
    add_event(board, "t", "edited", {"title": "noise cannot erase contradiction"})
    report = consumer.run_once(now=101)
    assert "contradictory" in str(report.errors)
    assert not transport.ops("open_thread")
    rows = [dict(r) for r in store._conn.execute("SELECT * FROM blocker_recovery")]
    assert rows and rows[0]["state"] == "quarantined"
    # Cursor advanced past the poison event: no pin.
    assert store.get_cursor("nib") == 3
    # Next pass: held out of the batch, no error spam.
    report2 = consumer.run_once(now=102)
    assert report2.errors == []
    assert not transport.calls


# --- D1: discovery-time quarantine is surfaced to report.errors -------------


def test_discovery_time_quarantine_is_surfaced_to_report(tmp_path):
    board, store, transport, consumer = setup(tmp_path)
    insert_task(board, "t")
    add_event(board, "t", "completed", {})
    add_event(board, "t", "gave_up", breaker_payload())
    store.activate("nib", 1, [])
    store.advance_cursor("nib", old=0, new=2)
    store.checkpoint_scan("nib", "t", 2)
    report = consumer.run_once(now=100)
    assert "contradictory" in str(report.errors)
    rows = [dict(r) for r in store._conn.execute("SELECT * FROM blocker_recovery")]
    assert rows and rows[0]["state"] == "quarantined"
    assert not transport.ops("open_thread")


# --- D2: manual reconciliation path, never auto-rearm -----------------------


def test_quarantine_has_manual_reconciliation_and_never_auto_rearms(tmp_path):
    board, store, transport, consumer = setup(tmp_path)
    insert_task(board, "t")
    consumer.run_once(now=100)
    add_event(board, "t", "completed", {})
    add_event(board, "t", "gave_up", breaker_payload())
    add_event(board, "t", "edited", {"title": "noise"})
    consumer.run_once(now=101)  # forward quarantine
    rec = BlockerRecovery(store, "nib")
    assert "t" in rec.held_tasks()
    rec.reconcile("t")
    # Reconciliation releases quarantine, not the independent retained catch-up.
    assert rec.quarantined() == []
    row = store._conn.execute(
        "SELECT state, detail FROM blocker_recovery WHERE task_id='t' AND evidence_id=2"
    ).fetchone()
    assert row["state"] == "suppressed"
    # No auto-rearm: a later pass neither re-quarantines nor opens a thread.
    report = consumer.run_once(now=102)
    assert "contradictory" not in str(report.errors)
    assert not transport.ops("open_thread")
    assert "t" not in rec.held_tasks()


# --- D3 / F3: raw unsanitized error scalar removed from template namespace --


def test_gave_up_custom_error_template_cannot_bypass_sanitization():
    out = render_reply(
        "gave_up", breaker_payload(error="api_key=SUPERSECRET raw"), template="{error}"
    )
    assert "SUPERSECRET" not in out


# --- D4: _last_block_reason tolerates malformed gave_up payload -------------


def test_last_block_reason_tolerates_malformed_gave_up_payload(tmp_path):
    board, store, transport, consumer = setup(tmp_path)
    insert_task(board, "t")
    add_event(board, "t", "gave_up", breaker_payload())
    board.execute("UPDATE task_events SET payload='[' WHERE kind='gave_up'")
    board.commit()
    conn = sqlite3.connect(tmp_path / "board.db")
    conn.row_factory = sqlite3.Row
    try:
        assert consumer._last_block_reason(conn, "t") == ""
    finally:
        conn.close()


# --- F4: check_binding covers the blocker ledger tables ---------------------


@pytest.mark.parametrize(
    "table,row",
    [
        ("blocker_migration", "('other', 1, 'id', 3, 3, 1)"),
        ("blocker_recovery", "('other', 1, 't', 1, 'gave_up', '{}', 0, 'pending', '')"),
    ],
)
def test_check_binding_scans_blocker_ledger_tables(tmp_path, table, row):
    store = StateStore(tmp_path / "state.db")
    store._conn.execute(f"INSERT INTO {table} VALUES {row}")
    with pytest.raises(BindingError):
        store.check_binding("nib", "discord:webhook:1@7")
