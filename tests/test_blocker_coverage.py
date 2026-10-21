"""Audited native blocker families, never presentation or retry signals."""

import pytest
from conftest import add_event, insert_task
from test_first_blocked import setup


def test_unrecognized_or_incomplete_breaker_payload_is_not_proof(tmp_path):
    board, store, transport, consumer = setup(tmp_path)
    insert_task(board, "t")
    consumer.run_once(now=100)
    add_event(board, "t", "gave_up", {"status": "blocked", "error": "intent alone"})
    report = consumer.run_once(now=101)
    assert not transport.calls
    assert "incomplete" in str(report.errors)
    assert store.scan_position("nib", "t") == 0


def breaker_payload(**extra):
    return {
        "failures": 1,
        "effective_limit": 1,
        "limit_source": "dispatcher",
        "error": "no worker",
        "trigger_outcome": "crashed",
        "retry_status": "ready",
        **extra,
    }


@pytest.mark.parametrize(
    "prior,payload",
    [
        ("completed", {}),
        ("archived", {}),
        ("scheduled", {}),
        ("dependency_wait", {}),
        ("block_loop_detected", {}),
        ("status", {"status": "triage"}),
        ("created", {"status": "todo"}),
        ("changes_requested", {"status": "todo"}),
    ],
)
def test_gave_up_rowcount_race_contradictory_history_is_quarantined(tmp_path, prior, payload):
    board, store, transport, consumer = setup(tmp_path)
    insert_task(board, "t")
    consumer.run_once(now=100)
    add_event(board, "t", prior, payload)
    add_event(board, "t", "gave_up", breaker_payload())
    add_event(board, "t", "edited", {"title": "noise cannot erase contradiction"})
    report = consumer.run_once(now=101)
    assert not transport.ops("open_thread")
    assert report.errors and "contradictory" in str(report.errors)
    assert store.scan_position("nib", "t") == 0


@pytest.mark.parametrize("raw", ["[]", "{", "null", '"blocked"'])
def test_malformed_evidence_is_not_checkpointed_away(tmp_path, raw):
    board, store, transport, consumer = setup(tmp_path)
    insert_task(board, "t")
    consumer.run_once(now=100)
    add_event(board, "t", "gave_up", breaker_payload())
    board.execute("UPDATE task_events SET payload=?", (raw,))
    board.commit()
    assert consumer.run_once(now=101).errors
    assert not transport.ops("open_thread")
    assert store.scan_position("nib", "t") == 0


@pytest.mark.parametrize("outcome", ["spawn_failed", "timed_out", "reclaimed", "crashed"])
@pytest.mark.parametrize(
    "extra",
    [
        {},
        {"sticky": True},
        {"terminal_provider": True},
        {"protocol_violations": 2, "protocol_violation_limit": 2},
    ],
)
def test_native_breaker_opens_even_after_rapid_resolution(tmp_path, outcome, extra):
    board, store, transport, consumer = setup(tmp_path)
    insert_task(board, "t", status="ready")
    consumer.run_once(now=100)
    add_event(
        board,
        "t",
        "gave_up",
        {
            "failures": 1,
            "effective_limit": 99,
            "limit_source": "task",
            "error": "worker unavailable",
            "trigger_outcome": outcome,
            "retry_status": "review",
            **extra,
        },
    )
    add_event(board, "t", "completed", {"summary": "resolved"})
    board.execute("UPDATE tasks SET status='done'")
    board.commit()
    report = consumer.run_once(now=101)
    assert report.errors == []
    assert len(transport.ops("open_thread")) == 1
    assert transport.message_types == ["gave_up", "completed"]
    consumer.run_once(now=102)
    assert len(transport.ops("open_thread")) == 1
    assert store.get_post("nib", "t")["last_event_id"] == 2


@pytest.mark.parametrize(
    "kind",
    [
        "dependency_wait",
        "block_loop_detected",
        "review_requested",
        "changes_requested",
        "review_reopened",
        "rate_limited",
        "spawn_failed",
        "timed_out",
        "crashed",
        "reclaimed",
        "protocol_violation",
        "completion_blocked_hallucination",
        "completion_blocked_empty_result",
        "imported",
        "status",
        "future_blocked",
    ],
)
def test_nonblocker_never_opens(tmp_path, kind):
    board, store, transport, consumer = setup(tmp_path)
    insert_task(board, "t")
    consumer.run_once(now=100)
    add_event(board, "t", kind, {"status": "blocked", "outcome": "blocked", "infrastructure": True})
    consumer.run_once(now=101)
    assert not transport.ops("open_thread")
    assert store.get_post("nib", "t") is None
