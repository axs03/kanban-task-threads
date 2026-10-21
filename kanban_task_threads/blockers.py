"""Explicit installed-native blocker contract; no labels or counter heuristics."""

import json

BLOCKER_KINDS = frozenset({"blocked", "gave_up"})
CLASSIFIER_VERSION = 1


class BlockerEvidenceError(ValueError):
    """Retained blocker evidence needs operator inspection, not a skipped scan."""


def event_payload(row):
    try:
        payload = json.loads(row["payload"]) if row["payload"] else {}
    except (TypeError, ValueError) as exc:
        raise BlockerEvidenceError("malformed blocker payload") from exc
    if not isinstance(payload, dict):
        raise BlockerEvidenceError("blocker payload is not an object")
    return payload


def is_blocker(conn, row):
    """Classify audited families while preserving the original row for replay."""
    if row["kind"] not in BLOCKER_KINDS:
        return False
    payload = event_payload(row)
    if row["kind"] == "gave_up":
        if (
            payload.get("trigger_outcome")
            not in ("spawn_failed", "crashed", "timed_out", "reclaimed")
            or not isinstance(payload.get("error"), str)
            or payload.get("retry_status") not in ("ready", "review")
            or any(type(payload.get(key)) is not int for key in ("failures", "effective_limit"))
            or payload.get("limit_source") not in ("task", "dispatcher")
        ):
            raise BlockerEvidenceError("incomplete or unsupported native gave_up evidence")
        # Native failure accounting does not check UPDATE rowcount. Look BACK
        # from the event, never at today's state: a later resolution is valid.
        # A preceding committed incompatible state is contradictory evidence.
        states = {
            "created": None,
            "status": None,
            "changes_requested": None,
            "review_reopened": None,
            "unblocked": "ready",
            "promoted": "ready",
            "promoted_manual": "ready",
            "claimed": "running",
            "blocked": "blocked",
            "gave_up": "blocked",
            "review_requested": "review",
            "completed": "done",
            "archived": "archived",
            "scheduled": "scheduled",
            "dependency_wait": "todo",
            "block_loop_detected": "triage",
            "descendant_invalidated": "todo",
            "timed_out": "ready",
            "reclaimed": "ready",
            "crashed": "ready",
            "protocol_violation": "ready",
            "stale": "ready",
            "reconciled": "ready",
            "rate_limited": "ready",
            "spawn_failed": "ready",
        }
        prior = conn.execute(
            "SELECT kind, payload FROM task_events WHERE task_id=? AND id<? AND kind IN ("
            + ",".join("?" for _ in states)
            + ") ORDER BY id DESC LIMIT 1",
            (row["task_id"], row["id"], *states),
        ).fetchone()
        if prior:
            payload = event_payload(prior)
            status = payload.get("status", states[prior["kind"]])
            if status not in ("running", "ready", "review", "blocked"):
                raise BlockerEvidenceError("contradictory state history before gave_up")
    return True
