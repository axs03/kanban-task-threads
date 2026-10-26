"""The operator's reconciliation verbs (ADR-0007): what the failure policy defers to
a human, made operable without hand-written SQL. Everything acts on the plugin
state DB only — Discord is never touched; the consumer does the rest on its
next pass."""

import pytest

from kanban_task_threads import reconcile
from kanban_task_threads.store import StateStore


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "state.db"
    store = StateStore(path)
    # a healthy post, an unknown-outcome create, a tombstone, a dead letter
    store.begin_create("default", "t_ok", now=100)
    store.complete_create(
        "default", "t_ok", thread_id="th1", message_id="m1", destination="discord:webhook:1@7"
    )
    store.begin_create("default", "t_pending", now=200)
    store.begin_create("default", "t_gone", now=100)
    store.complete_create(
        "default", "t_gone", thread_id="th2", message_id="m2", destination="discord:webhook:1@7"
    )
    store.mark_tombstone("default", "t_gone", "404 at event 9")
    store.begin_create("default", "t_dead", now=100)
    store.mark_dead_letter("default", "t_dead", "HTTP 400 tag required")
    return path


def ids(rows):
    return sorted(r["task_id"] for r in rows)


def test_attention_lists_exactly_what_needs_an_operator(db):
    assert ids(reconcile.attention(db)) == ["t_dead", "t_gone", "t_pending"]


def test_clear_forgets_an_unknown_create_so_the_next_pass_recreates(db):
    reconcile.clear(db, "default", "t_pending")
    assert ids(reconcile.attention(db)) == ["t_dead", "t_gone"]
    assert StateStore(db).get_post("default", "t_pending") is None


def test_clear_refuses_a_post_that_has_a_thread(db):
    with pytest.raises(reconcile.ReconcileError):
        reconcile.clear(db, "default", "t_gone")  # it HAS a thread: not a clear


def test_adopt_attaches_the_thread_that_did_get_created(db):
    reconcile.adopt(db, "default", "t_pending", thread_id="th9", message_id="m9")
    post = StateStore(db).get_post("default", "t_pending")
    assert post["pending_create_at"] is None
    assert (post["thread_id"], post["state"]) == ("th9", "live")


def test_rearm_revives_a_parked_post_keeping_its_thread(db):
    reconcile.rearm(db, "default", "t_dead")
    post = StateStore(db).get_post("default", "t_dead")
    assert post["state"] == "live"
    assert post["detail"] is None


def test_recreate_drops_the_row_so_a_fresh_post_opens(db):
    reconcile.recreate(db, "default", "t_gone")
    assert StateStore(db).get_post("default", "t_gone") is None


def test_unknown_task_raises(db):
    with pytest.raises(reconcile.ReconcileError):
        reconcile.rearm(db, "default", "t_nope")


def test_bound_file_rejects_foreign_board_mutation(db):
    store = StateStore(db)
    store._conn.execute("INSERT INTO board_binding VALUES ('default', 'discord:webhook:1@7')")
    store.begin_create("nib", "t_pending", now=1)
    with pytest.raises(reconcile.ReconcileError):
        reconcile.clear(db, "nib", "t_pending")
    assert store.get_post("nib", "t_pending") is not None


def test_adopt_can_record_verified_public_destination_without_secret(db):
    reconcile.adopt(
        db,
        "default",
        "t_pending",
        thread_id="found",
        message_id="starter",
        destination="discord:webhook:1@7",
    )
    assert StateStore(db).get_post("default", "t_pending")["destination"] == "discord:webhook:1@7"


def test_bound_adopt_rejects_destination_change(db):
    store = StateStore(db)
    store._conn.execute("INSERT INTO board_binding VALUES ('default', 'discord:webhook:1@7')")
    with pytest.raises(reconcile.ReconcileError):
        reconcile.adopt(
            db,
            "default",
            "t_pending",
            thread_id="found",
            message_id="starter",
            destination="discord:webhook:1@9",
        )
    assert store.get_post("default", "t_pending")["pending_create_at"] == 200


def test_reconcile_refuses_aliased_state_file(db, tmp_path):
    alias = tmp_path / "alias.db"
    alias.symlink_to(db)
    with pytest.raises(reconcile.ReconcileError):
        reconcile.clear(alias, "default", "t_pending")


def test_bound_pending_adopt_uses_its_verified_binding(db):
    store = StateStore(db)
    store._conn.execute("INSERT INTO board_binding VALUES ('default', 'discord:webhook:1@7')")
    reconcile.adopt(db, "default", "t_pending", thread_id="found", message_id="starter")
    assert store.get_post("default", "t_pending")["destination"] == "discord:webhook:1@7"
