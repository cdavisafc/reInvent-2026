# ABOUTME: Tests for the status page's row building: joining Temporal's view (workflows, Workers)
# with the AgentCore session heartbeats, and the freshness rules for each.

from session_status import (
    WorkerSeen,
    build_rows,
    index_workers,
    session_id_from_identity,
)

NOW = 10_000.0


def heartbeat(session_id: str, age: float, worker_running: bool = False) -> dict:
    return {
        "session_id": session_id,
        "last_seen": NOW - age,
        "worker_running": worker_running,
    }


def test_session_id_comes_from_agentcore_worker_identity() -> None:
    assert session_id_from_identity("agentcore:sess-1") == "sess-1"
    assert session_id_from_identity("12345@laptop") is None
    assert session_id_from_identity(None) is None


def test_index_workers_keeps_latest_heartbeat_per_identity() -> None:
    old = WorkerSeen("agentcore:sess-1", "SHUTDOWN", NOW - 300)
    new = WorkerSeen("agentcore:sess-1", "RUNNING", NOW - 5)

    assert index_workers([new, old]) == {"agentcore:sess-1": new}
    assert index_workers([old, new]) == {"agentcore:sess-1": new}


def test_working_session_is_polling_and_alive() -> None:
    [row] = build_rows(
        ["wf-1"],
        {"wf-1": "agentcore:sess-1"},
        index_workers([WorkerSeen("agentcore:sess-1", "RUNNING", NOW - 5)]),
        {"sess-1": heartbeat("sess-1", age=3, worker_running=True)},
        NOW,
    )

    assert row.workflow_id == "wf-1"
    assert row.session_id == "sess-1"
    assert row.worker_polling is True
    assert row.worker_heartbeat_age == 5
    assert row.polling_source == "temporal"
    assert row.session_alive is True
    assert row.session_last_seen_age == 3
    assert row.session_worker_running is True


def test_drained_worker_with_live_session_shows_the_gap() -> None:
    [row] = build_rows(
        ["wf-1"],
        {"wf-1": "agentcore:sess-1"},
        index_workers([WorkerSeen("agentcore:sess-1", "SHUTDOWN", NOW - 20)]),
        {"sess-1": heartbeat("sess-1", age=4)},
        NOW,
    )

    assert row.worker_polling is False
    assert row.session_alive is True


def test_stale_worker_heartbeat_is_not_polling_even_if_last_status_was_running() -> None:
    [row] = build_rows(
        ["wf-1"],
        {"wf-1": "agentcore:sess-1"},
        index_workers([WorkerSeen("agentcore:sess-1", "RUNNING", NOW - 120)]),
        {},
        NOW,
    )

    assert row.worker_polling is False


def test_ended_session_is_not_alive() -> None:
    [row] = build_rows(
        ["wf-1"],
        {"wf-1": "agentcore:sess-1"},
        {},
        {"sess-1": heartbeat("sess-1", age=90)},
        NOW,
    )

    assert row.worker_polling is False
    assert row.session_alive is False
    assert row.session_last_seen_age == 90


def test_workflow_never_run_by_agentcore_has_no_session() -> None:
    [row] = build_rows(["wf-1"], {"wf-1": None}, {}, {}, NOW)

    assert row.session_id is None
    assert row.worker_polling is False
    assert row.session_alive is False
    assert row.session_last_seen_age is None


def test_without_list_workers_polling_falls_back_to_the_session_report() -> None:
    rows = build_rows(
        ["wf-1", "wf-2"],
        {"wf-1": "agentcore:sess-1", "wf-2": "agentcore:sess-2"},
        None,
        {
            "sess-1": heartbeat("sess-1", age=3, worker_running=True),
            # A dead session's last report can't vouch for a Worker.
            "sess-2": heartbeat("sess-2", age=90, worker_running=True),
        },
        NOW,
    )

    assert [row.worker_polling for row in rows] == [True, False]
    assert {row.polling_source for row in rows} == {"self-reported"}


def test_live_sessions_no_workflow_points_at_are_listed_after_workflows() -> None:
    rows = build_rows(
        ["wf-1"],
        {"wf-1": "agentcore:sess-1"},
        {},
        {
            "sess-1": heartbeat("sess-1", age=3),
            "sess-orphan": heartbeat("sess-orphan", age=2),
            "sess-dead": heartbeat("sess-dead", age=500),
        },
        NOW,
    )

    assert [(row.workflow_id, row.session_id) for row in rows] == [
        ("wf-1", "sess-1"),
        (None, "sess-orphan"),
    ]
