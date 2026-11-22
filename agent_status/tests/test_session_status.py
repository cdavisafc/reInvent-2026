# ABOUTME: Tests for the status page's logic: its config file, the per-agent sections, and the rows
# that join Temporal's view (workflows, Workers) with the session heartbeats, with freshness rules.

import pytest

from session_status import (
    AgentConfig,
    ConfigError,
    WorkerSeen,
    build_rows,
    build_sections,
    index_workers,
    load_config,
    running_workflows_query,
    session_id_from_identity,
    workers_query,
)

NOW = 10_000.0


def heartbeat(
    session_id: str, age: float, worker_running: bool = False, task_queue: str = "queue-a"
) -> dict:
    return {
        "session_id": session_id,
        "task_queue": task_queue,
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


AGENT_A = AgentConfig(label="Agent A", task_queue="queue-a")
AGENT_B = AgentConfig(label="Agent B", task_queue="queue-b")


def test_sections_follow_config_order_with_each_agents_workflows() -> None:
    sections = build_sections(
        [AGENT_B, AGENT_A],
        {"queue-a": ["wf-a"], "queue-b": ["wf-b1", "wf-b2"]},
        {},
        {},
        {},
        NOW,
    )

    assert [(s.label, s.task_queue) for s in sections] == [
        ("Agent B", "queue-b"),
        ("Agent A", "queue-a"),
    ]
    assert [row.workflow_id for row in sections[0].rows] == ["wf-b1", "wf-b2"]
    assert [row.workflow_id for row in sections[1].rows] == ["wf-a"]


def test_live_sessions_no_workflow_points_at_appear_only_under_their_own_queue() -> None:
    sections = build_sections(
        [AGENT_A, AGENT_B],
        {"queue-a": [], "queue-b": []},
        {},
        {},
        {
            "sess-a": heartbeat("sess-a", age=2, task_queue="queue-a"),
            "sess-b": heartbeat("sess-b", age=2, task_queue="queue-b"),
        },
        NOW,
    )

    assert [[row.session_id for row in s.rows] for s in sections] == [["sess-a"], ["sess-b"]]


def test_heartbeats_for_unconfigured_queues_are_not_shown() -> None:
    [section] = build_sections(
        [AGENT_A],
        {"queue-a": []},
        {},
        {},
        {"sess-x": heartbeat("sess-x", age=2, task_queue="some-other-queue")},
        NOW,
    )

    assert section.rows == []


def test_running_workflows_query_narrows_by_workflow_type_only_when_configured() -> None:
    assert running_workflows_query(AGENT_A) == (
        "TaskQueue = 'queue-a' AND ExecutionStatus = 'Running'"
    )
    typed = AgentConfig(label="ReAct", task_queue="q", workflow_type="ReactAgent")
    assert running_workflows_query(typed) == (
        "TaskQueue = 'q' AND ExecutionStatus = 'Running' AND WorkflowType = 'ReactAgent'"
    )


def test_workers_query_covers_every_configured_queue() -> None:
    assert workers_query(["queue-a", "queue-b"]) == (
        "TaskQueue = 'queue-a' OR TaskQueue = 'queue-b'"
    )


def write_config(tmp_path, text: str):
    path = tmp_path / "agents.toml"
    path.write_text(text)
    return path


def test_load_config_reads_the_table_and_agents_in_order(tmp_path) -> None:
    config = load_config(
        write_config(
            tmp_path,
            """
heartbeat_table = "beats"

[[agents]]
label = "Concierge"
task_queue = "spike-concierge"

[[agents]]
label = "ReAct"
task_queue = "react"
workflow_type = "ReactAgent"
""",
        )
    )

    assert config.heartbeat_table == "beats"
    assert config.agents == (
        AgentConfig(label="Concierge", task_queue="spike-concierge"),
        AgentConfig(label="ReAct", task_queue="react", workflow_type="ReactAgent"),
    )


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ('heartbeat_table = "beats"\n', "at least one"),
        ('[[agents]]\nlabel = "A"\ntask_queue = "q"\n', "heartbeat_table"),
        ('heartbeat_table = "beats"\n[[agents]]\nlabel = "A"\n', "task_queue"),
        ('heartbeat_table = "beats"\n[[agents]]\ntask_queue = "q"\n', "label"),
        (
            (
                'heartbeat_table = "beats"\n'
                '[[agents]]\nlabel = "A"\ntask_queue = "q"\n'
                '[[agents]]\nlabel = "B"\ntask_queue = "q"\n'
            ),
            "more than once",
        ),
    ],
)
def test_load_config_rejects_incomplete_files(tmp_path, text: str, message: str) -> None:
    with pytest.raises(ConfigError, match=message):
        load_config(write_config(tmp_path, text))
