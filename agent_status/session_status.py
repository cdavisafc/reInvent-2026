# ABOUTME: Builds the status page's per-agent sections by joining Temporal's view (agent workflows,
# polling Workers) with the AgentCore session heartbeats. Pure logic; status_page.py fetches.

"""One section per configured agent (a task queue), each with one row per agent session (a running
workflow on that queue), plus live AgentCore sessions on that queue that no workflow points at.

- Workflow -> session: the identity on the workflow's latest WorkflowTaskStarted event. Workers
  on AgentCore connect as ``agentcore:<session id>`` (agentcore_worker.worker_identity).
- Worker polling: Temporal's ListWorkers view of that identity. When the server doesn't offer
  ListWorkers, the session's own report stands in, and the row says so.
- Session alive: the session heartbeat's ``last_seen`` (session_heartbeat.py) is recent.
"""

import tomllib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

IDENTITY_PREFIX = "agentcore:"
# Both heartbeats beat every 10s; three missed beats means gone.
FRESH_SECONDS = 30.0


class ConfigError(ValueError):
    """The status page's config file is missing something it needs."""


@dataclass(frozen=True)
class AgentConfig:
    """One agent on the page: its task queue, and the name its section shows."""

    label: str
    task_queue: str
    # Narrows the visibility query when the queue also runs other workflow types.
    workflow_type: str | None = None


@dataclass(frozen=True)
class StatusConfig:
    heartbeat_table: str
    agents: tuple[AgentConfig, ...]


def load_config(path: Path) -> StatusConfig:
    """Read and check the TOML config: a heartbeat table and at least one agent."""
    with open(path, "rb") as f:
        data = tomllib.load(f)
    table = data.get("heartbeat_table")
    if not table:
        raise ConfigError(f"{path}: heartbeat_table is required")
    entries = data.get("agents") or []
    if not entries:
        raise ConfigError(f"{path}: list at least one [[agents]] entry")

    agents = []
    for i, entry in enumerate(entries, start=1):
        for field in ("label", "task_queue"):
            if not entry.get(field):
                raise ConfigError(f"{path}: agent {i} needs a {field}")
        agents.append(
            AgentConfig(
                label=entry["label"],
                task_queue=entry["task_queue"],
                workflow_type=entry.get("workflow_type"),
            )
        )
    queues = [agent.task_queue for agent in agents]
    if duplicates := sorted({q for q in queues if queues.count(q) > 1}):
        raise ConfigError(f"{path}: task queue listed more than once: {', '.join(duplicates)}")
    return StatusConfig(heartbeat_table=table, agents=tuple(agents))


def running_workflows_query(agent: AgentConfig) -> str:
    """The visibility query for an agent's running workflows."""
    query = f"TaskQueue = '{agent.task_queue}' AND ExecutionStatus = 'Running'"
    if agent.workflow_type:
        query += f" AND WorkflowType = '{agent.workflow_type}'"
    return query


def workers_query(task_queues: Iterable[str]) -> str:
    """The ListWorkers query for every Worker on any of these queues."""
    return " OR ".join(f"TaskQueue = '{queue}'" for queue in task_queues)


@dataclass(frozen=True)
class WorkerSeen:
    """One Worker as Temporal's ListWorkers last saw it."""

    identity: str
    status: str  # WorkerStatus name without its prefix: RUNNING, SHUTTING_DOWN, SHUTDOWN
    heartbeat_time: float  # epoch seconds


@dataclass(frozen=True)
class SessionRow:
    workflow_id: str | None
    session_id: str | None
    worker_polling: bool
    worker_heartbeat_age: float | None
    polling_source: str  # "temporal" (ListWorkers) or "self-reported" (the session heartbeat)
    session_alive: bool
    session_last_seen_age: float | None
    session_worker_running: bool | None


@dataclass(frozen=True)
class AgentSection:
    label: str
    task_queue: str
    rows: list[SessionRow]


def session_id_from_identity(identity: str | None) -> str | None:
    if identity and identity.startswith(IDENTITY_PREFIX):
        return identity.removeprefix(IDENTITY_PREFIX)
    return None


def index_workers(workers: Iterable[WorkerSeen]) -> dict[str, WorkerSeen]:
    """Latest sighting per identity: a reused session ID starts a new Worker under the same one."""
    latest: dict[str, WorkerSeen] = {}
    for worker in workers:
        seen = latest.get(worker.identity)
        if seen is None or worker.heartbeat_time > seen.heartbeat_time:
            latest[worker.identity] = worker
    return latest


def build_rows(
    workflow_ids: Iterable[str],
    last_identity_by_workflow: Mapping[str, str | None],
    workers_by_identity: Mapping[str, WorkerSeen] | None,
    heartbeats_by_session: Mapping[str, Mapping],
    now: float,
) -> list[SessionRow]:
    """Rows for the given workflows, in order, then live sessions no workflow points at.

    ``workers_by_identity`` is None when ListWorkers is unavailable.
    """
    rows = []
    claimed: set[str] = set()
    for workflow_id in workflow_ids:
        session_id = session_id_from_identity(last_identity_by_workflow.get(workflow_id))
        if session_id:
            claimed.add(session_id)
        rows.append(_row(workflow_id, session_id, workers_by_identity, heartbeats_by_session, now))

    for session_id in sorted(heartbeats_by_session.keys() - claimed):
        row = _row(None, session_id, workers_by_identity, heartbeats_by_session, now)
        if row.session_alive:
            rows.append(row)
    return rows


def build_sections(
    agents: Sequence[AgentConfig],
    workflow_ids_by_queue: Mapping[str, Sequence[str]],
    last_identity_by_workflow: Mapping[str, str | None],
    workers_by_identity: Mapping[str, WorkerSeen] | None,
    heartbeats_by_session: Mapping[str, Mapping],
    now: float,
) -> list[AgentSection]:
    """One section per agent, in config order. Each sees only its own queue's heartbeats."""
    sections = []
    for agent in agents:
        heartbeats = {
            session_id: item
            for session_id, item in heartbeats_by_session.items()
            if item.get("task_queue") == agent.task_queue
        }
        rows = build_rows(
            workflow_ids_by_queue.get(agent.task_queue, []),
            last_identity_by_workflow,
            workers_by_identity,
            heartbeats,
            now,
        )
        sections.append(AgentSection(agent.label, agent.task_queue, rows))
    return sections


def _row(
    workflow_id: str | None,
    session_id: str | None,
    workers_by_identity: Mapping[str, WorkerSeen] | None,
    heartbeats_by_session: Mapping[str, Mapping],
    now: float,
) -> SessionRow:
    item = heartbeats_by_session.get(session_id) if session_id else None
    # DynamoDB returns numbers as Decimal.
    last_seen_age = now - float(item["last_seen"]) if item else None
    session_alive = last_seen_age is not None and last_seen_age < FRESH_SECONDS
    session_worker_running = bool(item["worker_running"]) if item else None

    if workers_by_identity is None:
        return SessionRow(
            workflow_id=workflow_id,
            session_id=session_id,
            worker_polling=session_alive and bool(session_worker_running),
            worker_heartbeat_age=None,
            polling_source="self-reported",
            session_alive=session_alive,
            session_last_seen_age=last_seen_age,
            session_worker_running=session_worker_running,
        )

    worker = workers_by_identity.get(f"{IDENTITY_PREFIX}{session_id}") if session_id else None
    heartbeat_age = now - worker.heartbeat_time if worker else None
    return SessionRow(
        workflow_id=workflow_id,
        session_id=session_id,
        worker_polling=(
            worker is not None
            and worker.status == "RUNNING"
            and heartbeat_age is not None
            and heartbeat_age < FRESH_SECONDS
        ),
        worker_heartbeat_age=heartbeat_age,
        polling_source="temporal",
        session_alive=session_alive,
        session_last_seen_age=last_seen_age,
        session_worker_running=session_worker_running,
    )
