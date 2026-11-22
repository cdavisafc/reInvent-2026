# ABOUTME: Amazon Bedrock AgentCore Runtime entry point that runs one order-support agent's Worker,
# chosen by ORDER_SUPPORT_AGENT. Starts on invocation, polls until idle, then drains.

"""Runs a Temporal Worker for one order-support agent inside an Amazon Bedrock AgentCore Runtime.

Every runtime in agentcore/agentcore.json deploys this same code and picks its agent with the
ORDER_SUPPORT_AGENT env var (see spike/hosted_agents.py). Each agent is its own Worker Deployment
on its own task queue, so the Temporal server invokes each runtime separately: a subagent (a child
workflow on another queue) gets its own invoke, microVM and Worker.

The lifecycle follows ../react_agent_sessions/agentcore_worker.py:

- ``BedrockAgentCoreApp`` serves /ping and /invocations. /invocations registers an async task,
  starts the Worker in the background and acknowledges immediately. AgentCore keeps the session
  alive while /ping reports "HealthyBusy", which the async task drives.
- The Worker polls until no Activity has run for AGENTCORE_DEBOUNCE_SECONDS, then drains and
  completes the async task, so the runtime scales to zero. A concierge waiting on a subagent has a
  ``run_subagent_turn`` Activity in flight, so it stays up while the child works.
- Activity sessions (unreleased temporalio, see ../prerelease/build-local-wheels.sh): every
  Activity a workflow starts on its own queue runs on the Worker that holds the workflow
  (``ActivitySessionInterceptor``), and the Worker admits exactly one workflow, keyed to the
  AgentCore session (``SingleSessionAdmission`` via ``session_config_for``).

Two heartbeats feed the local status page (../agent_status/status_page.py):

- The Temporal Worker heartbeat: the SDK reports the Worker to the Temporal server every
  WORKER_HEARTBEAT, read back with ListWorkers. It stops when the Worker drains. The Worker's
  identity is ``agentcore:<session id>``, so the workflow history names the session that ran each
  workflow task.
- The session heartbeat (session_heartbeat.py): a process-level task, started on the first invoke
  when SESSION_HEARTBEAT_TABLE is set, that writes to DynamoDB, naming the agent's task queue,
  until AgentCore destroys the microVM. It outlives the Worker and must never register an async
  task, or the session would report HealthyBusy and never go idle.
"""

import asyncio
import functools
import os
from datetime import timedelta

import boto3
from bedrock_agentcore.runtime import BedrockAgentCoreApp
from temporalio.client import Client
from temporalio.common import VersioningBehavior
from temporalio.runtime import Runtime, TelemetryConfig
from temporalio.worker import (
    ActivityInboundInterceptor,
    ExecuteActivityInput,
    Interceptor,
    Worker,
    WorkerDeploymentConfig,
    WorkerDeploymentVersion,
)

from activity_sessions import ActivitySessionInterceptor, session_config_for
from runtime_secrets import load_secrets_into_env
from session_heartbeat import SessionHeartbeat
from spike.hosted_agents import hosted_agent, workflow_runner

app = BedrockAgentCoreApp()
log = app.logger

# The background Worker, when one is running. Holds the strong reference that
# asyncio.create_task() does not keep, and doubles as the "already polling?" flag.
_worker: asyncio.Task[None] | None = None
# This session's liveness heartbeat, started on the first invoke; it lives as long as the process.
_heartbeat: SessionHeartbeat | None = None

# Which agent this runtime hosts: a key of spike.hosted_agents.HOSTED_AGENTS.
AGENT_NAME = os.environ.get("ORDER_SUPPORT_AGENT")
DEPLOYMENT_NAME = os.environ.get("TEMPORAL_DEPLOYMENT_NAME")
BUILD_ID = os.environ.get("TEMPORAL_BUILD_ID", "1.0.0")
# The Secrets Manager secret (JSON) holding the agent's keys. Unset for local runs, where the
# keys come from the shell environment instead.
SECRET_ID = os.environ.get("ORDER_SUPPORT_SECRET_ID")

# Session Workers require a fixed-size activity slot supplier, which is what
# max_concurrent_activities (without a tuner) gives.
ACTIVITY_SLOTS = 4

# How long the Worker keeps polling after it goes idle.
DEBOUNCE = float(os.environ.get("AGENTCORE_DEBOUNCE_SECONDS", "60"))
# How long the drain waits for in-flight Activities (a model, tool or subagent call).
DRAIN = timedelta(seconds=120)
# How often the SDK reports this Worker to the Temporal server (read back with ListWorkers).
WORKER_HEARTBEAT = timedelta(seconds=10)
# The shared DynamoDB table for the session heartbeat (../agent_status/). Unset (local runs) turns
# the heartbeat off.
HEARTBEAT_TABLE = os.environ.get("SESSION_HEARTBEAT_TABLE")


class ActivityTracker(Interceptor):
    """Tracks in-flight activities and blocks until AGENTCORE_DEBOUNCE_SECONDS elapses with no events."""

    def __init__(self) -> None:
        self.inflight = 0
        self.changed = asyncio.Event()

    def intercept_activity(
        self, next: ActivityInboundInterceptor
    ) -> ActivityInboundInterceptor:
        return _TrackedActivity(next, self)

    async def wait_until_idle(self, debounce: float) -> None:
        """Return once no Activity has run for ``debounce`` seconds."""
        while True:
            self.changed.clear()
            try:
                # Wake the moment an Activity starts or finishes; a timeout
                # instead means nothing has happened for the whole window.
                await asyncio.wait_for(self.changed.wait(), timeout=debounce)
            except TimeoutError:
                if self.inflight == 0:
                    return


class _TrackedActivity(ActivityInboundInterceptor):
    def __init__(
        self, next: ActivityInboundInterceptor, tracker: ActivityTracker
    ) -> None:
        super().__init__(next)
        self._tracker = tracker

    async def execute_activity(self, input: ExecuteActivityInput):
        self._tracker.inflight += 1
        self._tracker.changed.set()
        log.info("activity in flight: %d", self._tracker.inflight)
        try:
            return await self.next.execute_activity(input)
        finally:
            self._tracker.inflight -= 1
            self._tracker.changed.set()


def worker_identity(session_id: str | None) -> str | None:
    """The Temporal identity for this session's Worker; None keeps the SDK's default."""
    return f"agentcore:{session_id}" if session_id else None


@functools.cache
def _temporal_runtime() -> Runtime:
    # Created once: each Runtime starts its own thread pool.
    return Runtime(telemetry=TelemetryConfig(), worker_heartbeat_interval=WORKER_HEARTBEAT)


def _heartbeat_writer(table_name: str):
    table = boto3.resource("dynamodb").Table(table_name)
    return lambda item: table.put_item(Item=item)


def _start_heartbeat(session_id: str | None) -> None:
    """Start this session's heartbeat on the first invoke that can name the session."""
    global _heartbeat
    if _heartbeat is not None or not (HEARTBEAT_TABLE and session_id):
        return
    _heartbeat = SessionHeartbeat(
        session_id, hosted_agent(AGENT_NAME).task_queue, _heartbeat_writer(HEARTBEAT_TABLE)
    )
    _heartbeat.start()


async def run_worker(session_id: str | None) -> None:
    """Poll until idle, then drain."""
    agent = hosted_agent(AGENT_NAME)
    if SECRET_ID:
        load_secrets_into_env(
            SECRET_ID, agent.secret_keys, client=boto3.client("secretsmanager")
        )
    api_key = os.environ.get("TEMPORAL_API_KEY") or None
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
        api_key=api_key,
        tls=bool(api_key),
        identity=worker_identity(session_id),
        runtime=_temporal_runtime(),
        plugins=agent.plugins(),
    )

    tracker = ActivityTracker()
    log.info(
        "%s polling %s as %s/%s (session %s)",
        agent.name,
        agent.task_queue,
        DEPLOYMENT_NAME,
        BUILD_ID,
        session_id,
    )
    worker = Worker(
        client,
        task_queue=agent.task_queue,
        workflows=[agent.workflow],
        workflow_runner=workflow_runner(),
        max_concurrent_activities=ACTIVITY_SLOTS,
        session_config=session_config_for(session_id),
        interceptors=[tracker, ActivitySessionInterceptor()],
        deployment_config=WorkerDeploymentConfig(
            version=WorkerDeploymentVersion(
                deployment_name=DEPLOYMENT_NAME, build_id=BUILD_ID
            ),
            use_worker_versioning=True,
            default_versioning_behavior=VersioningBehavior.PINNED,
        ),
        graceful_shutdown_timeout=DRAIN,
    )
    async with worker:
        await tracker.wait_until_idle(DEBOUNCE)
    log.info("worker idle for %ss; drained", DEBOUNCE)


async def _run_until_idle(task_id: int, session_id: str | None) -> None:
    """Own the Worker's whole life, and always release the async task."""
    if _heartbeat is not None:
        _heartbeat.set_worker_running(True, worker_identity(session_id))
    try:
        await run_worker(session_id)
    except Exception:
        # Nothing awaits this task, so an error would otherwise be swallowed.
        log.exception("worker failed in async task")
    finally:
        # Without this the session stays HealthyBusy until MaxLifetime.
        app.complete_async_task(task_id)
        if _heartbeat is not None:
            _heartbeat.set_worker_running(False)


@app.entrypoint
async def invoke(payload: dict, context) -> dict:
    """Start the Worker and acknowledge. The payload is unused."""
    session_id = context.session_id or None
    # Prevent duplicate workers since we exit early
    global _worker
    if _worker is not None and not _worker.done():
        log.info("worker already polling for %s", AGENT_NAME)
        return {"message": "worker already polling", "agent": AGENT_NAME}

    _start_heartbeat(session_id)
    task_id = app.add_async_task("temporal-worker")
    _worker = asyncio.create_task(_run_until_idle(task_id, session_id))

    return {"message": "worker starting", "agent": AGENT_NAME}


if __name__ == "__main__":
    # Fail at startup, not on the first invoke, if the runtime's config is wrong.
    hosted_agent(AGENT_NAME)
    if not DEPLOYMENT_NAME:
        raise SystemExit("TEMPORAL_DEPLOYMENT_NAME must be set")
    app.run()
