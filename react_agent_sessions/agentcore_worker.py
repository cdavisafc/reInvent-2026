# ABOUTME: Amazon Bedrock AgentCore Runtime entry point that runs the ReAct agent's Temporal Worker.
# Starts the Worker on invocation, polls until idle, then drains — a scale-to-zero Serverless Worker.

"""Runs a Temporal Worker hosting the harness ReAct agent inside an Amazon Bedrock AgentCore
Runtime.

The AgentCore SDK provides the HTTP contract: ``BedrockAgentCoreApp`` serves /ping and
/invocations, and reports "HealthyBusy" while an async task is registered.

Temporal Cloud invokes the runtime when Tasks arrive on the Worker Deployment's task queue. The
Worker only starts *after* invocation, and it runs in the background: /invocations registers an
async task and acknowledges immediately rather than holding the response open for the whole polling
window. AgentCore keeps the session alive for as long as /ping reports "HealthyBusy", which is
exactly what add_async_task/complete_async_task drive.

The Worker polls until it has been idle for AGENTCORE_DEBOUNCE_SECONDS. Model calls (the OpenAI
plugin's model activities) and the weather/geo/IP tools are all Activities, so the interceptor
below sees the agent's whole turn and the Worker is not torn down mid-thought. When the agent
parks on ``ask_user``, nothing is in flight: the Worker drains, and Temporal Cloud invokes the
runtime again when the user's answer arrives.

Plugin wiring is the harness streaming path (see the harness react_agent example): the OpenAI
plugin's ``stream_to_provider`` + ``harness_observer_factory`` hooks route streamed model events
into the live turn stream; the harness plugin is listed last so its data converter layers on top.

Activity sessions (experimental; unreleased temporalio build, see ../prerelease/build-local-wheels.sh): every
Activity the agent workflow starts on this task queue -- model calls and tools -- runs on the
*same* Worker, and therefore the same AgentCore microVM, that holds the workflow
(``ActivitySessionInterceptor``). The Worker is keyed to the AgentCore session
(``context.session_id``) with ``SingleSessionAdmission``, so one runtime session serves exactly
one workflow for the Worker's lifetime. This needs a Temporal Service with sticky activity
execution enabled.
"""

import asyncio
import os
from datetime import timedelta

import boto3
from bedrock_agentcore.runtime import BedrockAgentCoreApp
from temporal_agent_harness.ai_sdks.openai_agents import (
    ModelActivityParameters,
    OpenAIAgentsPlugin,
)
from temporal_agent_harness.ai_sdks.openai_agents_harness import (
    harness_observer_factory,
    stream_to_provider,
)
from temporal_agent_harness.plugin import AgentHarnessPlugin
from temporalio.client import Client
from temporalio.common import VersioningBehavior
from temporalio.worker import (
    ActivityInboundInterceptor,
    ExecuteActivityInput,
    Interceptor,
    Worker,
    WorkerDeploymentConfig,
    WorkerDeploymentVersion,
)

from activity_sessions import ActivitySessionInterceptor, session_config_for
from human_tools import HUMAN_TOOLS
from runtime_secrets import load_secrets_into_env
from tool_activities import ALL_TOOLS
from workflow import TASK_QUEUE, ReactAgentWorkflow

app = BedrockAgentCoreApp()
log = app.logger

# The background Worker, when one is running. Holds the strong reference that
# asyncio.create_task() does not keep, and doubles as the "already polling?" flag.
_worker: asyncio.Task[None] | None = None

DEPLOYMENT_NAME = os.environ.get(
    "TEMPORAL_DEPLOYMENT_NAME", "react-agent-sessions-python"
)
BUILD_ID = os.environ.get("TEMPORAL_BUILD_ID", "1.0.0")
WORKER_TASK_QUEUE = os.environ.get("REACT_AGENT_TASK_QUEUE", TASK_QUEUE)
# The Secrets Manager secret (JSON) holding the keys below. Unset for local runs, where the
# keys come from the shell environment instead.
SECRET_ID = os.environ.get("REACT_AGENT_SECRET_ID")
SECRET_KEYS = ["TEMPORAL_API_KEY", "OPENAI_API_KEY"]

# Session Workers require a fixed-size activity slot supplier, which is what
# max_concurrent_activities (without a tuner) gives.
ACTIVITY_SLOTS = 4

# How long the Worker keeps polling after it goes idle.
DEBOUNCE = float(os.environ.get("AGENTCORE_DEBOUNCE_SECONDS", "60"))
# How long the drain waits for in-flight Activities (a model or tool call).
DRAIN = timedelta(seconds=120)


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


def _openai_agents_plugin() -> OpenAIAgentsPlugin:
    return OpenAIAgentsPlugin(
        model_params=ModelActivityParameters(
            # Streaming leans on activity heartbeats to notice a stuck model call; keep the
            # heartbeat timeout well under the overall start-to-close.
            start_to_close_timeout=timedelta(minutes=2),
            heartbeat_timeout=timedelta(seconds=30),
            # The harness streaming seam: route streamed events to the in-flight turn.
            stream_to_provider=stream_to_provider,
            # Each flush is one publish Signal; 250ms trades a little smoothness for less
            # workflow history than the source example's 50ms.
            streaming_batch_interval=timedelta(milliseconds=250),
        ),
        observer_factory=harness_observer_factory,
    )


async def run_worker(session_id: str | None) -> None:
    """Poll until idle, then drain."""
    if SECRET_ID:
        load_secrets_into_env(
            SECRET_ID, SECRET_KEYS, client=boto3.client("secretsmanager")
        )
    api_key = os.environ.get("TEMPORAL_API_KEY") or None
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
        api_key=api_key,
        tls=bool(api_key),
        plugins=[
            _openai_agents_plugin(),
            # Hands over the agent's whole toolset: the plugin registers the four tool activity
            # bodies and skips ask_user (a callback tool with no worker-side body). Offload is
            # off: the harness default writes to local disk, which is ephemeral here and
            # unreadable by the locally run server — an oversized payload should fail loudly.
            AgentHarnessPlugin(
                tools=[*ALL_TOOLS, *HUMAN_TOOLS], large_payload_offload=None
            ),
        ],
    )

    tracker = ActivityTracker()
    log.info(
        "polling %s as %s/%s (session %s)",
        WORKER_TASK_QUEUE,
        DEPLOYMENT_NAME,
        BUILD_ID,
        session_id,
    )
    worker = Worker(
        client,
        task_queue=WORKER_TASK_QUEUE,
        workflows=[ReactAgentWorkflow],
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
    try:
        await run_worker(session_id)
    except Exception:
        # Nothing awaits this task, so an error would otherwise be swallowed.
        log.exception("worker failed in async task")
    finally:
        # Without this the session stays HealthyBusy until MaxLifetime.
        app.complete_async_task(task_id)


@app.entrypoint
async def invoke(payload: dict, context) -> dict:
    """Start the Worker and acknowledge. The payload is unused."""
    session_id = context.session_id or None
    # Prevent duplicate workers since we exit early
    global _worker
    if _worker is not None and not _worker.done():
        log.info("worker already polling %s", WORKER_TASK_QUEUE)
        return {"message": "worker already polling", "task_queue": WORKER_TASK_QUEUE}

    task_id = app.add_async_task("temporal-worker")
    _worker = asyncio.create_task(_run_until_idle(task_id, session_id))

    return {"message": "worker starting", "task_queue": WORKER_TASK_QUEUE}


if __name__ == "__main__":
    app.run()
