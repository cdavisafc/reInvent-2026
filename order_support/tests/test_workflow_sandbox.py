# ABOUTME: Regression test: OpenTelemetry code that a span processor imports lazily, from inside a
# workflow, must not be re-imported by Temporal's sandbox (AgentCore's own span processor does this).

"""On AgentCore, ``bedrock_agentcore``'s span processor imports ``opentelemetry.baggage`` the first
time a span starts. Strands starts spans inside the workflow, so without a passthrough the sandbox
imports a fresh ``opentelemetry.context``, whose module body reads ``os.environ`` and is rejected
("Cannot access os.environ.get from inside a workflow"). The harness reports that to the UI as a
``message_handler_error``.
"""

import asyncio
import shutil
import sys
import uuid
from datetime import timedelta

from opentelemetry import trace
from opentelemetry.sdk.trace import SpanProcessor, TracerProvider
from temporal_agent_harness.harness.agent_protocol import (
    SEND_AGENT_MESSAGE_UPDATE,
    TURN_EVENTS_TOPIC,
    AgentConfig,
    AgentEvent,
    AgentEventType,
    AgentMessage,
    AgentMessageReply,
)
from temporalio.client import Client
from temporalio.contrib.workflow_streams import WorkflowStreamClient
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from hosted_agents import hosted_agent, shipping_plugins, workflow_runner
from tests.fake_judge import fake_judge_order_relation
from tests.test_subagent_e2e import _ScriptedShipping


class _LazyImportingSpanProcessor(SpanProcessor):
    """Like bedrock_agentcore.runtime.tracing's processor: imports OpenTelemetry on first use."""

    def on_start(self, span, parent_context=None) -> None:
        # Drop any cached copy so the import really runs, as it does on a fresh AgentCore Worker.
        for name in [m for m in sys.modules if m.startswith("opentelemetry.baggage")]:
            del sys.modules[name]
        from opentelemetry import baggage  # noqa: F401


def _install_lazy_importing_tracer_provider() -> None:
    provider = TracerProvider()
    provider.add_span_processor(_LazyImportingSpanProcessor())
    trace.set_tracer_provider(provider)


async def test_shipping_turn_survives_lazy_opentelemetry_imports_in_spans():
    _install_lazy_importing_tracer_provider()
    agent = hosted_agent("shipping")
    env = await WorkflowEnvironment.start_local(
        **({"dev_server_existing_path": path} if (path := shutil.which("temporal")) else {})
    )
    try:
        client = await Client.connect(
            env.client.service_client.config.target_host,
            namespace=env.client.namespace,
            plugins=shipping_plugins(_ScriptedShipping, judge=fake_judge_order_relation),
        )
        async with Worker(
            client,
            task_queue=agent.task_queue,
            workflows=[agent.workflow],
            workflow_runner=workflow_runner(),
        ):
            handle = await client.start_workflow(
                agent.workflow.run,  # type: ignore[attr-defined]  # added by @agent.defn
                AgentConfig(),
                id=f"shipping-{uuid.uuid4()}",
                task_queue=agent.task_queue,
            )
            await handle.execute_update(
                SEND_AGENT_MESSAGE_UPDATE,
                AgentMessage(type="ask", payload={"text": "Where is order 1042?"}),
                result_type=AgentMessageReply,
            )
            seen: list[AgentEventType] = []
            async with asyncio.timeout(30):
                async for item in WorkflowStreamClient.create(client, handle.id).subscribe(
                    topics=[TURN_EVENTS_TOPIC],
                    from_offset=0,
                    result_type=AgentEvent,
                    poll_cooldown=timedelta(milliseconds=10),
                ):
                    event = item.data.event
                    seen.append(event.type)
                    assert event.type != AgentEventType.MESSAGE_HANDLER_ERROR, event.message
                    # The scripted model tracks the order, then asks to expedite it.
                    if event.type == AgentEventType.TOOL_APPROVAL_REQUESTED:
                        break
    finally:
        await env.shutdown()

    assert AgentEventType.TOOL_END in seen
