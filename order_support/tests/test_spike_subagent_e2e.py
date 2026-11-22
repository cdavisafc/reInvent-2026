# ABOUTME: End-to-end spike test on a local dev server: an OpenAI Agents SDK parent on one queue drives
# a Strands child on another through subagent_toolset, and a gated tool inside the child is approved.

"""Scripted models stand in for OpenAI and Bedrock, so this runs with no credentials.

The two Workers use the production plugin builders (spike/hosted_agents.py) with the models
swapped: separate clients, the parent's with only the OpenAI Agents plugin and streaming on, the
child's with only the Strands plugin. The test approves the child's ``expedite_shipment`` call against the child's own workflow id, which
it reads from the parent's ``subagent_started`` event, as the UI does.
"""

import asyncio
import json
import shutil
import uuid
from collections.abc import AsyncIterable
from datetime import timedelta
from typing import Any

from agents import Model, ModelResponse
from openai.types.responses import (
    Response,
    ResponseCompletedEvent,
    ResponseFunctionToolCall,
)
from strands.models import Model as StrandsModel
from strands.types.streaming import StreamEvent
from temporal_agent_harness.ai_sdks.openai_agents.testing import (
    ResponseBuilders,
    TestModelProvider,
)
from temporal_agent_harness.harness.agent_protocol import (
    SEND_AGENT_MESSAGE_UPDATE,
    TOOL_APPROVAL_UPDATE,
    TURN_EVENTS_TOPIC,
    AgentConfig,
    AgentEvent,
    AgentEventType,
    AgentMessage,
    AgentMessageReply,
    TextReply,
    ToolApprovalDecision,
)
from temporalio.client import Client
from temporalio.contrib.workflow_streams import WorkflowStreamClient
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from spike.hosted_agents import concierge_plugins, shipping_plugins, workflow_runner

SHIPPING_REQUEST = "Where is order 1042? Expedite it if it is late."


def _last_tool_output(input_items: Any) -> str:
    outputs = [
        item["output"]
        for item in input_items
        if isinstance(item, dict) and item.get("type") == "function_call_output"
    ]
    return str(outputs[-1])


def _tool_call(name: str, arguments: dict[str, Any], call_id: str) -> ModelResponse:
    # ResponseBuilders.tool_call reuses one call id, which the SDK refuses across calls.
    return ResponseBuilders.model_response(
        ResponseFunctionToolCall(
            arguments=json.dumps(arguments),
            call_id=call_id,
            name=name,
            type="function_call",
            id=call_id,
            status="completed",
        )
    )


class _ScriptedConcierge(Model):
    """start_shipping -> shipping_ask(handle, ...) -> a final answer quoting the specialist."""

    def __init__(self) -> None:
        self._calls = 0

    async def get_response(self, system_instructions, input, *args, **kwargs) -> ModelResponse:
        self._calls += 1
        if self._calls == 1:
            return _tool_call("start_shipping", {}, "call-start")
        if self._calls == 2:
            handle = _last_tool_output(input).strip('"')
            arguments = {"subagent": handle, "message": {"text": SHIPPING_REQUEST}}
            return _tool_call("shipping_ask", arguments, "call-ask")
        reply = json.loads(_last_tool_output(input))
        return ResponseBuilders.output_message(f"Shipping says: {reply['text']}")

    async def stream_response(self, system_instructions, input, *args, **kwargs):
        # The concierge always streams. The model activity only needs the terminal
        # response.completed event, which carries the whole scripted response.
        scripted = await self.get_response(system_instructions, input)
        yield ResponseCompletedEvent(
            type="response.completed",
            sequence_number=0,
            response=Response(
                id=f"scripted-{self._calls}",
                created_at=0,
                model="scripted",
                object="response",
                output=scripted.output,
                parallel_tool_calls=False,
                tool_choice="auto",
                tools=[],
            ),
        )


class _ScriptedShipping(StrandsModel):
    """track_shipment -> expedite_shipment -> a reply that joins both tool results."""

    def __init__(self) -> None:
        self._calls = 0

    def update_config(self, **_model_config: Any) -> None:
        return None

    def get_config(self) -> dict[str, Any]:
        return {"model_id": "scripted"}

    def structured_output(self, *_args: Any, **_kwargs: Any) -> Any:
        raise NotImplementedError

    async def stream(self, messages: Any, *_args: Any, **_kwargs: Any) -> AsyncIterable[StreamEvent]:
        self._calls += 1
        yield {"messageStart": {"role": "assistant"}}
        if self._calls <= 2:
            name = "track_shipment" if self._calls == 1 else "expedite_shipment"
            yield {
                "contentBlockStart": {
                    "start": {"toolUse": {"toolUseId": f"tu-{self._calls}", "name": name}}
                }
            }
            yield {"contentBlockDelta": {"delta": {"toolUse": {"input": '{"order_id": "1042"}'}}}}
            yield {"contentBlockStop": {}}
            yield {"messageStop": {"stopReason": "tool_use"}}
            return
        results = [
            "".join(c.get("text", "") for c in block["toolResult"]["content"])
            for message in messages
            for block in message["content"]
            if "toolResult" in block
        ]
        yield {"contentBlockDelta": {"delta": {"text": " | ".join(results)}}}
        yield {"contentBlockStop": {}}
        yield {"messageStop": {"stopReason": "end_turn"}}


async def _approve_gated_calls(client: Client, workflow_id: str, approved: list[str]) -> None:
    """Approve every gated tool call the child asks about, as a person in the UI would."""
    handle = client.get_workflow_handle(workflow_id)
    async for item in WorkflowStreamClient.create(client, workflow_id).subscribe(
        topics=[TURN_EVENTS_TOPIC],
        from_offset=0,
        result_type=AgentEvent,
        poll_cooldown=timedelta(milliseconds=10),
    ):
        event = item.data.event
        if event.type == AgentEventType.TOOL_APPROVAL_REQUESTED:
            approved.append(event.tool_name)
            await handle.execute_update(
                TOOL_APPROVAL_UPDATE, ToolApprovalDecision(tool_id=event.tool_id, approved=True)
            )


async def test_openai_parent_drives_strands_child_on_another_queue():
    from spike.concierge_workflow import (
        CONCIERGE_TASK_QUEUE,
        SpikeConciergeAgentWorkflow,
    )
    from spike.contracts import SHIPPING_TASK_QUEUE
    from spike.shipping_workflow import SpikeShippingAgentWorkflow

    temporal_cli = shutil.which("temporal")
    env = await WorkflowEnvironment.start_local(
        **({"dev_server_existing_path": temporal_cli} if temporal_cli else {})
    )
    try:
        target = env.client.service_client.config.target_host
        concierge_client = await Client.connect(
            target,
            namespace=env.client.namespace,
            plugins=concierge_plugins(TestModelProvider(_ScriptedConcierge())),
        )
        shipping_client = await Client.connect(
            target,
            namespace=env.client.namespace,
            plugins=shipping_plugins(_ScriptedShipping),
        )
        async with (
            Worker(
                concierge_client,
                task_queue=CONCIERGE_TASK_QUEUE,
                workflows=[SpikeConciergeAgentWorkflow],
                workflow_runner=workflow_runner(),
            ),
            Worker(
                shipping_client,
                task_queue=SHIPPING_TASK_QUEUE,
                workflows=[SpikeShippingAgentWorkflow],
                workflow_runner=workflow_runner(),
            ),
        ):
            parent = await concierge_client.start_workflow(
                SpikeConciergeAgentWorkflow.run,  # type: ignore[attr-defined]  # added by @agent.defn
                AgentConfig(),
                id=f"spike-concierge-{uuid.uuid4()}",
                task_queue=CONCIERGE_TASK_QUEUE,
            )
            sent = await parent.execute_update(
                SEND_AGENT_MESSAGE_UPDATE,
                AgentMessage(type="ask", payload={"text": "Is order 1042 on time?"}),
                result_type=AgentMessageReply,
            )

            events: list[AgentEvent] = []
            approved: list[str] = []
            approver: asyncio.Task | None = None
            async with asyncio.timeout(60):
                async for item in WorkflowStreamClient.create(concierge_client, parent.id).subscribe(
                    topics=[TURN_EVENTS_TOPIC],
                    from_offset=0,
                    result_type=AgentEvent,
                    poll_cooldown=timedelta(milliseconds=10),
                ):
                    events.append(item.data)
                    event = item.data.event
                    if event.type == AgentEventType.SUBAGENT_STARTED:
                        approver = asyncio.create_task(
                            _approve_gated_calls(shipping_client, event.workflow_id, approved)
                        )
                    if event.type == AgentEventType.TURN_END:
                        break
            if approver is not None:
                approver.cancel()
    finally:
        await env.shutdown()

    types = [e.event.type for e in events]
    assert AgentEventType.SUBAGENT_STARTED in types
    assert AgentEventType.SUBAGENT_MESSAGE_SENT in types
    assert approved == ["expedite_shipment"]
    output = next(
        e.event.output
        for e in events
        if e.event.type == AgentEventType.MESSAGE_HANDLER_END and e.message_id == sent.message_id
    )
    reply = TextReply.model_validate(output).text
    assert reply.startswith("Shipping says: ")
    assert "1042" in reply and "EXP-" in reply
