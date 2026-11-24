# ABOUTME: End-to-end tests for a subagent the concierge reuses on a later turn: the merged stream a UI
# reads (AgentClient.attach) must carry the subagent's events for that later turn too.

"""A UI reads one merged stream per session (``AgentClient.attach``), which interleaves the parent's
events with each subagent's. When a turn ends the stream goes idle, and for the next prompt the UI
re-attaches from where it left off. The merge then mounts each subagent afresh, at the
``from_offset`` on the parent's ``subagent_message_sent``. These tests reuse one Shipping subagent
across two concierge turns and check its second-turn events reach the merged stream.
"""

import asyncio
import json
import uuid
from typing import ClassVar

from agents import Model, ModelResponse
from openai.types.responses import Response, ResponseCompletedEvent
from temporal_agent_harness.ai_sdks.openai_agents.testing import (
    ResponseBuilders,
    TestModelProvider,
)
from temporal_agent_harness.harness.agent_client import AgentClient
from temporal_agent_harness.harness.agent_protocol import (
    SEND_AGENT_MESSAGE_UPDATE,
    AgentConfig,
    AgentEvent,
    AgentEventType,
    AgentMessage,
    AgentMessageReply,
)
from temporalio.client import Client

from concierge.workflow import CONCIERGE_TASK_QUEUE, ConciergeAgentWorkflow
from contracts import SHIPPING_TASK_QUEUE
from hosted_agents import concierge_plugins, shipping_plugins
from shipping.workflow import ShippingAgentWorkflow
from tests.fake_judge import fake_judge_order_relation
from tests.test_auto_mode_e2e import ScriptedByPrompt
from tests.test_subagent_e2e import _dev_server, _subscribe, _tool_call, _worker

FIRST_PROMPT = "Where is order 1042?"
SECOND_PROMPT = "Expedite it."
# What the concierge passes on to Shipping for each customer prompt.
TO_SHIPPING = {FIRST_PROMPT: "Where is order 1042?", SECOND_PROMPT: "Expedite order 1042."}


class _ShippingByPrompt(ScriptedByPrompt):
    SCRIPT: ClassVar = {
        "Where is order 1042?": [("track_shipment", {"order_id": "1042"})],
        "Expedite order 1042.": [("expedite_shipment", {"order_id": "1042"})],
    }


class _ConciergeReusingShipping(Model):
    """Starts Shipping on the first prompt only, and asks the same Shipping on every prompt.
    Reads the conversation, so it needs no state of its own."""

    async def get_response(self, system_instructions, input, *args, **kwargs) -> ModelResponse:
        prompt_index = max(
            i for i, item in enumerate(input) if isinstance(item, dict) and item.get("role") == "user"
        )
        prompt = input[prompt_index]["content"]
        outputs = {
            item["call_id"]: str(item["output"])
            for item in input
            if isinstance(item, dict) and item.get("type") == "function_call_output"
        }
        this_turn = {
            item["call_id"]
            for item in input[prompt_index + 1 :]
            if isinstance(item, dict) and item.get("type") == "function_call_output"
        }
        if "call-start" not in outputs:
            return _tool_call("start_shipping", {}, "call-start")
        ask_id = f"call-ask-{prompt_index}"
        if ask_id not in this_turn:
            handle = outputs["call-start"].strip('"')
            arguments = {"subagent": handle, "message": {"text": TO_SHIPPING[prompt]}}
            return _tool_call("shipping_ask", arguments, ask_id)
        return ResponseBuilders.output_message(f"Shipping says: {json.loads(outputs[ask_id])['text']}")

    async def stream_response(self, system_instructions, input, *args, **kwargs):
        scripted = await self.get_response(system_instructions, input)
        yield ResponseCompletedEvent(
            type="response.completed",
            sequence_number=0,
            response=Response(
                id=f"scripted-{uuid.uuid4()}",
                created_at=0,
                model="scripted",
                object="response",
                output=scripted.output,
                parallel_tool_calls=False,
                tool_choice="auto",
                tools=[],
            ),
        )


async def _turn(client: Client, workflow_id: str, text: str, turns_so_far: int) -> None:
    """Send ``text`` and wait for the concierge's turn to end."""
    await client.get_workflow_handle(workflow_id).execute_update(
        SEND_AGENT_MESSAGE_UPDATE,
        AgentMessage(type="ask", payload={"text": text}),
        result_type=AgentMessageReply,
    )
    ended = 0
    async for item in _subscribe(client, workflow_id):
        if item.data.event.type == AgentEventType.TURN_END:
            ended += 1
            if ended == turns_so_far + 1:
                return


async def _attach(client: Client, workflow_id: str, from_offset: int) -> tuple[list[AgentEvent], int]:
    """Read the merged stream from ``from_offset`` until the session is idle, as a UI does;
    returns the events and the offset to resume from next time."""
    items = await AgentClient(temporal=client, workflow_id=workflow_id).attach(
        on_item=lambda event, offset: (event, offset), from_offset=from_offset
    )
    events: list[AgentEvent] = []
    resume = from_offset
    async for event, offset in items:
        events.append(event)
        resume = offset
    return events, resume


def _tool_starts(events: list[AgentEvent]) -> list[str]:
    return [e.event.tool_name for e in events if e.event.type == AgentEventType.TOOL_START]


async def _two_turns_with_attaches() -> tuple[list[AgentEvent], list[AgentEvent], list[AgentEvent]]:
    """Two concierge turns reusing one Shipping subagent. Returns the merged stream read after
    turn 1, the stream resumed from there after turn 2, and a full replay from 0."""
    async with _dev_server() as env:
        target = env.client.service_client.config.target_host
        concierge = await Client.connect(
            target,
            namespace=env.client.namespace,
            plugins=concierge_plugins(TestModelProvider(_ConciergeReusingShipping())),
        )
        shipping = await Client.connect(
            target, namespace=env.client.namespace, plugins=shipping_plugins(_ShippingByPrompt, judge=fake_judge_order_relation)
        )
        async with (
            _worker(concierge, CONCIERGE_TASK_QUEUE, ConciergeAgentWorkflow),
            _worker(shipping, SHIPPING_TASK_QUEUE, ShippingAgentWorkflow),
        ):
            parent = await concierge.start_workflow(
                ConciergeAgentWorkflow.run,  # type: ignore[attr-defined]  # added by @agent.defn
                AgentConfig(),
                id=f"concierge-{uuid.uuid4()}",
                task_queue=CONCIERGE_TASK_QUEUE,
            )
            async with asyncio.timeout(60):
                await _turn(concierge, parent.id, FIRST_PROMPT, turns_so_far=0)
                first, resume = await _attach(concierge, parent.id, from_offset=0)
                await _turn(concierge, parent.id, SECOND_PROMPT, turns_so_far=1)
                resumed, _ = await _attach(concierge, parent.id, from_offset=resume)
                replay, _ = await _attach(concierge, parent.id, from_offset=0)
    return first, resumed, replay


async def test_reused_subagent_events_reach_the_merged_stream_on_every_turn():
    first, resumed, replay = await _two_turns_with_attaches()

    assert _tool_starts(first) == ["start_shipping", "shipping_ask", "track_shipment"]
    # The second turn, read by resuming as a UI does after sending its next prompt.
    assert _tool_starts(resumed) == ["shipping_ask", "expedite_shipment"]
    # And a fresh page load replaying the whole session.
    assert _tool_starts(replay) == [
        "start_shipping",
        "shipping_ask",
        "track_shipment",
        "shipping_ask",
        "expedite_shipment",
    ]
