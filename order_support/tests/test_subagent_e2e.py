# ABOUTME: End-to-end tests on a local dev server: the OpenAI Agents SDK concierge drives the Strands
# specialists on their own queues through subagent_toolset, with approvals inside the specialists.

"""Scripted models stand in for OpenAI and Bedrock, so this runs with no credentials.

The Workers use the production plugin builders (hosted_agents.py) with the models swapped: separate
clients, the concierge's with only the OpenAI Agents plugin and streaming on, each specialist's with
only the Strands plugin. The tests approve a specialist's gated call against the specialist's own
workflow id, which they read from the concierge's ``subagent_started`` event, as the UI does.

The specialists run in auto mode (order_policy.py), with a deterministic fake judge
(tests/fake_judge.py) in place of Claude, so a call for the order the conversation is about may be
approved with no person; the tests approve only what the rules escalate. While a child waits on a person, the parent must hold nothing on its Worker, so a
serverless Worker can drain: the parent waits for the child's reply signal in its workflow
(subagent_signals.py), not in an activity.
"""

import asyncio
import json
import shutil
import uuid
from collections.abc import AsyncIterable, AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, ClassVar

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
    AutoApprovalVerdict,
    TextReply,
    ToolApprovalDecision,
)
from temporalio.client import Client, WorkflowHandle
from temporalio.contrib.workflow_streams import WorkflowStreamClient
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from concierge.workflow import CONCIERGE_TASK_QUEUE, ConciergeAgentWorkflow
from contracts import ORDERS_TASK_QUEUE, SHIPPING_TASK_QUEUE
from hosted_agents import (
    concierge_plugins,
    orders_plugins,
    shipping_plugins,
    workflow_runner,
)
from orders.workflow import OrdersAgentWorkflow
from shipping.workflow import ShippingAgentWorkflow
from tests.fake_judge import fake_judge_order_relation

SHIPPING_REQUEST = "Where is order 1042? Expedite it if it is late."
ORDERS_REQUEST = "Order 1042 is late. Refund it if it is eligible."
REQUESTS = {"shipping": SHIPPING_REQUEST, "orders": ORDERS_REQUEST}


def _tool_outputs(input_items: Any) -> dict[str, str]:
    """Each function call's output, by call id."""
    return {
        item["call_id"]: str(item["output"])
        for item in input_items
        if isinstance(item, dict) and item.get("type") == "function_call_output"
    }


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


def _reply_or_error(output: str) -> str:
    try:
        return json.loads(output)["text"]
    except (ValueError, KeyError, TypeError):
        # A failed subagent turn reaches the model as the tool's error text.
        return output


class _ScriptedConcierge(Model):
    """For each specialist in turn: start_<key> -> <key>_ask(handle, ...); then a final answer
    quoting every specialist's reply, in order."""

    def __init__(self, specialists: tuple[str, ...] = ("shipping",)) -> None:
        self._specialists = specialists
        self._calls = 0

    async def get_response(self, system_instructions, input, *args, **kwargs) -> ModelResponse:
        self._calls += 1
        outputs = _tool_outputs(input)
        index, step = divmod(self._calls - 1, 2)
        if index < len(self._specialists):
            key = self._specialists[index]
            if step == 0:
                return _tool_call(f"start_{key}", {}, f"call-start-{key}")
            handle = outputs[f"call-start-{key}"].strip('"')
            arguments = {"subagent": handle, "message": {"text": REQUESTS[key]}}
            return _tool_call(f"{key}_ask", arguments, f"call-ask-{key}")
        replies = [
            f"{key.capitalize()} says: {_reply_or_error(outputs[f'call-ask-{key}'])}"
            for key in self._specialists
        ]
        return ResponseBuilders.output_message(" || ".join(replies))

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


class _ScriptedSpecialist(StrandsModel):
    """Calls each of ``TOOL_CALLS`` in turn, then replies with every tool result joined."""

    TOOL_CALLS: ClassVar[tuple[tuple[str, dict[str, Any]], ...]] = ()

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
        if self._calls <= len(self.TOOL_CALLS):
            name, tool_input = self.TOOL_CALLS[self._calls - 1]
            yield {
                "contentBlockStart": {
                    "start": {"toolUse": {"toolUseId": f"tu-{self._calls}", "name": name}}
                }
            }
            yield {"contentBlockDelta": {"delta": {"toolUse": {"input": json.dumps(tool_input)}}}}
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


class _ScriptedShipping(_ScriptedSpecialist):
    """track_shipment -> expedite_shipment (gated) -> a reply joining both results."""

    TOOL_CALLS = (
        ("track_shipment", {"order_id": "1042"}),
        ("expedite_shipment", {"order_id": "1042"}),
    )


class _ScriptedShippingEstimate(_ScriptedSpecialist):
    """get_delivery_estimate only: nothing for a person to approve."""

    TOOL_CALLS = (("get_delivery_estimate", {"order_id": "1042"}),)


class _ScriptedOrders(_ScriptedSpecialist):
    """check_return_eligibility -> issue_refund (gated) -> a reply joining both results."""

    TOOL_CALLS = (
        ("check_return_eligibility", {"order_id": "1042"}),
        ("issue_refund", {"order_id": "1042", "amount": 240.0, "reason": "late delivery"}),
    )


class _FailingShipping(_ScriptedShipping):
    """A shipping model whose first call fails for good, so the child's turn errors."""

    async def stream(self, messages: Any, *_args: Any, **_kwargs: Any) -> AsyncIterable[StreamEvent]:
        raise ApplicationError("Bedrock is unavailable", non_retryable=True)
        yield  # an async generator, like the real model


@asynccontextmanager
async def _dev_server() -> AsyncIterator[WorkflowEnvironment]:
    temporal_cli = shutil.which("temporal")
    env = await WorkflowEnvironment.start_local(
        **({"dev_server_existing_path": temporal_cli} if temporal_cli else {})
    )
    try:
        yield env
    finally:
        await env.shutdown()


@dataclass
class _Clients:
    """One client per Worker, with the production plugins and scripted models."""

    concierge: Client
    shipping: Client
    orders: Client


async def _clients(
    env: WorkflowEnvironment,
    *,
    specialists: tuple[str, ...] = ("shipping",),
    shipping_model: type[StrandsModel] = _ScriptedShipping,
    orders_model: type[StrandsModel] = _ScriptedOrders,
) -> _Clients:
    target = env.client.service_client.config.target_host

    async def connect(plugins) -> Client:
        return await Client.connect(target, namespace=env.client.namespace, plugins=plugins)

    return _Clients(
        concierge=await connect(
            concierge_plugins(TestModelProvider(_ScriptedConcierge(specialists)))
        ),
        shipping=await connect(shipping_plugins(shipping_model, judge=fake_judge_order_relation)),
        orders=await connect(orders_plugins(orders_model, judge=fake_judge_order_relation)),
    )


def _worker(client: Client, task_queue: str, workflow: type) -> Worker:
    return Worker(
        client, task_queue=task_queue, workflows=[workflow], workflow_runner=workflow_runner()
    )


def _concierge_worker(clients: _Clients) -> Worker:
    return _worker(clients.concierge, CONCIERGE_TASK_QUEUE, ConciergeAgentWorkflow)


def _shipping_worker(clients: _Clients) -> Worker:
    return _worker(clients.shipping, SHIPPING_TASK_QUEUE, ShippingAgentWorkflow)


def _orders_worker(clients: _Clients) -> Worker:
    return _worker(clients.orders, ORDERS_TASK_QUEUE, OrdersAgentWorkflow)


async def _ask_concierge(
    client: Client, text: str = "Is order 1042 on time?"
) -> tuple[WorkflowHandle, AgentMessageReply]:
    parent = await client.start_workflow(
        ConciergeAgentWorkflow.run,  # type: ignore[attr-defined]  # added by @agent.defn
        AgentConfig(),
        id=f"concierge-{uuid.uuid4()}",
        task_queue=CONCIERGE_TASK_QUEUE,
    )
    sent = await parent.execute_update(
        SEND_AGENT_MESSAGE_UPDATE,
        AgentMessage(type="ask", payload={"text": text}),
        result_type=AgentMessageReply,
    )
    return parent, sent


def _subscribe(client: Client, workflow_id: str):
    return WorkflowStreamClient.create(client, workflow_id).subscribe(
        topics=[TURN_EVENTS_TOPIC],
        from_offset=0,
        result_type=AgentEvent,
        poll_cooldown=timedelta(milliseconds=10),
    )


async def _first_event(client: Client, workflow_id: str, event_type: AgentEventType):
    async for item in _subscribe(client, workflow_id):
        if item.data.event.type == event_type:
            return item.data.event
    raise AssertionError(f"stream ended before {event_type}")


async def _events_until_turn_end(client: Client, workflow_id: str) -> list[AgentEvent]:
    events = []
    async for item in _subscribe(client, workflow_id):
        events.append(item.data)
        if item.data.event.type == AgentEventType.TURN_END:
            return events
    raise AssertionError("stream ended before turn_end")


async def _until_no_pending_activities(handle: WorkflowHandle) -> None:
    while True:
        description = await handle.describe()
        if not description.raw_description.pending_activities:
            return
        await asyncio.sleep(0.1)


def _reply_text(events: list[AgentEvent], message_id: str) -> str:
    output = next(
        e.event.output
        for e in events
        if e.event.type == AgentEventType.MESSAGE_HANDLER_END and e.message_id == message_id
    )
    return TextReply.model_validate(output).text


@dataclass
class _Gate:
    """What happened at a specialist's approval gate: each auto-mode verdict, by tool, and the
    calls a person (the test) approved because the rules escalated them."""

    verdicts: list[tuple[str, str]]
    approved_by_person: list[str]


async def _approve_escalated_calls(client: Client, workflow_id: str, gate: _Gate) -> None:
    """Record each auto-mode verdict, and approve the escalated calls as a person in the UI would.

    A gated call's ``tool_approval_requested`` is published before the rules judge it, so a
    person only acts on the ones the rules hand back.
    """
    handle = client.get_workflow_handle(workflow_id)
    async for item in _subscribe(client, workflow_id):
        event = item.data.event
        if event.type == AgentEventType.AUTO_APPROVAL_EVALUATION_ENDED:
            gate.verdicts.append((event.tool_name, event.verdict))
            if event.verdict == AutoApprovalVerdict.ESCALATE:
                gate.approved_by_person.append(event.tool_name)
                await handle.execute_update(
                    TOOL_APPROVAL_UPDATE,
                    ToolApprovalDecision(tool_id=event.tool_id, approved=True),
                )


async def _run_concierge_approving_specialists(
    clients: _Clients, text: str
) -> tuple[list[AgentEvent], _Gate, AgentMessageReply]:
    """Ask the concierge, approving every escalated call in each specialist it starts; returns
    the concierge's events up to turn_end and what happened at the specialists' gates."""
    specialist_clients = {
        SHIPPING_TASK_QUEUE: clients.shipping,
        ORDERS_TASK_QUEUE: clients.orders,
    }
    parent, sent = await _ask_concierge(clients.concierge, text)
    events: list[AgentEvent] = []
    gate = _Gate(verdicts=[], approved_by_person=[])
    approvers: list[asyncio.Task] = []
    try:
        async with asyncio.timeout(60):
            async for item in _subscribe(clients.concierge, parent.id):
                events.append(item.data)
                event = item.data.event
                if event.type == AgentEventType.SUBAGENT_STARTED:
                    child = await clients.concierge.get_workflow_handle(event.workflow_id).describe()
                    approvers.append(
                        asyncio.create_task(
                            _approve_escalated_calls(
                                specialist_clients[child.task_queue], event.workflow_id, gate
                            )
                        )
                    )
                if event.type == AgentEventType.TURN_END:
                    break
    finally:
        for approver in approvers:
            approver.cancel()
    return events, gate, sent


async def test_openai_parent_drives_strands_child_on_another_queue():
    """Shipping expedites order 1042, the order the concierge asked about, with no person."""
    async with _dev_server() as env:
        clients = await _clients(env)
        async with _concierge_worker(clients), _shipping_worker(clients):
            events, gate, sent = await _run_concierge_approving_specialists(
                clients, "Is order 1042 on time?"
            )

    types = [e.event.type for e in events]
    assert AgentEventType.SUBAGENT_STARTED in types
    assert AgentEventType.SUBAGENT_MESSAGE_SENT in types
    assert gate.verdicts == [("expedite_shipment", AutoApprovalVerdict.APPROVE)]
    assert gate.approved_by_person == []
    reply = _reply_text(events, sent.message_id)
    assert reply.startswith("Shipping says: ")
    assert "1042" in reply and "EXP-1042" in reply


async def test_concierge_asks_shipping_then_orders_and_the_refund_is_approved_in_orders():
    """The headline prompt: Shipping gives the estimate, then Orders checks eligibility and
    refunds. The $240 refund is over the limit, so a person approves it inside the nested
    Orders subagent."""
    async with _dev_server() as env:
        clients = await _clients(
            env,
            specialists=("shipping", "orders"),
            shipping_model=_ScriptedShippingEstimate,
        )
        async with _concierge_worker(clients), _shipping_worker(clients), _orders_worker(clients):
            events, gate, sent = await _run_concierge_approving_specialists(
                clients, "Order 1042 is late. If it won't arrive by Friday, refund me."
            )

    started = [e.event for e in events if e.event.type == AgentEventType.SUBAGENT_STARTED]
    assert len(started) == 2
    assert gate.verdicts == [("issue_refund", AutoApprovalVerdict.ESCALATE)]
    assert gate.approved_by_person == ["issue_refund"]
    reply = _reply_text(events, sent.message_id)
    shipping_reply, orders_reply = reply.split(" || ")
    assert shipping_reply.startswith("Shipping says: ") and "expected" in shipping_reply
    assert orders_reply.startswith("Orders says: ")
    assert "late" in orders_reply and "RF-1042" in orders_reply


async def test_concierge_holds_nothing_while_orders_waits_for_approval():
    """While Orders waits on a person for the $240 refund, the Concierge has no activity in
    flight, so its Worker can go away; a new Worker finishes the turn once Orders replies."""
    async with _dev_server() as env:
        clients = await _clients(env, specialists=("orders",))
        async with _orders_worker(clients):
            async with asyncio.timeout(60):
                async with _concierge_worker(clients):
                    parent, sent = await _ask_concierge(clients.concierge)
                    started = await _first_event(
                        clients.concierge, parent.id, AgentEventType.SUBAGENT_STARTED
                    )
                    escalated = await _first_event(
                        clients.orders,
                        started.workflow_id,
                        AgentEventType.AUTO_APPROVAL_EVALUATION_ENDED,
                    )
                    assert escalated.verdict == AutoApprovalVerdict.ESCALATE
                    await asyncio.wait_for(_until_no_pending_activities(parent), timeout=10)

                # The Concierge's Worker is gone. Approve, then bring up a new one.
                await clients.orders.get_workflow_handle(started.workflow_id).execute_update(
                    TOOL_APPROVAL_UPDATE,
                    ToolApprovalDecision(tool_id=escalated.tool_id, approved=True),
                )
                async with _concierge_worker(clients):
                    events = await _events_until_turn_end(clients.concierge, parent.id)

    reply = _reply_text(events, sent.message_id)
    assert reply.startswith("Orders says: ")
    assert "RF-1042" in reply
    markers = [
        e.event.type
        for e in events
        if e.event.type
        in (AgentEventType.SUBAGENT_MESSAGE_SENT, AgentEventType.SUBAGENT_REPLY_RECEIVED)
    ]
    assert markers == [AgentEventType.SUBAGENT_MESSAGE_SENT, AgentEventType.SUBAGENT_REPLY_RECEIVED]
    received = next(
        e.event for e in events if e.event.type == AgentEventType.SUBAGENT_REPLY_RECEIVED
    )
    assert received.outcome == "ok"


async def test_child_failure_reaches_the_parent_as_a_tool_error():
    async with _dev_server() as env:
        clients = await _clients(env, shipping_model=_FailingShipping)
        async with _concierge_worker(clients), _shipping_worker(clients):
            parent, sent = await _ask_concierge(clients.concierge)
            async with asyncio.timeout(60):
                events = await _events_until_turn_end(clients.concierge, parent.id)

    received = next(
        e.event for e in events if e.event.type == AgentEventType.SUBAGENT_REPLY_RECEIVED
    )
    assert received.outcome == "error"
    assert _reply_text(events, sent.message_id).startswith("Shipping says: ")
