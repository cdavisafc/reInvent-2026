# ABOUTME: End-to-end auto-mode tests on a local dev server: a specialist judges each gated call from
# its conversation history, across turns, and records every decision as agent events.

"""A specialist driven directly over several turns, with a scripted Strands model and the fake judge.

Earlier turns build the history: which order is being discussed, what the tools said about it
(a reissue), and what has already been done. A later turn's refund or expedite is then approved,
denied or escalated to a person from that history, and the harness publishes each decision on the
agent's event stream.
"""

import asyncio
import json
import uuid
from collections.abc import AsyncIterable
from typing import Any, ClassVar

from strands.models import Model as StrandsModel
from strands.types.streaming import StreamEvent
from temporal_agent_harness.harness.agent_protocol import (
    SEND_AGENT_MESSAGE_UPDATE,
    TOOL_APPROVAL_UPDATE,
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

from contracts import ORDERS_TASK_QUEUE, SHIPPING_TASK_QUEUE
from hosted_agents import orders_plugins, shipping_plugins
from order_policy import ORDER_RULES
from orders.workflow import OrdersAgentWorkflow
from shipping.workflow import ShippingAgentWorkflow
from tests.fake_judge import fake_judge_order_relation
from tests.test_subagent_e2e import _dev_server, _subscribe, _worker

# What the scripted model calls for each prompt, keyed by the prompt's text.
SCRIPT: dict[str, list[tuple[str, dict[str, Any]]]] = {
    "Can I return order 1017?": [("check_return_eligibility", {"order_id": "1017"})],
    "Order 1042 is late. What can you do?": [("check_return_eligibility", {"order_id": "1042"})],
    "Refund it.": [("issue_refund", {"order_id": "1017", "amount": 45.0, "reason": "return"})],
    "Refund the whole order.": [
        ("issue_refund", {"order_id": "1042", "amount": 240.0, "reason": "late delivery"})
    ],
    "Actually, refund order 1042 instead.": [
        ("issue_refund", {"order_id": "1042", "amount": 240.0, "reason": "late delivery"})
    ],
}


def _text(block: dict) -> str:
    return block.get("text", "")


class ScriptedByPrompt(StrandsModel):
    """Calls the tools ``SCRIPT`` lists for the latest prompt, one per model call, then replies
    with those calls' results joined. Reads the conversation, so it needs no state of its own."""

    SCRIPT: ClassVar[dict[str, list[tuple[str, dict[str, Any]]]]] = {}

    def update_config(self, **_model_config: Any) -> None:
        return None

    def get_config(self) -> dict[str, Any]:
        return {"model_id": "scripted"}

    def structured_output(self, *_args: Any, **_kwargs: Any) -> Any:
        raise NotImplementedError

    async def stream(self, messages: Any, *_args: Any, **_kwargs: Any) -> AsyncIterable[StreamEvent]:
        prompt_index = max(
            i
            for i, m in enumerate(messages)
            if m["role"] == "user" and any("text" in block for block in m["content"])
        )
        prompt = "".join(_text(b) for b in messages[prompt_index]["content"])
        results = [
            "".join(_text(c) for c in block["toolResult"]["content"])
            for message in messages[prompt_index + 1 :]
            for block in message["content"]
            if "toolResult" in block
        ]
        calls = self.SCRIPT[prompt]
        yield {"messageStart": {"role": "assistant"}}
        if len(results) < len(calls):
            name, tool_input = calls[len(results)]
            tool_use_id = f"tu-{prompt_index}-{len(results)}"
            yield {
                "contentBlockStart": {
                    "start": {"toolUse": {"toolUseId": tool_use_id, "name": name}}
                }
            }
            yield {"contentBlockDelta": {"delta": {"toolUse": {"input": json.dumps(tool_input)}}}}
            yield {"contentBlockStop": {}}
            yield {"messageStop": {"stopReason": "tool_use"}}
            return
        yield {"contentBlockDelta": {"delta": {"text": " | ".join(results)}}}
        yield {"contentBlockStop": {}}
        yield {"messageStop": {"stopReason": "end_turn"}}


class _ScriptedOrders(ScriptedByPrompt):
    SCRIPT = SCRIPT


class _ScriptedShipping(ScriptedByPrompt):
    SCRIPT: ClassVar = {
        "Where is order 1028?": [("track_shipment", {"order_id": "1028"})],
        "Expedite order 1077.": [("expedite_shipment", {"order_id": "1077"})],
        "Order 1021 is the replacement for order 1028. Expedite order 1021.": [
            ("expedite_shipment", {"order_id": "1021"})
        ],
        "Expedite order 1021 again.": [("expedite_shipment", {"order_id": "1021"})],
        "Expedite order 1021.": [("expedite_shipment", {"order_id": "1021"})],
    }


async def _send(handle: WorkflowHandle, text: str) -> str:
    sent = await handle.execute_update(
        SEND_AGENT_MESSAGE_UPDATE,
        AgentMessage(type="ask", payload={"text": text}),
        result_type=AgentMessageReply,
    )
    return sent.message_id


async def _conversation(
    client: Client, workflow: type, task_queue: str, prompts: list[str]
) -> tuple[list[AgentEvent], list[str]]:
    """Send each prompt as its own turn, approving any escalated call as a person would; returns
    every event and each prompt's message id."""
    handle = await client.start_workflow(
        workflow.run,  # type: ignore[attr-defined]  # added by @agent.defn
        AgentConfig(),
        id=f"{task_queue}-{uuid.uuid4()}",
        task_queue=task_queue,
    )
    events: list[AgentEvent] = []
    message_ids: list[str] = []
    async with asyncio.timeout(60):
        stream = _subscribe(client, handle.id)
        for prompt in prompts:
            message_ids.append(await _send(handle, prompt))
            async for item in stream:
                events.append(item.data)
                event = item.data.event
                if (
                    event.type == AgentEventType.AUTO_APPROVAL_EVALUATION_ENDED
                    and event.verdict == AutoApprovalVerdict.ESCALATE
                ):
                    await handle.execute_update(
                        TOOL_APPROVAL_UPDATE,
                        ToolApprovalDecision(tool_id=event.tool_id, approved=True),
                    )
                if event.type == AgentEventType.TURN_END:
                    break
    return events, message_ids


async def _run(prompts: list[str], *, specialist: str = "orders") -> tuple[list[AgentEvent], list[str]]:
    plugins, task_queue, workflow = {
        "orders": (orders_plugins(_ScriptedOrders, judge=fake_judge_order_relation), ORDERS_TASK_QUEUE, OrdersAgentWorkflow),
        "shipping": (shipping_plugins(_ScriptedShipping, judge=fake_judge_order_relation), SHIPPING_TASK_QUEUE, ShippingAgentWorkflow),
    }[specialist]
    async with _dev_server() as env:
        client = await Client.connect(
            env.client.service_client.config.target_host,
            namespace=env.client.namespace,
            plugins=plugins,
        )
        async with _worker(client, task_queue, workflow):
            return await _conversation(client, workflow, task_queue, prompts)


def _of(events: list[AgentEvent], event_type: AgentEventType) -> list:
    return [e.event for e in events if e.event.type == event_type]


def _reply(events: list[AgentEvent], message_id: str) -> str:
    output = next(
        e.event.output
        for e in events
        if e.event.type == AgentEventType.MESSAGE_HANDLER_END and e.message_id == message_id
    )
    return TextReply.model_validate(output).text


async def test_small_refund_of_the_order_discussed_earlier_runs_with_no_person():
    events, ids = await _run(["Can I return order 1017?", "Refund it."])

    [evaluation] = _of(events, AgentEventType.AUTO_APPROVAL_EVALUATION_ENDED)
    assert (evaluation.tool_name, evaluation.verdict) == ("issue_refund", AutoApprovalVerdict.APPROVE)
    assert evaluation.evaluator == ORDER_RULES
    assert evaluation.details["relation"] == "discussed"
    assert evaluation.details["discussed_order"] == "1017"
    [resolved] = _of(events, AgentEventType.TOOL_APPROVAL_RESOLVED)
    assert resolved.approved and resolved.reason == evaluation.reason
    assert "RF-1017" in _reply(events, ids[1])


async def test_refund_of_another_order_is_rejected_and_never_runs():
    events, ids = await _run(["Can I return order 1017?", "Actually, refund order 1042 instead."])

    [evaluation] = _of(events, AgentEventType.AUTO_APPROVAL_EVALUATION_ENDED)
    assert evaluation.verdict == AutoApprovalVerdict.DENY
    assert evaluation.details["requested_order"] == "1042"
    assert evaluation.details["relation"] == "unrelated"
    [resolved] = _of(events, AgentEventType.TOOL_APPROVAL_RESOLVED)
    assert not resolved.approved
    assert "issue_refund" not in [e.tool_name for e in _of(events, AgentEventType.TOOL_START)]
    # The model is told why, so it can explain it to the customer.
    assert "Start a new conversation for order 1042" in _reply(events, ids[1])


async def test_large_refund_of_the_order_discussed_waits_for_a_person():
    events, ids = await _run(["Order 1042 is late. What can you do?", "Refund the whole order."])

    [evaluation] = _of(events, AgentEventType.AUTO_APPROVAL_EVALUATION_ENDED)
    assert evaluation.verdict == AutoApprovalVerdict.ESCALATE
    assert evaluation.details["amount"] == "240.00"
    # Escalating resolves nothing; the person's approval (the test's) does.
    [resolved] = _of(events, AgentEventType.TOOL_APPROVAL_RESOLVED)
    assert resolved.approved and resolved.reason != evaluation.reason
    assert "RF-1042" in _reply(events, ids[1])


async def test_lookups_are_never_judged():
    events, _ = await _run(["Can I return order 1017?"])

    assert _of(events, AgentEventType.AUTO_APPROVAL_EVALUATION_STARTED) == []
    assert _of(events, AgentEventType.TOOL_APPROVAL_REQUESTED) == []


async def test_an_order_the_records_link_is_expedited_with_no_person():
    """The history, not a stored value: the tool result about order 1028 is what links 1077 to it."""
    events, ids = await _run(["Where is order 1028?", "Expedite order 1077."], specialist="shipping")

    [evaluation] = _of(events, AgentEventType.AUTO_APPROVAL_EVALUATION_ENDED)
    assert (evaluation.verdict, evaluation.details["relation"]) == (AutoApprovalVerdict.APPROVE, "linked")
    assert "reissued as order 1077" in evaluation.details["evidence"]
    assert "EXP-1077" in _reply(events, ids[1])


async def test_a_claimed_link_needs_a_person_once_then_counts_as_the_same_order():
    """Only a message says 1021 replaces 1028, so a person confirms the first expedite. The
    history then shows that expedite ran, which lets the next one through."""
    events, ids = await _run(
        [
            "Where is order 1028?",
            "Order 1021 is the replacement for order 1028. Expedite order 1021.",
            "Expedite order 1021 again.",
        ],
        specialist="shipping",
    )

    first, second = _of(events, AgentEventType.AUTO_APPROVAL_EVALUATION_ENDED)
    assert (first.verdict, first.details["relation"]) == (AutoApprovalVerdict.ESCALATE, "claimed")
    assert "replacement for order 1028" in first.reason
    assert first.details["acted_on_before"] is False
    assert (second.verdict, second.details["relation"]) == (AutoApprovalVerdict.APPROVE, "claimed")
    assert second.details["acted_on_before"] is True
    assert "EXP-1021" in _reply(events, ids[1]) and "EXP-1021" in _reply(events, ids[2])


async def test_an_order_nothing_in_the_history_connects_is_rejected():
    events, ids = await _run(["Where is order 1028?", "Expedite order 1021."], specialist="shipping")

    [evaluation] = _of(events, AgentEventType.AUTO_APPROVAL_EVALUATION_ENDED)
    assert evaluation.verdict == AutoApprovalVerdict.DENY
    assert "Start a new conversation for order 1021" in _reply(events, ids[1])
