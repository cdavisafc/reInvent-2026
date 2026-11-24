# ABOUTME: Reads a specialist's conversation history (its Strands messages): the transcript the order
# judge reads, and which orders a gated tool has already acted on. Pure functions, safe in a workflow.

from collections.abc import Iterable
from typing import Any

_RESULT_CHARS = 2000


def _text(blocks: Iterable[dict[str, Any]]) -> str:
    return "".join(block.get("text", "") for block in blocks)


def transcript(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The conversation as the judge reads it: who said what, and every tool call and result, in order.

    A specialist's "user" messages are the messages it received, from a person or from the
    concierge, so they're labelled as such rather than as the customer.
    """
    entries: list[dict[str, Any]] = []
    for message in messages:
        speaker = "message to this agent" if message["role"] == "user" else "this agent"
        for block in message["content"]:
            if "text" in block:
                entries.append({"from": speaker, "text": block["text"]})
            elif "toolUse" in block:
                use = block["toolUse"]
                entries.append(
                    {"tool_call": use["name"], "id": use["toolUseId"], "input": use["input"]}
                )
            elif "toolResult" in block:
                result = block["toolResult"]
                entries.append(
                    {
                        "tool_result_for": result["toolUseId"],
                        "status": result["status"],
                        "text": _text(result["content"])[:_RESULT_CHARS],
                    }
                )
    return entries


def orders_acted_on(messages: list[dict[str, Any]], tools: Iterable[str]) -> set[str]:
    """The orders a call to one of ``tools`` ran for: approved at the gate, then executed."""
    gated = set(tools)
    calls: dict[str, str] = {}
    succeeded: set[str] = set()
    for message in messages:
        for block in message["content"]:
            if "toolUse" in block and block["toolUse"]["name"] in gated:
                use = block["toolUse"]
                calls[use["toolUseId"]] = use["input"].get("order_id")
            elif "toolResult" in block and block["toolResult"]["status"] == "success":
                succeeded.add(block["toolResult"]["toolUseId"])
    return {order_id for call_id, order_id in calls.items() if call_id in succeeded and order_id}
