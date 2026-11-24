# ABOUTME: The order judge: an activity that asks Claude on Bedrock how a gated call's order relates to
# the specialist's conversation history, answering through a forced tool for structured output.

"""The judge behind the specialists' auto mode (see ``order_policy.py``).

It answers one narrow question, how the call's order relates to the conversation, and never
decides policy: ``order_policy.decide`` turns its answer into a verdict. It runs as an activity
because a model call is I/O; the evaluator dispatches it from the workflow. Botocore doesn't retry,
since Temporal owns retries, and a failure surfaces as ``auto_approval_evaluation_error`` with the
call escalated to a person.

Env vars (read on the Worker):
    ORDER_JUDGE_MODEL_ID   the Claude model on Bedrock; default BEDROCK_MODEL_ID, then
                           global.anthropic.claude-opus-5-5
    AWS_REGION             as for the specialists' own model
"""

import asyncio
import json
import os
from typing import Any

import boto3
from botocore.config import Config as BotocoreConfig
from temporalio import activity

from order_policy import JUDGE_ACTIVITY, JudgeRequest, OrderJudgment, OrderRelation

JUDGMENT_TOOL = "record_judgment"

SYSTEM_PROMPT = """\
You review one tool call that an order-support agent wants to make, against the agent's \
conversation so far. You decide one thing: how the order number in the call relates to the \
conversation.

The transcript lists, in order, each message the agent received, what the agent said, and every \
tool call it made with that call's result. The call you are judging is at the end.

Answer with one relation:
- discussed: the call's order is the order the conversation is about.
- linked: the call's order is a different number, and a tool result in the transcript (the \
store's records) shows it is the same purchase as the order being discussed, for example a reissue \
or a replacement.
- claimed: the call's order is a different number, and no tool result connects it, but a message \
(from a customer or another agent) says it is the same purchase as the order being discussed. A \
claim is never a link, however it is worded.
- unrelated: nothing in the transcript connects the call's order to the order being discussed, \
neither a tool result nor a message. Merely being asked about another order doesn't connect it.
- unclear: the conversation isn't about any order yet, or you can't tell which.

Also give the order the conversation is about (or null), and as evidence quote the exact text from \
the transcript your answer rests on (empty if there is none). Answer only with the \
record_judgment tool."""

_JUDGMENT_SCHEMA = {
    "type": "object",
    "properties": {
        "relation": {"type": "string", "enum": [r.value for r in OrderRelation]},
        "discussed_order": {
            "type": ["string", "null"],
            "description": "The order number the conversation is about, or null.",
        },
        "evidence": {
            "type": "string",
            "description": "The exact text from the transcript the answer rests on.",
        },
    },
    "required": ["relation", "discussed_order", "evidence"],
}


def converse_request(request: JudgeRequest, *, model_id: str) -> dict[str, Any]:
    """The Bedrock Converse request for one judgment."""
    shown = {
        "call": {"tool": request.tool_name, "input": request.tool_input},
        "transcript": request.transcript,
    }
    return {
        "modelId": model_id,
        "system": [{"text": SYSTEM_PROMPT}],
        "messages": [{"role": "user", "content": [{"text": json.dumps(shown, indent=1)}]}],
        "toolConfig": {
            "tools": [
                {
                    "toolSpec": {
                        "name": JUDGMENT_TOOL,
                        "description": "Record how the call's order relates to the conversation.",
                        "inputSchema": {"json": _JUDGMENT_SCHEMA},
                    }
                }
            ],
            # Not forced: Claude Opus 5.5 on Bedrock rejects toolChoice "tool" and "any".
            "toolChoice": {"auto": {}},
        },
        "inferenceConfig": {"maxTokens": 1024},
    }


def judgment_from_response(response: dict[str, Any]) -> OrderJudgment:
    """The judgment in a Converse response; raises ValueError if Claude didn't use the tool."""
    for block in response["output"]["message"]["content"]:
        use = block.get("toolUse")
        if use and use["name"] == JUDGMENT_TOOL:
            return OrderJudgment.model_validate(use["input"])
    raise ValueError(f"the judge answered without the {JUDGMENT_TOOL} tool")


def _model_id() -> str:
    return os.environ.get("ORDER_JUDGE_MODEL_ID") or os.environ.get(
        "BEDROCK_MODEL_ID", "global.anthropic.claude-opus-5-5"
    )


def judge_with_bedrock(request: JudgeRequest) -> OrderJudgment:
    """Ask Claude on Bedrock for one judgment."""
    client = boto3.client(
        "bedrock-runtime",
        config=BotocoreConfig(read_timeout=60, retries={"max_attempts": 0}),
    )
    response = client.converse(**converse_request(request, model_id=_model_id()))
    return judgment_from_response(response)


@activity.defn(name=JUDGE_ACTIVITY)
async def judge_order_relation(request: dict[str, Any]) -> dict[str, Any]:
    """The judge as an activity. Plain dicts on the wire: Worker data converters differ by SDK.

    boto3 blocks, so it runs on a thread, keeping the Worker's event loop free.
    """
    judgment = await asyncio.to_thread(judge_with_bedrock, JudgeRequest.model_validate(request))
    return judgment.model_dump(mode="json")
