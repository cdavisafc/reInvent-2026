# ABOUTME: A deterministic stand-in for the order judge activity, for tests with no Bedrock: reads the
# transcript with fixed rules that follow the real judge's contract (only a tool result links orders).

import re
from typing import Any

from temporalio import activity

from order_policy import JUDGE_ACTIVITY, JudgeRequest, OrderJudgment, OrderRelation

_ORDER = re.compile(r"\border (\d{4})\b", re.IGNORECASE)


def judge(request: JudgeRequest) -> OrderJudgment:
    """The discussed order is the first one a message to the agent names. Another order is linked
    when a tool result says one was reissued as, or replaces, the other, and claimed when only a
    message says it replaces the discussed order."""
    messages = [e["text"] for e in request.transcript if e.get("from") == "message to this agent"]
    named = [match for text in messages for match in _ORDER.findall(text)]
    if not named:
        return OrderJudgment(relation=OrderRelation.UNCLEAR)
    discussed = named[0]
    requested = request.tool_input["order_id"]
    if requested == discussed:
        return OrderJudgment(relation=OrderRelation.DISCUSSED, discussed_order=discussed)
    links = (
        f"order {discussed}",
        f"reissued as order {requested}",
        f"replacement for order {discussed}",
    )
    for entry in request.transcript:
        text = entry.get("text", "")
        if "tool_result_for" in entry and links[0].lower() in text.lower() and (
            links[1] in text or links[2] in text
        ):
            return OrderJudgment(
                relation=OrderRelation.LINKED, discussed_order=discussed, evidence=text
            )
    claim = f"order {requested} is the replacement for order {discussed}"
    for text in messages:
        if claim in text.lower():
            return OrderJudgment(
                relation=OrderRelation.CLAIMED, discussed_order=discussed, evidence=text
            )
    return OrderJudgment(relation=OrderRelation.UNRELATED, discussed_order=discussed)


@activity.defn(name=JUDGE_ACTIVITY)
async def fake_judge_order_relation(request: dict[str, Any]) -> dict[str, Any]:
    return judge(JudgeRequest.model_validate(request)).model_dump(mode="json")
