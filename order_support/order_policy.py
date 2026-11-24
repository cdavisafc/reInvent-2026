# ABOUTME: The specialists' auto-mode decisions: a judge reads the conversation history to say how a
# call's order relates to it, and code turns that, plus what the history shows was done, into a verdict.

"""Auto-mode decisions for refunds and expedites, judged from the conversation history.

Nothing is pinned. On each gated call the evaluator reads the specialist's whole conversation
(its Strands messages: every message received, every tool call and result) and asks a judge one
narrow question: how does the order this call names relate to the conversation? The judge
(Claude on Bedrock, in the ``judge_order_relation`` activity, ``order_judge.py``) answers with an
:class:`OrderRelation`, the order it thinks is being discussed, and the text its answer rests on.

Code then decides, in :func:`decide`, from that answer and one fact it reads from the history
itself: whether a refund or expedite for this order number already ran.

* ``unrelated`` → DENY: the model is told to start a new conversation for that order;
* ``unclear``, or the call names no order → ESCALATE;
* ``claimed`` (only a message, not the store's records, says it's the same purchase) and nothing
  has been done to this number yet → ESCALATE, so a person confirms the claim;
* ``discussed``, ``linked`` (the history's tool results show it's the same purchase under another
  number), or ``claimed`` and already acted on → the limits: an expedite is approved; a refund up
  to :data:`REFUND_AUTO_APPROVE_LIMIT` is approved, over it → ESCALATE.

The model never decides policy, and the dollar limit never depends on a model. Everything here
except the judge runs inside the workflow, so it is deterministic and does no I/O. The harness
publishes each decision, with its ``details``, as ``auto_approval_evaluation_*`` events, and turns a
judge failure into an escalation.
"""

from collections.abc import Awaitable, Callable
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any

from pydantic import BaseModel
from temporal_agent_harness.harness.agent_protocol import (
    AutoApprovalContext,
    AutoApprovalCriteria,
    AutoApprovalCriteriaSet,
    AutoApprovalDecision,
    AutoApprovalVerdict,
)

from order_history import orders_acted_on, transcript

ORDER_RULES = "order-rules"
REFUND_AUTO_APPROVE_LIMIT = Decimal("100.00")
# The specialists' gated tools: the only calls auto mode judges.
GATED_TOOLS = ("issue_refund", "expedite_shipment")
# The judge's activity (order_judge.py), named here so a workflow can call it without importing it.
JUDGE_ACTIVITY = "judge_order_relation"

ORDER_RULES_CRITERIA = AutoApprovalCriteria(
    sets={
        ORDER_RULES: AutoApprovalCriteriaSet(
            effect=(
                "Moves money back to a customer (issue_refund) or pays for faster shipping "
                "(expedite_shipment). Neither can be taken back."
            ),
            approve_when=(
                "The call is for the order the conversation is about, as its history shows.",
                "The call is for an order the history's tool results show is the same purchase.",
                (
                    "The call is for an order only a message says is the same purchase, and an "
                    "earlier call for that number already ran."
                ),
                f"A refund is at most ${REFUND_AUTO_APPROVE_LIMIT}.",
            ),
            deny_when=("Nothing in the conversation's history connects the call's order to it.",),
            escalate_when=(
                (
                    "Only a message, not a tool result, says the order is the same purchase, and "
                    "nothing has been done to that number yet."
                ),
                "It isn't clear which order the conversation is about.",
                f"A refund is more than ${REFUND_AUTO_APPROVE_LIMIT}.",
            ),
        )
    },
    default=ORDER_RULES,
)


class OrderRelation(StrEnum):
    DISCUSSED = "discussed"
    # A tool result (the store's records) shows it's the same purchase.
    LINKED = "linked"
    # Only a message, from a customer or another agent, says it's the same purchase.
    CLAIMED = "claimed"
    UNRELATED = "unrelated"
    UNCLEAR = "unclear"


class OrderJudgment(BaseModel):
    """The judge's reading of the history: how the call's order relates to the conversation."""

    relation: OrderRelation
    # The order the judge thinks the conversation is about, if any.
    discussed_order: str | None = None
    # The text from the history the answer rests on.
    evidence: str = ""


class JudgeRequest(BaseModel):
    """What the judge is shown: the call, and the conversation so far."""

    tool_name: str
    tool_input: dict[str, Any]
    transcript: list[dict[str, Any]]


Judge = Callable[[JudgeRequest], Awaitable[OrderJudgment]]


def _money(value: Any) -> Decimal | None:
    try:
        return Decimal(str(value)).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return None


def _no_order(details: dict[str, Any]) -> AutoApprovalDecision:
    return AutoApprovalDecision(
        verdict=AutoApprovalVerdict.ESCALATE,
        reason="The call names no order, so a person decides.",
        details=details,
    )


def decide(
    tool_name: str,
    tool_input: dict[str, Any],
    judgment: OrderJudgment,
    acted_on: set[str],
) -> AutoApprovalDecision:
    """The verdict on one gated call, from the judge's answer and the orders already acted on."""
    requested = tool_input["order_id"]
    discussed = judgment.discussed_order
    amount = _money(tool_input["amount"]) if "amount" in tool_input else None
    details = {
        "relation": str(judgment.relation),
        "discussed_order": discussed,
        "evidence": judgment.evidence,
        "requested_order": requested,
        "acted_on_before": requested in acted_on,
        "amount": None if amount is None else str(amount),
        "refund_auto_approve_limit": str(REFUND_AUTO_APPROVE_LIMIT),
    }

    def decision(verdict: AutoApprovalVerdict, reason: str) -> AutoApprovalDecision:
        return AutoApprovalDecision(verdict=verdict, reason=reason, details=details)

    about = f"order {discussed}, the order it is about" if discussed else "the order it is about"
    if judgment.relation is OrderRelation.UNRELATED:
        return decision(
            AutoApprovalVerdict.DENY,
            f"Nothing in this conversation connects order {requested} to {about}. "
            f"Start a new conversation for order {requested}.",
        )
    if judgment.relation is OrderRelation.UNCLEAR:
        return decision(
            AutoApprovalVerdict.ESCALATE,
            "This conversation doesn't make clear which order it is about, so a person decides.",
        )
    if judgment.relation is OrderRelation.CLAIMED and requested not in acted_on:
        return decision(
            AutoApprovalVerdict.ESCALATE,
            f"Only a message says order {requested} is the same purchase as order {discussed}: "
            f'"{judgment.evidence}" A person confirms the first action on order {requested}.',
        )

    which = {
        OrderRelation.DISCUSSED: f"order {requested}, the order this conversation is about",
        OrderRelation.LINKED: f"order {requested}, which the store's records link to order {discussed}",
        OrderRelation.CLAIMED: f"order {requested}, confirmed earlier as the same purchase as order {discussed}",
    }[judgment.relation]
    if tool_name == "expedite_shipment":
        return decision(AutoApprovalVerdict.APPROVE, f"Expediting {which}.")
    if tool_name == "issue_refund":
        if amount is None:
            return decision(
                AutoApprovalVerdict.ESCALATE, "The refund names no amount, so a person decides."
            )
        if amount > REFUND_AUTO_APPROVE_LIMIT:
            return decision(
                AutoApprovalVerdict.ESCALATE,
                f"A ${amount} refund for {which} is over the ${REFUND_AUTO_APPROVE_LIMIT} "
                "auto-approval limit, so a person decides.",
            )
        return decision(
            AutoApprovalVerdict.APPROVE,
            f"A ${amount} refund for {which}, within the ${REFUND_AUTO_APPROVE_LIMIT} limit.",
        )
    return decision(
        AutoApprovalVerdict.ESCALATE,
        f"The order rules don't cover {tool_name}, so a person decides.",
    )


def history_evaluator(history: Callable[[], list[dict[str, Any]]], judge: Judge):
    """An auto-mode evaluator that judges each call against ``history()`` as it is at that moment.

    ``history`` returns the specialist's Strands messages; ``judge`` asks the judge (in the
    workflow, an activity). Both are read per call, so every decision sees the whole conversation.
    """

    async def evaluate(context: AutoApprovalContext) -> AutoApprovalDecision:
        if not context.tool_input.get("order_id"):
            return _no_order({"requested_order": None})
        messages = history()
        judgment = await judge(
            JudgeRequest(
                tool_name=context.tool_name,
                tool_input=context.tool_input,
                transcript=transcript(messages),
            )
        )
        return decide(
            context.tool_name,
            context.tool_input,
            judgment,
            orders_acted_on(messages, GATED_TOOLS),
        )

    evaluate.__auto_mode_evaluator__ = ORDER_RULES  # type: ignore[attr-defined]
    return evaluate
