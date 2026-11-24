# ABOUTME: Tests for the specialists' auto-mode decisions: approve / deny / escalate from the judge's
# reading of the conversation history, what the history shows was already acted on, and the $ limit.

from decimal import Decimal

import pytest
from temporal_agent_harness.harness.agent_protocol import (
    AutoApprovalContext,
    AutoApprovalVerdict,
)

from order_policy import (
    ORDER_RULES,
    ORDER_RULES_CRITERIA,
    REFUND_AUTO_APPROVE_LIMIT,
    JudgeRequest,
    OrderJudgment,
    OrderRelation,
    decide,
    history_evaluator,
)

APPROVE, DENY, ESCALATE = (
    AutoApprovalVerdict.APPROVE,
    AutoApprovalVerdict.DENY,
    AutoApprovalVerdict.ESCALATE,
)
REISSUE = "Order 1028: lost. This order was reissued as order 1077."
CLAIM = "Order 1021 is the replacement for order 1028."


def _judged(relation, discussed="1028", evidence=REISSUE) -> OrderJudgment:
    return OrderJudgment(relation=relation, discussed_order=discussed, evidence=evidence)


def _refund(order_id, amount):
    return {"order_id": order_id, "amount": amount, "reason": "late"}


# --------------------------------------------------------------------------- the order


def test_an_order_the_history_doesnt_connect_is_denied():
    decision = decide(
        "expedite_shipment", {"order_id": "1021"}, _judged(OrderRelation.UNRELATED, evidence=""), set()
    )
    assert decision.verdict is DENY
    assert "order 1021" in decision.reason and "order 1028" in decision.reason
    assert "new conversation" in decision.reason


def test_unclear_escalates():
    decision = decide(
        "expedite_shipment", {"order_id": "1028"}, _judged(OrderRelation.UNCLEAR, discussed=None), set()
    )
    assert decision.verdict is ESCALATE


def test_an_order_the_records_link_is_the_same_order_straight_away():
    decision = decide("expedite_shipment", {"order_id": "1077"}, _judged(OrderRelation.LINKED), set())
    assert decision.verdict is APPROVE
    assert "order 1028" in decision.reason


def test_a_link_only_claimed_in_a_message_escalates_the_first_time_quoting_the_claim():
    judged = _judged(OrderRelation.CLAIMED, evidence=CLAIM)
    decision = decide("expedite_shipment", {"order_id": "1021"}, judged, set())
    assert decision.verdict is ESCALATE
    assert "order 1021" in decision.reason and "order 1028" in decision.reason
    assert CLAIM in decision.reason


def test_a_claimed_order_already_acted_on_is_treated_as_the_same_order():
    judged = _judged(OrderRelation.CLAIMED, evidence=CLAIM)
    decision = decide("expedite_shipment", {"order_id": "1021"}, judged, acted_on={"1021"})
    assert decision.verdict is APPROVE


def test_acting_on_the_original_doesnt_clear_a_claimed_number():
    judged = _judged(OrderRelation.CLAIMED, evidence=CLAIM)
    decision = decide("expedite_shipment", {"order_id": "1021"}, judged, acted_on={"1028"})
    assert decision.verdict is ESCALATE


def test_the_discussed_order_is_expedited():
    decision = decide("expedite_shipment", {"order_id": "1028"}, _judged(OrderRelation.DISCUSSED), set())
    assert decision.verdict is APPROVE


# --------------------------------------------------------------------------- the limit


@pytest.mark.parametrize("amount", [0.01, 45.0, float(REFUND_AUTO_APPROVE_LIMIT)])
def test_refund_of_the_discussed_order_up_to_the_limit_is_approved(amount):
    decision = decide("issue_refund", _refund("1028", amount), _judged(OrderRelation.DISCUSSED), set())
    assert decision.verdict is APPROVE


@pytest.mark.parametrize("amount", [100.01, 240.0])
def test_refund_over_the_limit_escalates(amount):
    decision = decide("issue_refund", _refund("1028", amount), _judged(OrderRelation.DISCUSSED), set())
    assert decision.verdict is ESCALATE
    assert "$100.00" in decision.reason


def test_the_limit_still_applies_to_a_linked_order():
    judged = _judged(OrderRelation.LINKED)
    small = decide("issue_refund", _refund("1077", 25.0), judged, set())
    large = decide("issue_refund", _refund("1077", 310.0), judged, set())
    assert (small.verdict, large.verdict) == (APPROVE, ESCALATE)


def test_refund_without_an_amount_escalates():
    decision = decide("issue_refund", {"order_id": "1028"}, _judged(OrderRelation.DISCUSSED), set())
    assert decision.verdict is ESCALATE


def test_a_tool_the_rules_dont_cover_escalates():
    decision = decide("cancel_order", {"order_id": "1028"}, _judged(OrderRelation.DISCUSSED), set())
    assert decision.verdict is ESCALATE


def test_every_decision_records_what_it_was_made_from():
    decision = decide("issue_refund", _refund("1077", 25.0), _judged(OrderRelation.LINKED), {"1077"})
    assert decision.details == {
        "relation": "linked",
        "discussed_order": "1028",
        "evidence": REISSUE,
        "requested_order": "1077",
        "acted_on_before": True,
        "amount": "25.00",
        "refund_auto_approve_limit": "100.00",
    }


def test_the_limit_is_one_hundred_dollars():
    assert REFUND_AUTO_APPROVE_LIMIT == Decimal("100.00")


# --------------------------------------------------------------------------- the evaluator


def _context(tool_name, tool_input) -> AutoApprovalContext:
    return AutoApprovalContext(
        tool_name=tool_name,
        tool_input=tool_input,
        inherently_safe=False,
        criteria=ORDER_RULES_CRITERIA,
        criteria_set=ORDER_RULES_CRITERIA.sets[ORDER_RULES],
        criteria_set_name=ORDER_RULES,
    )


HISTORY = [
    {"role": "user", "content": [{"text": "Where is order 1028?"}]},
    {"role": "assistant", "content": [{"toolUse": {"toolUseId": "t1", "name": "track_shipment", "input": {"order_id": "1028"}}}]},
    {"role": "user", "content": [{"toolResult": {"toolUseId": "t1", "status": "success", "content": [{"text": REISSUE}]}}]},
]


async def test_evaluator_asks_the_judge_about_the_live_history_and_names_itself():
    history = list(HISTORY)
    asked: list[JudgeRequest] = []

    async def judge(request: JudgeRequest) -> OrderJudgment:
        asked.append(request)
        return _judged(OrderRelation.LINKED)

    evaluator = history_evaluator(lambda: history, judge)
    decision = await evaluator(_context("expedite_shipment", {"order_id": "1077"}))

    assert decision.verdict is APPROVE
    [request] = asked
    assert request.tool_name == "expedite_shipment"
    assert request.tool_input == {"order_id": "1077"}
    assert request.transcript[0] == {"from": "message to this agent", "text": "Where is order 1028?"}
    assert evaluator.__auto_mode_evaluator__ == ORDER_RULES


async def test_evaluator_reads_acted_on_orders_from_the_history():
    history = [
        *HISTORY,
        {"role": "assistant", "content": [{"toolUse": {"toolUseId": "t2", "name": "expedite_shipment", "input": {"order_id": "1021"}}}]},
        {"role": "user", "content": [{"toolResult": {"toolUseId": "t2", "status": "success", "content": [{"text": "EXP-1021"}]}}]},
    ]

    async def judge(request: JudgeRequest) -> OrderJudgment:
        return _judged(OrderRelation.CLAIMED, evidence=CLAIM)

    decision = await history_evaluator(lambda: history, judge)(
        _context("expedite_shipment", {"order_id": "1021"})
    )
    assert decision.verdict is APPROVE


async def test_evaluator_escalates_a_call_with_no_order_without_asking_the_judge():
    async def judge(request: JudgeRequest) -> OrderJudgment:
        raise AssertionError("the judge shouldn't be asked")

    decision = await history_evaluator(lambda: HISTORY, judge)(_context("expedite_shipment", {}))
    assert decision.verdict is ESCALATE


def test_criteria_cover_every_tool_by_default():
    assert ORDER_RULES_CRITERIA.default == ORDER_RULES
    assert not ORDER_RULES_CRITERIA.sets[ORDER_RULES].is_empty
