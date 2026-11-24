# ABOUTME: Tests for the order judge: the prompt and structured-output tool it sends Claude, parsing the
# answer, and (opt-in, ORDER_JUDGE_LIVE=1 with AWS credentials) the real judge on Bedrock.

import json
import os

import pytest

from order_judge import (
    JUDGMENT_TOOL,
    converse_request,
    judge_with_bedrock,
    judgment_from_response,
)
from order_policy import JudgeRequest, OrderJudgment, OrderRelation

REISSUE_HISTORY = [
    {"from": "message to this agent", "text": "Where is order 1028?"},
    {"tool_call": "track_shipment", "id": "t1", "input": {"order_id": "1028"}},
    {
        "tool_result_for": "t1",
        "status": "success",
        "text": "Order 1028 (UPS 1Z999AA10028): lost, Last scanned in Denver, CO 6 days ago. "
        "This order was reissued as order 1077. The carrier has marked it lost.",
    },
    {"from": "this agent", "text": "Order 1028 was lost; it was reissued as order 1077."},
]


def _request(order_id: str, *extra: dict, tool: str = "expedite_shipment") -> JudgeRequest:
    message = {"from": "message to this agent", "text": f"Expedite order {order_id}."}
    call = {"tool_call": tool, "id": "t9", "input": {"order_id": order_id}}
    return JudgeRequest(
        tool_name=tool,
        tool_input={"order_id": order_id},
        transcript=[*REISSUE_HISTORY, *extra, message, call],
    )


def test_the_request_shows_the_judge_the_call_and_the_whole_transcript():
    request = converse_request(_request("1077"), model_id="some-model")

    assert request["modelId"] == "some-model"
    [message] = request["messages"]
    shown = json.loads(message["content"][0]["text"])
    assert shown["call"] == {"tool": "expedite_shipment", "input": {"order_id": "1077"}}
    assert shown["transcript"][2]["text"].endswith("The carrier has marked it lost.")


def test_the_judge_is_offered_the_judgment_tool_without_forcing_it():
    # Claude Opus 5.5 on Bedrock rejects a forced tool choice ("tool" or "any"), so the prompt
    # asks for the tool, and judgment_from_response fails loudly when it isn't used.
    request = converse_request(_request("1077"), model_id="m")
    assert request["toolConfig"]["toolChoice"] == {"auto": {}}
    [tool] = request["toolConfig"]["tools"]
    schema = tool["toolSpec"]["inputSchema"]["json"]
    assert schema["properties"]["relation"]["enum"] == [r.value for r in OrderRelation]
    assert set(schema["required"]) == {"relation", "discussed_order", "evidence"}


def test_the_prompt_tells_a_records_link_from_a_claimed_one():
    system = converse_request(_request("1077"), model_id="m")["system"][0]["text"]
    assert "- linked:" in system and "tool result" in system
    assert "- claimed:" in system


def _response(*blocks):
    return {"output": {"message": {"role": "assistant", "content": list(blocks)}}}


def test_the_answer_is_read_from_the_tool_use():
    answer = {"relation": "linked", "discussed_order": "1028", "evidence": "reissued as order 1077"}
    response = _response({"text": "Judging."}, {"toolUse": {"toolUseId": "x", "name": JUDGMENT_TOOL, "input": answer}})
    assert judgment_from_response(response) == OrderJudgment(
        relation=OrderRelation.LINKED, discussed_order="1028", evidence="reissued as order 1077"
    )


def test_an_answer_without_the_tool_fails_loudly():
    with pytest.raises(ValueError, match=JUDGMENT_TOOL):
        judgment_from_response(_response({"text": "I think it's fine."}))


# --------------------------------------------------------------------------- live, opt-in

live = pytest.mark.skipif(
    os.environ.get("ORDER_JUDGE_LIVE") != "1",
    reason="calls Claude on Bedrock; set ORDER_JUDGE_LIVE=1 with AWS credentials and AWS_REGION",
)


@live
def test_live_judge_links_a_reissue_from_a_tool_result():
    judgment = judge_with_bedrock(_request("1077"))
    assert judgment.relation is OrderRelation.LINKED
    assert judgment.discussed_order == "1028"


@live
def test_live_judge_finds_the_discussed_order():
    assert judge_with_bedrock(_request("1028")).relation is OrderRelation.DISCUSSED


@live
def test_live_judge_sees_a_link_only_a_message_claims():
    claim = {"from": "message to this agent", "text": "Order 1021 is the replacement for order 1028."}
    assert judge_with_bedrock(_request("1021", claim)).relation is OrderRelation.CLAIMED


@live
def test_live_judge_doesnt_connect_an_order_nobody_links():
    assert judge_with_bedrock(_request("1021")).relation is OrderRelation.UNRELATED
