# ABOUTME: Tests for reading a specialist's conversation history: the transcript the judge reads, and
# which orders a gated tool already acted on.

from order_history import orders_acted_on, transcript

GATED = ("issue_refund", "expedite_shipment")


def _user(text):
    return {"role": "user", "content": [{"text": text}]}


def _assistant(*blocks):
    return {"role": "assistant", "content": list(blocks)}


def _use(tool_use_id, name, **tool_input):
    return {"toolUse": {"toolUseId": tool_use_id, "name": name, "input": tool_input}}


def _result(tool_use_id, text, status="success"):
    return {
        "role": "user",
        "content": [
            {"toolResult": {"toolUseId": tool_use_id, "status": status, "content": [{"text": text}]}}
        ],
    }


CONVERSATION = [
    _user("Where is order 1028?"),
    _assistant({"text": "Let me check."}, _use("t1", "track_shipment", order_id="1028")),
    _result("t1", "Order 1028: lost. This order was reissued as order 1077."),
    _assistant({"text": "It was lost and reissued as order 1077."}),
    _user("Expedite order 1077."),
    _assistant(_use("t2", "expedite_shipment", order_id="1077")),
]


def test_transcript_keeps_who_said_what_and_every_tool_call_and_result_in_order():
    assert transcript(CONVERSATION) == [
        {"from": "message to this agent", "text": "Where is order 1028?"},
        {"from": "this agent", "text": "Let me check."},
        {"tool_call": "track_shipment", "id": "t1", "input": {"order_id": "1028"}},
        {
            "tool_result_for": "t1",
            "status": "success",
            "text": "Order 1028: lost. This order was reissued as order 1077.",
        },
        {"from": "this agent", "text": "It was lost and reissued as order 1077."},
        {"from": "message to this agent", "text": "Expedite order 1077."},
        {"tool_call": "expedite_shipment", "id": "t2", "input": {"order_id": "1077"}},
    ]


def test_transcript_skips_blocks_it_has_no_use_for():
    messages = [_assistant({"reasoningContent": {"reasoningText": {"text": "hmm"}}}, {"text": "Hi"})]
    assert transcript(messages) == [{"from": "this agent", "text": "Hi"}]


def test_transcript_shortens_long_tool_results():
    entry = transcript([_result("t1", "x" * 5000)])[0]
    assert len(entry["text"]) <= 2000


def test_a_gated_call_that_ran_counts_as_acted_on():
    messages = [*CONVERSATION, _result("t2", "Order 1077 upgraded; confirmation EXP-1077.")]
    assert orders_acted_on(messages, GATED) == {"1077"}


def test_a_call_still_waiting_for_its_result_does_not_count():
    # The call being judged is in the history already, with no result yet.
    assert orders_acted_on(CONVERSATION, GATED) == set()


def test_a_denied_call_does_not_count():
    messages = [*CONVERSATION, _result("t2", "denied: not this order", status="error")]
    assert orders_acted_on(messages, GATED) == set()


def test_lookups_do_not_count():
    assert orders_acted_on(CONVERSATION[:3], GATED) == set()
