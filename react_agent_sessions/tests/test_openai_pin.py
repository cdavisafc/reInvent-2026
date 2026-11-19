# ABOUTME: Guards the openai SDK pin the harness's streaming observer depends on.
# The harness reads ResponseFunctionCallArgumentsDoneEvent.name on every tool call; openai>=3.9 dropped it.

from openai.types.responses import ResponseFunctionCallArgumentsDoneEvent


def test_function_call_done_event_still_carries_name() -> None:
    # temporal-agent-harness 0.11 (openai_agents_harness._emit_tool_requested) evaluates
    # event.name eagerly, so without this field every streamed tool call fails the model
    # activity with AttributeError. Remove the openai pin in pyproject.toml once the harness
    # stops reading it.
    assert "name" in ResponseFunctionCallArgumentsDoneEvent.model_fields
