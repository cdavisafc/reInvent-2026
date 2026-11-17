# ABOUTME: The ReAct agent workflow: an OpenAI Agents SDK agent on temporal-agent-harness.
# Chains weather/geo/IP activity tools and an ask_user callback tool until it can answer.

"""The ReAct agent: a multi-tool OpenAI Agents SDK agent on the harness.

A conversational agent in the ReAct pattern — it reasons, then acts by calling a tool, and loops
on the result until it can answer in plain text. It finds the weather for a named city
(``get_coordinates`` -> ``get_weather``) or for the user's current location (``get_ip_address``
-> ``get_location_info`` -> ``get_weather``).

Streaming is a toggle (``REACT_AGENT_STREAM``, default on — see ``STREAM_RESPONSES``). Streaming
runs the model with ``Runner.run_streamed(..., context=self._runner)`` — passing the harness runner
as the SDK run context is what lets the streaming seam resolve the in-flight turn — so model calls
route through the streaming activity and the harness observer translates raw OpenAI events into the
live turn stream. Non-streaming uses ``Runner.run(...)``: the turn runs to completion and returns
one reply, with tool lifecycle / ``ask_user`` / the final reply still on the turn stream but no
``reply_delta`` or ``model_interaction_*``. The local tools are durable harness activity tools
adapted onto the SDK with ``as_openai_agent_tools`` (so the harness owns the approval policy and
each tool's ``tool_start`` / ``tool_end`` / ``tool_error`` events). It also has one
human-in-the-loop tool, ``ask_user`` (an ``@agent.callback_tool_defn`` callback tool): when the
model needs clarification it parks the turn in-workflow until an external client returns the
user's answer — see ``client.py``.

The worker that hosts this workflow runs on Amazon Bedrock AgentCore Runtime as a Temporal
Serverless Worker — see ``agentcore_worker.py``.
"""

from __future__ import annotations

import os

from temporalio import workflow
from temporalio.contrib.workflow_streams import WorkflowStream

with workflow.unsafe.imports_passed_through():
    from agents import Agent as OpenAIAgent
    from agents import Runner, TResponseInputItem
    from temporal_agent_harness.ai_sdks.openai_agents_harness import (
        as_openai_agent_tools,
    )
    from temporal_agent_harness.harness import agent
    from temporal_agent_harness.harness.agent_protocol import (
        AgentConfig,
        TextMessage,
        TextReply,
        ToolApprovalPolicy,
    )
    from temporal_agent_harness.harness.agent_workflow import AgentWorkflowRunner

    from human_tools import HUMAN_TOOLS
    from tool_activities import ALL_TOOLS


TASK_QUEUE = "react-agent"
DEFAULT_MODEL = "gpt-5.6-luna"

# Streaming vs non-streaming is chosen here, at the SDK call site (see `ask`). Toggle it with the
# REACT_AGENT_STREAM env var in the WORKER's environment (the workflow runs on the worker). Default
# is streaming — today's behavior. Read once at import, so it's fixed per worker process, not per
# turn; keep it consistent across worker restarts for a given session (it isn't recorded in
# workflow history — see README).
STREAM_RESPONSES = os.environ.get("REACT_AGENT_STREAM", "1").lower() not in {
    "0",
    "false",
    "no",
}

SYSTEM_INSTRUCTION = """\
You are a helpful location, weather and formula 1 data assistant. Answer the user in brief, natural prose.

Use your tools to answer accurately rather than guessing. You can find the weather two ways:
 - For a named city: look up its coordinates with `get_coordinates`, then call `get_weather`.
 - For the user's current location: call `get_ip_address`, then `get_location_info` (to get
   coordinates from the IP), then `get_weather`.
Chain tools as needed — you will usually need more than one — and once you have enough
information, reply in a sentence or two. `get_weather` returns the temperature in Fahrenheit, a
weather code, and wind speed; summarize it in plain language.

Formula 1 data is available through the MCP server. Use the tools available through that MCP server
to assist the user with their formula 1 related questions. When the user asks about races relative
to a date, specifically look at the race dates in the data returned from the MCP server. If the user
asks for a date using relative terms, such as "next week", "last week", "this month", "last month",
"the next race", "the last race", etc., you should use today's date and the race dates in the data 
returned from the MCP server to determine the correct date to use. Look at the
dates for all events before answering date-relative questions.

Use the tools you have available, without worrying about whether you have the permission to use them.
The harness approval policy will handle the permissions for you.

If a request is ambiguous or needs information only the user can give — which city they mean, for 
example — call `ask_user` with a clear question and use their
answer to continue. Prefer asking over guessing when it matters. If the user's answer is not clear
call `ask_user` again.

Never use the ask_user tool to ask for permissions to execute a tool - that is handled by the 
harness approval policy."""


@agent.defn(name="ReactAgent")
class ReactAgentWorkflow:
    """A ReAct agent (weather/geo/IP tools + ask_user) driven by the OpenAI Agents SDK."""

    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=WorkflowStream(),
            # No human-in-the-loop yet — don't gate tool calls (demo4-hitl will tighten this).
            # A caller can still override per session via AgentConfig.approval_policy.
            approval_policy_default=ToolApprovalPolicy.allow_inherently_safe(),
        )
        # OpenAI conversation state, threaded across turns as the SDK's input-item list.
        self._conversation: list[TResponseInputItem] = []

    @agent.accepts
    async def ask(self, message: TextMessage) -> TextReply:
        """Chat with the assistant. Ask about the weather in a city, or where you are, and it
        chains its tools (coordinates / IP-location / weather) and tells you what it found."""
        sdk_agent = OpenAIAgent(
            name="ReactAgent",
            instructions=SYSTEM_INSTRUCTION,
            model=DEFAULT_MODEL,
            # ALL_TOOLS are durable activity tools; HUMAN_TOOLS is the ask_user callback tool.
            # Both mix freely through the one adapter — a callback tool carries __agent_tool__ and
            # funnels into run_tool exactly like an activity tool.
            tools=as_openai_agent_tools(self._runner, [*ALL_TOOLS, *HUMAN_TOOLS]),
        )
        input_items: list[TResponseInputItem] = [
            *self._conversation,
            {"role": "user", "content": message.text},
        ]

        if STREAM_RESPONSES:
            # run_streamed returns immediately; iterate its events to drive the turn to completion.
            # context=self._runner hands the harness runner to the streaming seam
            # (stream_to_provider reads the in-flight turn off it); required only on this path.
            result = Runner.run_streamed(sdk_agent, input=input_items, context=self._runner)
            async for _event in result.stream_events():
                pass
        else:
            # Non-streaming: run the whole turn to completion, then return one reply. No context
            # and no stream seam. Tool cards, ask_user, and the final reply still appear on the turn
            # stream; token-by-token reply_delta and model_interaction_* do not.
            result = await Runner.run(sdk_agent, input=input_items)

        self._conversation = result.to_input_list()
        return TextReply(text=str(result.final_output))
