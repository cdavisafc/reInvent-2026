# ABOUTME: The Strands machinery both specialists share: a SubagentSignalRunner in auto mode, judging
# gated calls from the conversation history, a harness Strands agent on Bedrock, and one ask turn.

"""A Strands specialist, built inside a workflow's ``@agent.init``.

The runner registers itself on the enclosing workflow instance, so building it here rather than in
the workflow class is the same to the harness. Every specialist uses
:class:`~subagent_signals.SubagentSignalRunner`, so it signals its reply to a parent concierge
instead of holding the concierge's Worker (see ``subagent_signals.py``).

Every specialist runs in auto mode. Lookups and ``ask_user`` are inherently safe and run without
being judged. Each gated call (a refund, an expedite) is judged against this agent's conversation
history, the Strands agent's messages, by the order judge (an activity, ``order_judge.py``) and
the rules in ``order_policy.py``.
"""

from collections.abc import Callable
from datetime import timedelta
from typing import Any

from temporal_agent_harness.ai_sdks.strands_agents_harness import (
    as_strands_tools,
    harness_strands_agent,
)
from temporal_agent_harness.harness.agent_protocol import (
    AgentConfig,
    TextMessage,
    TextReply,
    ToolApprovalPolicy,
)
from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.contrib.workflow_streams import WorkflowStream

from human_tools import HUMAN_TOOLS
from order_policy import (
    JUDGE_ACTIVITY,
    ORDER_RULES_CRITERIA,
    JudgeRequest,
    OrderJudgment,
    history_evaluator,
)
from subagent_signals import SubagentSignalRunner

# The name each specialist Worker's StrandsPlugin(models=...) registers the Bedrock model under.
MODEL = "bedrock"
# The judge sits on the critical path of every gated call: keep it short, and after a few failed
# attempts let the harness escalate the call to a person.
JUDGE_TIMEOUT = timedelta(seconds=60)
JUDGE_RETRY = RetryPolicy(maximum_attempts=3)


async def judge_in_activity(request: JudgeRequest) -> OrderJudgment:
    """Ask the order judge, as an activity. Plain dicts on the wire, as the activity expects."""
    answer = await workflow.execute_activity(
        JUDGE_ACTIVITY,
        request.model_dump(mode="json"),
        start_to_close_timeout=JUDGE_TIMEOUT,
        retry_policy=JUDGE_RETRY,
        result_type=dict,
        summary=f"judge {request.tool_name}({request.tool_input.get('order_id')})",
    )
    return OrderJudgment.model_validate(answer)


class SpecialistAgent:
    """One Strands agent and its runner; the conversation lives as long as the workflow."""

    def __init__(
        self,
        config: AgentConfig,
        *,
        system_prompt: str,
        tools: list[Callable[..., Any]],
        model_timeout: timedelta,
    ) -> None:
        self._runner = SubagentSignalRunner(
            config,
            stream=WorkflowStream(),
            approval_policy_default=ToolApprovalPolicy.auto_mode(pre_approve_inherently_safe=True),
            auto_approval_criteria_default=ORDER_RULES_CRITERIA,
            # Reads the conversation as it is when each call is judged.
            auto_mode_evaluator=history_evaluator(lambda: self._agent.messages, judge_in_activity),
        )
        self._agent = harness_strands_agent(
            self._runner,
            model=MODEL,
            start_to_close_timeout=model_timeout,
            system_prompt=system_prompt,
            tools=as_strands_tools(self._runner, [*tools, *HUMAN_TOOLS]),
            # Replies reach clients through the turn stream, not Strands' stdout printer.
            callback_handler=None,
        )

    async def ask(self, message: TextMessage) -> TextReply:
        try:
            result = await self._agent.invoke_async(message.text)
        except Exception:
            # The harness reports a handler's failure to the UI as str(e) only; log the
            # traceback so the Worker's logs show where it came from.
            workflow.logger.exception("specialist turn failed")
            raise
        return TextReply(text=str(result))
