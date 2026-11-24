# ABOUTME: The spike's Strands child agent (Claude on Bedrock): tracks and expedites shipments.
# A harness Strands agent built once in @agent.init; each turn calls invoke_async.

from datetime import timedelta

from temporalio import workflow
from temporalio.contrib.workflow_streams import WorkflowStream

with workflow.unsafe.imports_passed_through():
    from temporal_agent_harness.ai_sdks.strands_agents_harness import (
        as_strands_tools,
        harness_strands_agent,
    )
    from temporal_agent_harness.harness import agent
    from temporal_agent_harness.harness.agent_protocol import (
        AgentConfig,
        TextMessage,
        TextReply,
        ToolApprovalPolicy,
    )

    from human_tools import HUMAN_TOOLS
    from spike.contracts import SHIPPING_WORKFLOW_TYPE
    from spike.shipping_tools import SHIPPING_TOOLS
    from subagent_signals import SubagentSignalRunner

# The name the worker's StrandsPlugin(models=...) registers the Bedrock model under.
MODEL = "bedrock"

SYSTEM_PROMPT = """\
You are the shipping specialist for Acme Outfitters. Answer briefly, in plain prose.

Use `track_shipment` for where an order is and when it will arrive. Use `expedite_shipment` only
when asked to speed an order up. Call your tools without asking for permission: the harness
approval policy decides which calls need a person's approval. Use `ask_user` only when you need
information only the user has, such as which order they mean."""


@agent.defn(name=SHIPPING_WORKFLOW_TYPE)
class SpikeShippingAgentWorkflow:
    """A Strands shipping specialist, driven as a subagent or directly."""

    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = SubagentSignalRunner(
            config,
            stream=WorkflowStream(),
            # track_shipment and ask_user run freely; expedite_shipment waits for a person.
            approval_policy_default=ToolApprovalPolicy.allow_inherently_safe(),
        )
        # Strands keeps the conversation in the agent, which lives as long as the workflow.
        self._agent = harness_strands_agent(
            self._runner,
            model=MODEL,
            start_to_close_timeout=timedelta(minutes=2),
            system_prompt=SYSTEM_PROMPT,
            tools=as_strands_tools(self._runner, [*SHIPPING_TOOLS, *HUMAN_TOOLS]),
            # Replies reach clients through the turn stream, not Strands' stdout printer.
            callback_handler=None,
        )

    @agent.accepts
    async def ask(self, message: TextMessage) -> TextReply:
        """Ask the shipping specialist about an order's shipment: where it is, when it will
        arrive, or to expedite it."""
        try:
            result = await self._agent.invoke_async(message.text)
        except Exception:
            # The harness reports a handler's failure to the UI as str(e) only; log the
            # traceback so the Worker's logs show where it came from.
            workflow.logger.exception("shipping turn failed")
            raise
        return TextReply(text=str(result))
