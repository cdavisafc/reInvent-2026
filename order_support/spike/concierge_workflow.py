# ABOUTME: The spike's OpenAI Agents SDK parent: drives the Strands shipping child as a subagent.
# Builds start/ask/stop tools from the stub contract, so it never imports Strands.

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
    from spike.contracts import (
        SHIPPING_TASK_QUEUE,
        SHIPPING_WORKFLOW_TYPE,
        ShippingContract,
    )
    from spike.keyword_tools import keyword_only

CONCIERGE_TASK_QUEUE = "spike-concierge"
DEFAULT_MODEL = "gpt-5.6-luna"

SHIPPING_SUBAGENT_TOOLS = keyword_only(
    agent.subagent_toolset(
        ShippingContract,
        key="shipping",
        task_queue=SHIPPING_TASK_QUEUE,
        workflow_type=SHIPPING_WORKFLOW_TYPE,
    )
)
CONCIERGE_TOOLS = [*SHIPPING_SUBAGENT_TOOLS, *HUMAN_TOOLS]

# Starting, messaging and stopping the specialist run without a person's approval. The
# specialist's own gated tools still escalate to a person, against the child's workflow.
APPROVAL_POLICY = ToolApprovalPolicy.allow_inherently_safe().with_changes(
    auto_approve_tools=[tool.__name__ for tool in SHIPPING_SUBAGENT_TOOLS]
)

SYSTEM_INSTRUCTION = """\
You are the order-support concierge for Acme Outfitters. Answer the customer briefly, in plain prose.

You don't look anything up yourself. For anything about an order's shipment (where it is, when
it will arrive, expediting it), start the shipping specialist with `start_shipping`, ask it with
`shipping_ask`, and pass its answer on. Reuse the same specialist for follow-up questions in this
conversation. Call `ask_user` only when you need information only the customer has."""


@agent.defn(name="SpikeConciergeAgent")
class SpikeConciergeAgentWorkflow:
    """An OpenAI Agents SDK concierge that delegates shipping questions to a Strands subagent."""

    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = AgentWorkflowRunner(
            config, stream=WorkflowStream(), approval_policy_default=APPROVAL_POLICY
        )
        self._conversation: list[TResponseInputItem] = []

    @agent.accepts
    async def ask(self, message: TextMessage) -> TextReply:
        """Ask the concierge about an order. It hands shipping questions to the shipping
        specialist and tells you what it found."""
        sdk_agent = OpenAIAgent(
            name="SpikeConcierge",
            instructions=SYSTEM_INSTRUCTION,
            model=DEFAULT_MODEL,
            tools=as_openai_agent_tools(self._runner, CONCIERGE_TOOLS),
        )
        input_items: list[TResponseInputItem] = [
            *self._conversation,
            {"role": "user", "content": message.text},
        ]
        # Streamed, so the UI shows tokens as they arrive. context=self._runner lets the
        # streaming seam find the in-flight turn.
        result = Runner.run_streamed(sdk_agent, input=input_items, context=self._runner)
        async for _event in result.stream_events():
            pass
        self._conversation = result.to_input_list()
        return TextReply(text=str(result.final_output))
