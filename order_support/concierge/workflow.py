# ABOUTME: The Concierge (OpenAI Agents SDK): routes each request to the Shipping specialist, the Orders
# & Refunds specialist, or both, as harness subagents. Builds their tools from stub contracts, never importing Strands.

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

    from contracts import (
        ORDERS_TASK_QUEUE,
        ORDERS_WORKFLOW_TYPE,
        SHIPPING_TASK_QUEUE,
        SHIPPING_WORKFLOW_TYPE,
        OrdersContract,
        ShippingContract,
    )
    from human_tools import HUMAN_TOOLS
    from keyword_tools import keyword_only
    from subagent_signals import SubagentSignalRunner

CONCIERGE_WORKFLOW_TYPE = "ConciergeAgent"
CONCIERGE_TASK_QUEUE = "order-support-concierge"
DEFAULT_MODEL = "gpt-5.6-luna"

SUBAGENT_TOOLS = [
    *keyword_only(
        agent.subagent_toolset(
            ShippingContract,
            key="shipping",
            task_queue=SHIPPING_TASK_QUEUE,
            workflow_type=SHIPPING_WORKFLOW_TYPE,
        )
    ),
    *keyword_only(
        agent.subagent_toolset(
            OrdersContract,
            key="orders",
            task_queue=ORDERS_TASK_QUEUE,
            workflow_type=ORDERS_WORKFLOW_TYPE,
        )
    ),
]
CONCIERGE_TOOLS = [*SUBAGENT_TOOLS, *HUMAN_TOOLS]

# Starting, messaging and stopping the specialists run without a person's approval. Their own
# gated tools still escalate to a person, against the specialist's workflow.
APPROVAL_POLICY = ToolApprovalPolicy.allow_inherently_safe().with_changes(
    auto_approve_tools=[tool.__name__ for tool in SUBAGENT_TOOLS]
)

SYSTEM_INSTRUCTION = """\
You are the order-support concierge for Acme Outfitters. Answer the customer briefly, in plain prose.

You don't look anything up yourself; two specialists do:
- Shipping (`start_shipping`, then `shipping_ask`): where a shipment is, when it will arrive,
  expediting it.
- Orders & refunds (`start_orders`, then `orders_ask`): what an order contains and cost, a
  customer's orders, whether an order can be returned, refunding it.

Send a question about one of those to that specialist only. When a request needs both, ask them in
the order the request implies, and give the second what it needs from the first's answer. Always
name the order as "order <number>" in your message to a specialist. Start each specialist at most
once in this conversation and reuse it for follow-ups. A specialist acts only on the order the
conversation is about, or on one its records show is the same purchase (such as a reissue); if it
refuses another order, tell the customer to start a new conversation for it. Call `ask_user` only
when you need information only the customer has."""


@agent.defn(name=CONCIERGE_WORKFLOW_TYPE)
class ConciergeAgentWorkflow:
    """An OpenAI Agents SDK concierge that delegates to the Strands specialists as subagents."""

    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = SubagentSignalRunner(
            config, stream=WorkflowStream(), approval_policy_default=APPROVAL_POLICY
        )
        self._conversation: list[TResponseInputItem] = []

    @agent.accepts
    async def ask(self, message: TextMessage) -> TextReply:
        """Ask the concierge about an order. It hands the question to the shipping or orders
        specialist, or both, and tells you what they found."""
        sdk_agent = OpenAIAgent(
            name="Concierge",
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
