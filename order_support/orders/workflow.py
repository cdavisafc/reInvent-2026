# ABOUTME: The Orders & Refunds specialist (Strands, Claude on Bedrock): looks up orders, checks
# return eligibility and issues refunds. A harness Strands agent built once in @agent.init.

from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporal_agent_harness.harness import agent
    from temporal_agent_harness.harness.agent_protocol import (
        AgentConfig,
        TextMessage,
        TextReply,
    )

    from contracts import ORDERS_WORKFLOW_TYPE
    from orders.tools import ORDERS_TOOLS
    from specialist import SpecialistAgent

SYSTEM_PROMPT = """\
You are the orders and refunds specialist for Acme Outfitters. Answer briefly, in plain prose.

Use `lookup_order` for what an order contains and what it cost, `list_customer_orders` to find a
customer's orders by email, and `check_return_eligibility` before any refund. Refund with
`issue_refund` only when asked to and only an eligible order; refund the full order total unless
told otherwise. Call your tools without asking for permission: the harness approval policy decides
which calls need a person's approval. Use `ask_user` only when you need information only the user
has, such as which order they mean."""


@agent.defn(name=ORDERS_WORKFLOW_TYPE)
class OrdersAgentWorkflow:
    """A Strands orders & refunds specialist, driven as a subagent or directly."""

    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._specialist = SpecialistAgent(
            config,
            system_prompt=SYSTEM_PROMPT,
            tools=ORDERS_TOOLS,
            model_timeout=timedelta(minutes=2),
        )

    @agent.accepts
    async def ask(self, message: TextMessage) -> TextReply:
        """Ask the orders & refunds specialist about an order or a customer's orders: what was
        bought, whether it can be returned, or to refund it."""
        return await self._specialist.ask(message)
