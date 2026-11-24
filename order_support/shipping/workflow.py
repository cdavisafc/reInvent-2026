# ABOUTME: The Shipping specialist (Strands, Claude on Bedrock): tracks shipments, gives delivery
# estimates and expedites orders. A harness Strands agent built once in @agent.init.

from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporal_agent_harness.harness import agent
    from temporal_agent_harness.harness.agent_protocol import (
        AgentConfig,
        TextMessage,
        TextReply,
    )

    from contracts import SHIPPING_WORKFLOW_TYPE
    from shipping.tools import SHIPPING_TOOLS
    from specialist import SpecialistAgent

SYSTEM_PROMPT = """\
You are the shipping specialist for Acme Outfitters. Answer briefly, in plain prose.

Use `track_shipment` for where an order is and whether it is late, and `get_delivery_estimate`
for when it will arrive. Use `expedite_shipment` only when asked to speed an order up. Call your
tools without asking for permission: the harness approval policy decides which calls need a
person's approval. Use `ask_user` only when you need information only the user has, such as which
order they mean."""


@agent.defn(name=SHIPPING_WORKFLOW_TYPE)
class ShippingAgentWorkflow:
    """A Strands shipping specialist, driven as a subagent or directly."""

    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._specialist = SpecialistAgent(
            config,
            system_prompt=SYSTEM_PROMPT,
            tools=SHIPPING_TOOLS,
            model_timeout=timedelta(minutes=2),
        )

    @agent.accepts
    async def ask(self, message: TextMessage) -> TextReply:
        """Ask the shipping specialist about an order's shipment: where it is, when it will
        arrive, or to expedite it."""
        return await self._specialist.ask(message)
