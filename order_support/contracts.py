# ABOUTME: The specialists' message contracts, as stubs the concierge reflects on to build subagent tools.
# Lets the concierge call the Strands specialists without importing Strands; a test fails if they drift.

"""Stub contracts for the two specialists.

``subagent_toolset`` reads a class's ``@agent.accepts`` handlers by pure reflection, so a stub with
the same handler signatures and docstrings as the real workflow is all the concierge needs.
``workflow_type`` and ``task_queue`` are passed explicitly because the stubs have no
``@workflow.defn``. ``tests/test_contracts.py`` keeps each stub and its real workflow in step.
"""

from temporal_agent_harness.harness import agent
from temporal_agent_harness.harness.agent_protocol import TextMessage, TextReply

SHIPPING_WORKFLOW_TYPE = "ShippingAgent"
SHIPPING_TASK_QUEUE = "order-support-shipping"

ORDERS_WORKFLOW_TYPE = "OrdersAgent"
ORDERS_TASK_QUEUE = "order-support-orders"

_STUB = "a contract stub; the real handler runs in the child workflow"


class ShippingContract:
    """The shipping specialist's handlers, as the concierge sees them."""

    @agent.accepts
    async def ask(self, message: TextMessage) -> TextReply:
        """Ask the shipping specialist about an order's shipment: where it is, when it will
        arrive, or to expedite it."""
        raise NotImplementedError(_STUB)


class OrdersContract:
    """The orders & refunds specialist's handlers, as the concierge sees them."""

    @agent.accepts
    async def ask(self, message: TextMessage) -> TextReply:
        """Ask the orders & refunds specialist about an order or a customer's orders: what was
        bought, whether it can be returned, or to refund it."""
        raise NotImplementedError(_STUB)
