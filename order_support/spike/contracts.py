# ABOUTME: The Strands shipping child's message contract, as a stub the OpenAI parent reflects on.
# Lets the parent build subagent tools without importing Strands; a test fails if it drifts.

"""Stub contract for the spike's shipping child.

``subagent_toolset`` reads a class's ``@agent.accepts`` handlers by pure reflection, so this
stub, with the same handler signatures and docstrings as ``SpikeShippingAgentWorkflow``, is all
the parent needs. ``workflow_type`` and ``task_queue`` are passed explicitly because the stub has
no ``@workflow.defn``. ``tests/test_spike_contract.py`` keeps the stub and the real child in step.
"""

from temporal_agent_harness.harness import agent
from temporal_agent_harness.harness.agent_protocol import TextMessage, TextReply

SHIPPING_WORKFLOW_TYPE = "SpikeShippingAgent"
SHIPPING_TASK_QUEUE = "spike-shipping"


class ShippingContract:
    """The shipping specialist's handlers, as the parent sees them."""

    @agent.accepts
    async def ask(self, message: TextMessage) -> TextReply:
        """Ask the shipping specialist about an order's shipment: where it is, when it will
        arrive, or to expedite it."""
        raise NotImplementedError("a contract stub; the real handler runs in the child workflow")
