# ABOUTME: The spike shipping child's tools, as harness activity tools with canned results.
# track_shipment is inherently safe; expedite_shipment needs a person's approval.

# No `from __future__ import annotations`: the harness reads these annotations concretely.

from datetime import timedelta

from temporal_agent_harness.harness import agent
from temporalio.workflow import ActivityConfig

_TOOL_CONFIG = ActivityConfig(start_to_close_timeout=timedelta(seconds=30))


@agent.activity_tool_defn(activity_config=_TOOL_CONFIG, inherently_safe=True)
async def track_shipment(order_id: str) -> str:
    """Get the current status and location of an order's shipment.

    Args:
        order_id: The order number, e.g. "1042".
    """
    return f"Order {order_id}: in transit, delayed at the Reno hub; estimated delivery Monday."


@agent.activity_tool_defn(activity_config=_TOOL_CONFIG)
async def expedite_shipment(order_id: str) -> str:
    """Upgrade an order's shipment to express delivery.

    Args:
        order_id: The order number, e.g. "1042".
    """
    return f"Order {order_id} expedited; confirmation EXP-{order_id}."


SHIPPING_TOOLS = [track_shipment, expedite_shipment]
