# ABOUTME: The Orders & Refunds specialist's tools, as harness activity tools over the mock store.
# The lookups are inherently safe; issue_refund is gated.

# No `from __future__ import annotations`: the harness reads these annotations concretely.

from datetime import date, timedelta
from decimal import ROUND_HALF_UP, Decimal

from temporal_agent_harness.harness import agent
from temporalio.workflow import ActivityConfig

import store

_TOOL_CONFIG = ActivityConfig(start_to_close_timeout=timedelta(seconds=30))
_CENT = Decimal("0.01")


def order_text(order_id: str, today: date) -> str:
    try:
        o = store.order(order_id)
    except store.UnknownOrderError as e:
        return f"Not found: {e}."
    customer = store.customer(o.customer_email)
    placed = today - timedelta(days=o.placed_days_ago)
    shipment = store.shipment(order_id, today)
    return (
        f"Order {order_id} for {customer.name} ({customer.email}), placed {store.day_label(placed)}: "
        f"{', '.join(o.items)}. Total ${o.total}. Shipment: {shipment.status}.{o.reissue_note}"
    )


def customer_orders_text(email: str, today: date) -> str:
    try:
        customer = store.customer(email)
    except store.UnknownCustomerError as e:
        return f"Not found: {e}."
    lines = [
        f"- {o.order_id}: {', '.join(o.items)}, ${o.total}, {store.shipment(o.order_id, today).status}"
        for o in store.customer_orders(email)
    ]
    return f"{customer.name}'s orders, newest first:\n" + "\n".join(lines)


def eligibility_text(order_id: str, today: date) -> str:
    try:
        result = store.return_eligibility(order_id, today)
    except store.UnknownOrderError as e:
        return f"Not found: {e}."
    verdict = "can be returned or refunded" if result.eligible else "can't be returned or refunded"
    return f"Order {order_id} {verdict}: {result.reason}."


def refund_text(order_id: str, amount: float, reason: str, today: date) -> str:
    cents = Decimal(str(amount)).quantize(_CENT, rounding=ROUND_HALF_UP)
    try:
        confirmation = store.refund(order_id, cents, today)
    except (store.UnknownOrderError, store.RefundRefused) as e:
        return f"Not refunded: {e}."
    return f"Refunded ${cents} for order {order_id} ({reason}); confirmation {confirmation}."


@agent.activity_tool_defn(activity_config=_TOOL_CONFIG, inherently_safe=True)
async def lookup_order(order_id: str) -> str:
    """Get an order's items, total, customer, order date and shipment status.

    Args:
        order_id: The order number, e.g. "1042".
    """
    return order_text(order_id, date.today())


@agent.activity_tool_defn(activity_config=_TOOL_CONFIG, inherently_safe=True)
async def list_customer_orders(email: str) -> str:
    """List a customer's orders, newest first.

    Args:
        email: The customer's email address.
    """
    return customer_orders_text(email, date.today())


@agent.activity_tool_defn(activity_config=_TOOL_CONFIG, inherently_safe=True)
async def check_return_eligibility(order_id: str) -> str:
    """Check whether an order can be returned or refunded today, and why.

    Args:
        order_id: The order number, e.g. "1042".
    """
    return eligibility_text(order_id, date.today())


@agent.activity_tool_defn(activity_config=_TOOL_CONFIG)
async def issue_refund(order_id: str, amount: float, reason: str) -> str:
    """Refund money for an order to the customer's original payment method.

    Args:
        order_id: The order number, e.g. "1042".
        amount: The refund in US dollars, at most the order total.
        reason: Why the customer is being refunded, in a few words.
    """
    return refund_text(order_id, amount, reason, date.today())


ORDERS_TOOLS = [lookup_order, list_customer_orders, check_return_eligibility, issue_refund]
