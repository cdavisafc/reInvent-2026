# ABOUTME: The Shipping specialist's tools, as harness activity tools over the mock store.
# track_shipment and get_delivery_estimate are inherently safe; expedite_shipment is gated.

# No `from __future__ import annotations`: the harness reads these annotations concretely.

import os
import random
from collections.abc import Callable
from datetime import date, timedelta

from temporal_agent_harness.harness import agent
from temporalio.workflow import ActivityConfig

import store
from store import ShipmentStatus, day_label

_TOOL_CONFIG = ActivityConfig(start_to_close_timeout=timedelta(seconds=30))


class CarrierUnavailable(RuntimeError):
    """The (simulated) carrier API failed; Temporal retries the activity."""


def raise_if_carrier_flakes(rng: Callable[[], float] = random.random) -> None:
    """Fail at the rate CARRIER_FLAKE_RATE sets (0 to 1, default 0), to show retries on stage."""
    rate = float(os.environ.get("CARRIER_FLAKE_RATE", "0"))
    if rng() < rate:
        raise CarrierUnavailable("the carrier's tracking API timed out")


def shipment_status_text(order_id: str, today: date) -> str:
    try:
        s = store.shipment(order_id, today)
    except store.UnknownOrderError as e:
        return f"Can't track it: {e}."
    heading = (
        f"Order {order_id} ({s.carrier} {s.tracking_number}): {s.status}, {s.location}."
        f"{store.order(order_id).reissue_note}"
    )
    if s.status is ShipmentStatus.DELIVERED:
        assert s.delivered_on is not None
        return f"{heading} Delivered {day_label(s.delivered_on)}."
    if s.status is ShipmentStatus.LOST:
        return f"{heading} The carrier has marked it lost; it was promised by {day_label(s.promised_by)}."
    assert s.estimated_delivery is not None
    timing = "late" if s.is_late else "on schedule"
    return (
        f"{heading} Promised by {day_label(s.promised_by)}, now expected "
        f"{day_label(s.estimated_delivery)}: {timing}."
    )


def delivery_estimate_text(order_id: str, today: date) -> str:
    try:
        s = store.shipment(order_id, today)
    except store.UnknownOrderError as e:
        return f"No estimate: {e}."
    if s.status is ShipmentStatus.DELIVERED:
        assert s.delivered_on is not None
        return f"Order {order_id} was already delivered, {day_label(s.delivered_on)}."
    if s.status is ShipmentStatus.LOST:
        return f"Order {order_id} has no delivery estimate: the carrier has marked it lost."
    assert s.estimated_delivery is not None
    return (
        f"Order {order_id} is expected {day_label(s.estimated_delivery)} "
        f"(promised by {day_label(s.promised_by)}). Today is {day_label(today)}."
    )


def expedite_text(order_id: str, today: date) -> str:
    try:
        confirmation = store.expedite(order_id, today)
    except (store.UnknownOrderError, store.ExpediteRefused) as e:
        return f"Not expedited: {e}."
    return f"Order {order_id} upgraded to express shipping; confirmation {confirmation}."


@agent.activity_tool_defn(activity_config=_TOOL_CONFIG, inherently_safe=True)
async def track_shipment(order_id: str) -> str:
    """Get the current status and location of an order's shipment, and whether it is late.

    Args:
        order_id: The order number, e.g. "1042".
    """
    raise_if_carrier_flakes()
    return shipment_status_text(order_id, date.today())


@agent.activity_tool_defn(activity_config=_TOOL_CONFIG, inherently_safe=True)
async def get_delivery_estimate(order_id: str) -> str:
    """Get the date an order is now expected to arrive, the date we promised, and today's date.

    Args:
        order_id: The order number, e.g. "1042".
    """
    return delivery_estimate_text(order_id, date.today())


@agent.activity_tool_defn(activity_config=_TOOL_CONFIG)
async def expedite_shipment(order_id: str) -> str:
    """Upgrade an order's shipment to express delivery.

    Args:
        order_id: The order number, e.g. "1042".
    """
    return expedite_text(order_id, date.today())


SHIPPING_TOOLS = [track_shipment, get_delivery_estimate, expedite_shipment]
