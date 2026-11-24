# ABOUTME: The mock Acme Outfitters store: customers, orders and shipments, plus the return, refund
# and expedite rules. Read-only and deterministic; every date is relative to the caller's "today".

"""Mock data for the order-support demo.

Dates are stored as day offsets from "today" and resolved against the ``today`` each query is
given, so a late order stays late whatever day the demo runs. Nothing is ever mutated: a refund or
an expedite returns a deterministic confirmation id, since each AgentCore microVM has its own
memory.
"""

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from enum import StrEnum

RETURN_WINDOW = timedelta(days=30)


class UnknownOrderError(LookupError):
    """No order has this id."""


class UnknownCustomerError(LookupError):
    """No customer has this email."""


class RefundRefused(ValueError):
    """The store's rules don't allow this refund."""


class ExpediteRefused(ValueError):
    """The store's rules don't allow expediting this order."""


class ShipmentStatus(StrEnum):
    PROCESSING = "processing"
    IN_TRANSIT = "in transit"
    DELIVERED = "delivered"
    LOST = "lost"


@dataclass(frozen=True)
class Customer:
    email: str
    name: str


@dataclass(frozen=True)
class Order:
    order_id: str
    customer_email: str
    items: tuple[str, ...]
    total: Decimal
    placed_days_ago: int
    # A lost order is reissued under a new number; each side names the other.
    reissued_as: str | None = None
    replaces: str | None = None

    @property
    def reissue_note(self) -> str:
        """The reissue link, as the tools tell the model: empty for most orders."""
        if self.reissued_as:
            return f" This order was reissued as order {self.reissued_as}."
        if self.replaces:
            return f" This order is a replacement for order {self.replaces}."
        return ""


@dataclass(frozen=True)
class Shipment:
    order_id: str
    carrier: str
    tracking_number: str
    status: ShipmentStatus
    location: str
    promised_by: date
    estimated_delivery: date | None
    delivered_on: date | None

    @property
    def is_late(self) -> bool:
        """Still on its way, and expected after the date we promised."""
        return (
            self.status in (ShipmentStatus.PROCESSING, ShipmentStatus.IN_TRANSIT)
            and self.estimated_delivery is not None
            and self.estimated_delivery > self.promised_by
        )


@dataclass(frozen=True)
class ReturnEligibility:
    eligible: bool
    reason: str


@dataclass(frozen=True)
class _ShipmentRecord:
    """A shipment with its dates as day offsets from today (negative is in the past)."""

    carrier: str
    tracking_number: str
    status: ShipmentStatus
    location: str
    promised_in: int
    estimated_in: int | None = None
    delivered_in: int | None = None


_CUSTOMERS = {
    c.email: c
    for c in [
        Customer("maya.chen@example.com", "Maya Chen"),
        Customer("luis.ortega@example.com", "Luis Ortega"),
        Customer("priya.natarajan@example.com", "Priya Natarajan"),
        Customer("sam.okafor@example.com", "Sam Okafor"),
        Customer("grace.liu@example.com", "Grace Liu"),
        Customer("tom.becker@example.com", "Tom Becker"),
    ]
}


def _order(order_id, email, items, total, placed_days_ago, **reissue) -> Order:
    return Order(order_id, email, tuple(items), Decimal(total), placed_days_ago, **reissue)


_ORDERS = {
    o.order_id: o
    for o in [
        _order("1003", "luis.ortega@example.com", ["Ridgeline hiking boots"], "129.00", 52),
        _order("1017", "maya.chen@example.com", ["Trail running socks (3-pack)"], "45.00", 14),
        _order("1021", "luis.ortega@example.com", ["Stormshell rain jacket"], "89.00", 3),
        _order("1028", "priya.natarajan@example.com", ["Summit 55L backpack"], "310.00", 11, reissued_as="1077"),
        _order("1033", "priya.natarajan@example.com", ["Pocket water filter"], "62.00", 1),
        _order("1036", "sam.okafor@example.com", ["Headlamp", "Spare batteries"], "95.00", 8),
        _order("1042", "maya.chen@example.com", ["Alpine 2-person tent"], "240.00", 9),
        _order("1048", "sam.okafor@example.com", ["Down sleeping bag"], "150.00", 7),
        _order("1051", "grace.liu@example.com", ["Sun hat"], "35.00", 6),
        _order("1055", "grace.liu@example.com", ["Touring kayak paddle"], "480.00", 2),
        _order("1060", "tom.becker@example.com", ["Locking carabiners (4)"], "18.00", 0),
        _order("1064", "tom.becker@example.com", ["Carbon trekking poles"], "205.00", 25),
        _order("1077", "priya.natarajan@example.com", ["Summit 55L backpack"], "310.00", 4, replaces="1028"),
    ]
}

_SHIPMENTS = {
    "1003": _ShipmentRecord("UPS", "1Z999AA10001", ShipmentStatus.DELIVERED, "Delivered, front porch", -47, delivered_in=-45),
    "1017": _ShipmentRecord("USPS", "9400100000000001017", ShipmentStatus.DELIVERED, "Delivered, mailbox", -9, delivered_in=-10),
    "1021": _ShipmentRecord("FedEx", "7700001021", ShipmentStatus.IN_TRANSIT, "Sacramento, CA facility", 3, estimated_in=2),
    "1028": _ShipmentRecord("UPS", "1Z999AA10028", ShipmentStatus.LOST, "Last scanned in Denver, CO 6 days ago", -2),
    "1033": _ShipmentRecord("FedEx", "7700001033", ShipmentStatus.PROCESSING, "Packing at the Ogden, UT warehouse", 6, estimated_in=6),
    "1036": _ShipmentRecord("USPS", "9400100000000001036", ShipmentStatus.DELIVERED, "Delivered, front desk", -2, delivered_in=-3),
    "1042": _ShipmentRecord("UPS", "1Z999AA10042", ShipmentStatus.IN_TRANSIT, "Held at the Reno, NV hub for weather", -1, estimated_in=8),
    "1048": _ShipmentRecord("FedEx", "7700001048", ShipmentStatus.IN_TRANSIT, "Out of Portland, OR facility", -2, estimated_in=1),
    "1051": _ShipmentRecord("USPS", "9400100000000001051", ShipmentStatus.DELIVERED, "Delivered, mailbox", -1, delivered_in=-2),
    "1055": _ShipmentRecord("UPS", "1Z999AA10055", ShipmentStatus.IN_TRANSIT, "Boise, ID hub", 5, estimated_in=4),
    "1060": _ShipmentRecord("USPS", "9400100000000001060", ShipmentStatus.PROCESSING, "Order received", 7, estimated_in=7),
    "1077": _ShipmentRecord("UPS", "1Z999AA10077", ShipmentStatus.IN_TRANSIT, "Salt Lake City, UT hub", -1, estimated_in=4),
    "1064": _ShipmentRecord("FedEx", "7700001064", ShipmentStatus.DELIVERED, "Delivered, garage", -18, delivered_in=-20),
}


def all_orders() -> list[Order]:
    return list(_ORDERS.values())


def order(order_id: str) -> Order:
    if order_id not in _ORDERS:
        raise UnknownOrderError(f"there is no order {order_id}")
    return _ORDERS[order_id]


def customer_orders(email: str) -> list[Order]:
    """The customer's orders, newest first."""
    customer(email)  # raises for an unknown email
    orders = [o for o in _ORDERS.values() if o.customer_email == email]
    return sorted(orders, key=lambda o: o.placed_days_ago)


def customer(email: str) -> Customer:
    if email not in _CUSTOMERS:
        raise UnknownCustomerError(f"there is no customer with email {email}")
    return _CUSTOMERS[email]


def shipment(order_id: str, today: date) -> Shipment:
    order(order_id)  # raises for an unknown order
    record = _SHIPMENTS[order_id]

    def day(offset: int | None) -> date | None:
        return None if offset is None else today + timedelta(days=offset)

    return Shipment(
        order_id=order_id,
        carrier=record.carrier,
        tracking_number=record.tracking_number,
        status=record.status,
        location=record.location,
        promised_by=today + timedelta(days=record.promised_in),
        estimated_delivery=day(record.estimated_in),
        delivered_on=day(record.delivered_in),
    )


def return_eligibility(order_id: str, today: date) -> ReturnEligibility:
    """Whether the order can be returned or refunded today, and why."""
    s = shipment(order_id, today)
    if s.status is ShipmentStatus.DELIVERED:
        assert s.delivered_on is not None
        closes = s.delivered_on + RETURN_WINDOW
        if today <= closes:
            return ReturnEligibility(True, f"delivered {s.delivered_on}; the return window is open until {closes}")
        return ReturnEligibility(False, f"delivered {s.delivered_on}; the return window closed on {closes}")
    if s.status is ShipmentStatus.LOST:
        return ReturnEligibility(True, "the shipment was lost in transit")
    if s.is_late:
        return ReturnEligibility(
            True, f"the shipment is late: promised by {s.promised_by}, now expected {s.estimated_delivery}"
        )
    return ReturnEligibility(False, "the order hasn't been delivered yet and is on schedule")


def refund(order_id: str, amount: Decimal, today: date) -> str:
    """Refund ``amount`` of the order; returns a confirmation id."""
    total = order(order_id).total
    if amount <= 0:
        raise RefundRefused(f"a refund must be more than $0.00, not ${amount}")
    if amount > total:
        raise RefundRefused(f"${amount} is more than the order total of ${total}")
    eligibility = return_eligibility(order_id, today)
    if not eligibility.eligible:
        raise RefundRefused(f"order {order_id} can't be refunded: {eligibility.reason}")
    return f"RF-{order_id}"


def expedite(order_id: str, today: date) -> str:
    """Upgrade an undelivered order to express shipping; returns a confirmation id."""
    s = shipment(order_id, today)
    if s.status not in (ShipmentStatus.PROCESSING, ShipmentStatus.IN_TRANSIT):
        raise ExpediteRefused(f"order {order_id} can't be expedited: its shipment is {s.status}")
    return f"EXP-{order_id}"


def day_label(day: date) -> str:
    """A date as the tools show it to the model, with its weekday: "Thursday, 2026-12-10"."""
    return f"{day:%A}, {day.isoformat()}"
