# ABOUTME: Tests for the mock Acme Outfitters store: order and shipment lookups, return eligibility,
# and the refund and expedite rules the specialists' tools rely on.

from datetime import date, timedelta
from decimal import Decimal

import pytest

import store
from store import ShipmentStatus

# A Wednesday. The store's dates are relative to "today", so any day works.
TODAY = date(2026, 12, 2)


def test_order_lookup_returns_the_order():
    order = store.order("1042")
    assert order.order_id == "1042"
    assert order.total == Decimal("240.00")


def test_unknown_order_raises():
    with pytest.raises(store.UnknownOrderError, match="9999"):
        store.order("9999")


def test_customer_orders_by_email_are_newest_first():
    orders = store.customer_orders("maya.chen@example.com")
    assert [o.order_id for o in orders] == ["1042", "1017"]


def test_customer_orders_for_unknown_email_raises():
    with pytest.raises(store.UnknownCustomerError):
        store.customer_orders("nobody@example.com")


def test_order_totals_fall_on_both_sides_of_the_refund_limit():
    # The auto-mode demo needs a small refund (auto-approved) and a large one (a person decides).
    assert store.order("1017").total <= Decimal(100)
    assert store.order("1042").total > Decimal(100)


def test_the_store_covers_every_shipment_status():
    statuses = {store.shipment(o.order_id, TODAY).status for o in store.all_orders()}
    assert statuses == set(ShipmentStatus)


def test_headline_order_is_late_and_misses_the_coming_friday_on_any_day():
    # "Order 1042 is late. If it won't arrive by Friday, refund me."
    for offset in range(7):
        today = TODAY + timedelta(days=offset)
        shipment = store.shipment("1042", today)
        days_to_friday = (4 - today.weekday()) % 7 or 7
        assert shipment.is_late
        assert shipment.estimated_delivery > today + timedelta(days=days_to_friday)


def test_on_time_shipment_is_not_late():
    assert not store.shipment("1021", TODAY).is_late


def test_delivered_order_has_a_delivery_date_and_no_estimate():
    shipment = store.shipment("1017", TODAY)
    assert shipment.status is ShipmentStatus.DELIVERED
    assert shipment.delivered_on is not None and shipment.delivered_on < TODAY
    assert not shipment.is_late


@pytest.mark.parametrize(
    ("order_id", "eligible", "reason_fragment"),
    [
        ("1017", True, "return window"),  # delivered 10 days ago
        ("1003", False, "closed"),  # delivered 45 days ago
        ("1042", True, "late"),
        ("1028", True, "lost"),
        ("1021", False, "on schedule"),  # in transit, on time
        ("1033", False, "on schedule"),  # still processing
    ],
)
def test_return_eligibility(order_id, eligible, reason_fragment):
    result = store.return_eligibility(order_id, TODAY)
    assert result.eligible is eligible
    assert reason_fragment in result.reason


def test_refund_of_an_eligible_order_returns_a_confirmation():
    assert store.refund("1017", Decimal("45.00"), TODAY) == "RF-1017"


def test_refund_more_than_the_order_total_is_refused():
    with pytest.raises(store.RefundRefused, match="45.00"):
        store.refund("1017", Decimal("45.01"), TODAY)


@pytest.mark.parametrize("amount", [Decimal(0), Decimal(-5)])
def test_refund_must_be_positive(amount):
    with pytest.raises(store.RefundRefused):
        store.refund("1017", amount, TODAY)


def test_refund_of_an_ineligible_order_is_refused_with_the_reason():
    with pytest.raises(store.RefundRefused, match="closed"):
        store.refund("1003", Decimal(10), TODAY)


@pytest.mark.parametrize("order_id", ["1042", "1033"])  # in transit, processing
def test_expedite_an_undelivered_order(order_id):
    assert store.expedite(order_id, TODAY) == f"EXP-{order_id}"


@pytest.mark.parametrize("order_id", ["1017", "1028"])  # delivered, lost
def test_expedite_refused_once_the_order_is_delivered_or_lost(order_id):
    with pytest.raises(store.ExpediteRefused):
        store.expedite(order_id, TODAY)


def test_lost_order_was_reissued_and_the_link_runs_both_ways():
    assert store.order("1028").reissued_as == "1077"
    assert store.order("1077").replaces == "1028"
    assert store.order("1042").reissued_as is None and store.order("1042").replaces is None


def test_the_reissue_is_late_so_it_can_be_expedited_and_refunded():
    assert store.shipment("1077", TODAY).is_late
    assert store.return_eligibility("1077", TODAY).eligible
    assert store.expedite("1077", TODAY) == "EXP-1077"
