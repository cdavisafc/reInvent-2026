# ABOUTME: Tests for what the Orders & Refunds specialist's tools tell the model: order details, a
# customer's orders, return eligibility, and refund confirmations and refusals.

from datetime import date

import pytest

from orders.tools import (
    customer_orders_text,
    eligibility_text,
    order_text,
    refund_text,
)

TODAY = date(2026, 12, 2)


def test_order_text_gives_items_total_customer_and_status():
    text = order_text("1042", TODAY)
    for fact in ["Alpine 2-person tent", "$240.00", "Maya Chen", "in transit"]:
        assert fact in text


def test_customer_orders_lists_each_order_newest_first():
    text = customer_orders_text("maya.chen@example.com", TODAY)
    assert text.index("1042") < text.index("1017")
    assert "Maya Chen" in text


def test_customer_orders_for_unknown_email_is_reported_not_raised():
    assert "no customer" in customer_orders_text("nobody@example.com", TODAY)


@pytest.mark.parametrize(
    ("order_id", "verdict"), [("1017", "can be returned"), ("1003", "can't be returned")]
)
def test_eligibility_text(order_id, verdict):
    assert verdict in eligibility_text(order_id, TODAY)


def test_refund_confirms_the_amount_and_id():
    text = refund_text("1017", 45.0, "customer changed their mind", TODAY)
    assert "$45.00" in text
    assert "RF-1017" in text


def test_refund_rounds_the_amount_to_cents():
    assert "$44.99" in refund_text("1017", 44.989, "partial", TODAY)


def test_refund_refusal_is_reported_not_raised():
    text = refund_text("1017", 500.0, "too much", TODAY)
    assert text.startswith("Not refunded")
    assert "$45.00" in text


@pytest.mark.parametrize(
    "text_for",
    [
        lambda: order_text("9999", TODAY),
        lambda: eligibility_text("9999", TODAY),
        lambda: refund_text("9999", 10.0, "x", TODAY),
    ],
)
def test_unknown_order_is_reported_not_raised(text_for):
    assert "no order 9999" in text_for()


def test_order_text_shows_the_reissue_link_both_ways():
    assert "reissued as order 1077" in order_text("1028", TODAY)
    assert "replacement for order 1028" in order_text("1077", TODAY)
