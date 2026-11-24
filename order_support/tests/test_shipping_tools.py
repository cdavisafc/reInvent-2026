# ABOUTME: Tests for what the Shipping specialist's tools tell the model: shipment status, delivery
# estimates, expedite confirmations and refusals, and the optional carrier flakiness.

from datetime import date

import pytest

from shipping.tools import (
    CarrierUnavailable,
    delivery_estimate_text,
    expedite_text,
    raise_if_carrier_flakes,
    shipment_status_text,
)

TODAY = date(2026, 12, 2)  # a Wednesday


def test_status_of_a_late_shipment_says_where_it_is_and_that_it_is_late():
    text = shipment_status_text("1042", TODAY)
    assert "in transit" in text
    assert "Reno" in text
    assert "late" in text
    assert "UPS" in text


def test_status_of_a_delivered_order_gives_the_delivery_date():
    text = shipment_status_text("1017", TODAY)
    assert "delivered" in text
    assert "2026-11-22" in text


def test_estimate_names_the_weekday_so_the_model_can_compare_to_friday():
    text = delivery_estimate_text("1042", TODAY)
    assert "Thursday, 2026-12-10" in text
    assert "promised by Tuesday, 2026-12-01" in text


def test_estimate_for_a_lost_shipment_says_there_is_none():
    assert "lost" in delivery_estimate_text("1028", TODAY)


def test_expedite_confirms_with_an_id():
    assert "EXP-1042" in expedite_text("1042", TODAY)


def test_expedite_refusal_is_reported_not_raised():
    text = expedite_text("1017", TODAY)
    assert text.startswith("Not expedited")
    assert "delivered" in text


@pytest.mark.parametrize(
    "text_for", [shipment_status_text, delivery_estimate_text, expedite_text]
)
def test_unknown_order_is_reported_not_raised(text_for):
    assert "no order 9999" in text_for("9999", TODAY)


def test_carrier_never_flakes_by_default(monkeypatch):
    monkeypatch.delenv("CARRIER_FLAKE_RATE", raising=False)
    raise_if_carrier_flakes(rng=lambda: 0.0)


def test_carrier_flakes_when_the_roll_is_under_the_rate(monkeypatch):
    monkeypatch.setenv("CARRIER_FLAKE_RATE", "0.5")
    with pytest.raises(CarrierUnavailable):
        raise_if_carrier_flakes(rng=lambda: 0.49)
    raise_if_carrier_flakes(rng=lambda: 0.5)


def test_status_of_a_reissued_order_names_its_replacement():
    assert "reissued as order 1077" in shipment_status_text("1028", TODAY)


def test_status_of_a_replacement_names_the_order_it_replaces():
    assert "replacement for order 1028" in shipment_status_text("1077", TODAY)
