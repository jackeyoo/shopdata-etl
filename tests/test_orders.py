"""Unit tests for order cleaning and currency conversion (no database involved)."""

import pandas as pd
import pytest

from pipeline import (
    convert_to_usd,
    filter_valid_orders,
    normalize_currency,
    transform_orders,
)


@pytest.fixture
def rates() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "rate_date": ["2024-01-01", "2024-01-02", "2024-01-01"],
            "currency": ["EUR", "EUR", "THB"],
            "rate_to_usd": [1.10, 1.20, 0.03],
        }
    )


def _orders(**cols) -> pd.DataFrame:
    return pd.DataFrame(cols)


class TestFilterValidOrders:
    def test_removes_zero_negative_and_null_amounts(self):
        df = _orders(order_id=[1, 2, 3, 4, 5], total_amount=[10.0, 0.0, -5.0, None, 0.01])
        assert filter_valid_orders(df)["order_id"].tolist() == [1, 5]

    def test_parses_amounts_stored_as_text(self):
        df = _orders(order_id=[1, 2], total_amount=["$1,250.50", "-3"])
        result = filter_valid_orders(df)
        assert result["order_id"].tolist() == [1]
        assert result["total_amount"].item() == pytest.approx(1250.50)


class TestNormalizeCurrency:
    def test_trims_uppercases_and_defaults_to_usd(self):
        raw = pd.Series([" eur", "THB ", None, "", "usd"])
        assert normalize_currency(raw).tolist() == ["EUR", "THB", "USD", "USD", "USD"]


class TestConvertToUsd:
    def test_uses_rate_for_the_order_date(self, rates):
        df = _orders(order_date=["2024-01-01", "2024-01-02"],
                     total_amount=[100.0, 100.0], currency=["EUR", "EUR"])
        result = convert_to_usd(df, rates)
        assert result["usd_amount"].tolist() == [110.0, 120.0]
        assert not result["fx_rate_missing"].any()

    def test_order_timestamp_matches_daily_rate(self, rates):
        df = _orders(order_date=["2024/01/01 18:30:00"], total_amount=[1000.0],
                     currency=["thb"])
        assert convert_to_usd(df, rates)["usd_amount"].item() == 30.0

    def test_missing_currency_is_treated_as_usd(self, rates):
        df = _orders(order_date=["2024-01-01"], total_amount=[42.5], currency=[None])
        result = convert_to_usd(df, rates)
        assert result["currency"].item() == "USD"
        assert result["usd_amount"].item() == 42.5
        assert not result["fx_rate_missing"].item()

    def test_currency_without_rate_falls_back_to_usd_and_is_flagged(self, rates):
        df = _orders(order_date=["2024-01-01", "2024-03-01"],
                     total_amount=[50.0, 80.0], currency=["CHF", "EUR"])
        result = convert_to_usd(df, rates)
        assert result["usd_amount"].tolist() == [50.0, 80.0]
        assert result["exchange_rate"].tolist() == [1.0, 1.0]
        assert result["fx_rate_missing"].tolist() == [True, True]

    def test_usd_orders_are_unchanged(self, rates):
        df = _orders(order_date=["2024-01-01"], total_amount=[19.99], currency=["USD"])
        assert convert_to_usd(df, rates)["usd_amount"].item() == 19.99

    def test_duplicate_rates_do_not_duplicate_orders(self, rates):
        dup_rates = pd.concat([rates, rates], ignore_index=True)
        df = _orders(order_date=["2024-01-01"], total_amount=[10.0], currency=["EUR"])
        assert len(convert_to_usd(df, dup_rates)) == 1

    def test_accepts_alternative_rate_column_names(self):
        alt = pd.DataFrame({"date": ["2024-01-01"], "currency_code": ["EUR"],
                            "rate": [2.0]})
        df = _orders(order_date=["2024-01-01"], total_amount=[10.0], currency=["EUR"])
        assert convert_to_usd(df, alt)["usd_amount"].item() == 20.0

    def test_usd_amount_is_rounded_to_cents(self, rates):
        df = _orders(order_date=["2024-01-01"], total_amount=[33.333], currency=["THB"])
        assert convert_to_usd(df, rates)["usd_amount"].item() == 1.0


def test_transform_orders_end_to_end(rates):
    raw = _orders(
        order_id=[1, 1, 2, 3, 4],
        customer_id=[10, 10, 10, 11, 12],
        order_date=["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-01", "2024-01-01"],
        total_amount=[100.0, 100.0, -20.0, 0.0, 200.0],
        currency=["eur", "eur", "EUR", "USD", None],
    )
    result = transform_orders(raw, rates)

    assert result["order_id"].tolist() == [1, 4]          # dup, negative, zero removed
    assert result["usd_amount"].tolist() == [110.0, 200.0]
    assert result["order_date"].tolist() == ["2024-01-01", "2024-01-01"]
