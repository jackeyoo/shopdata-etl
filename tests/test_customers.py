"""Unit tests for customer cleaning rules (no database involved)."""

import pandas as pd
import pytest

from pipeline import (
    UNKNOWN_EMAIL,
    DataValidationError,
    clean_email,
    deduplicate_customers,
    standardize_phone,
    transform_customers,
)


class TestStandardizePhone:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("+1 (555) 123-4567", "15551234567"),   # example from the brief
            ("555.123.4567", "5551234567"),
            ("1-555-123-4567", "15551234567"),
            ("  15551234567  ", "15551234567"),
            ("(555) 123 4567 ext", "5551234567"),
            (15551234567, "15551234567"),           # stored as INTEGER
            (15551234567.0, "15551234567"),         # surfaced as REAL by SQLite
        ],
    )
    def test_strips_all_non_digits(self, raw, expected):
        assert standardize_phone(raw) == expected

    @pytest.mark.parametrize("raw", [None, float("nan"), "", "N/A", "---"])
    def test_missing_or_digitless_values_become_none(self, raw):
        assert standardize_phone(raw) is None


class TestCleanEmail:
    def test_missing_and_blank_emails_get_placeholder(self):
        raw = pd.Series(["a@x.com", None, "", "   ", "NULL", float("nan")])
        result = clean_email(raw).tolist()
        assert result == ["a@x.com"] + [UNKNOWN_EMAIL] * 5

    def test_valid_emails_are_trimmed_and_lowercased(self):
        assert clean_email(pd.Series(["  Ann.Lee@Example.COM "])).tolist() == [
            "ann.lee@example.com"
        ]


class TestDeduplicateCustomers:
    def test_keeps_most_recent_signup_per_customer(self):
        df = pd.DataFrame(
            {
                "customer_id": [1, 1, 2, 1],
                "email": ["old@x.com", "newest@x.com", "b@x.com", "mid@x.com"],
                "signup_date": ["2022-01-01", "2023-06-01", "2022-05-05", "2022/12/31"],
            }
        )
        result = deduplicate_customers(df).set_index("customer_id")
        assert len(result) == 2
        assert result.loc[1, "email"] == "newest@x.com"
        assert result.loc[2, "email"] == "b@x.com"

    def test_mixed_date_formats_are_compared_chronologically(self):
        # As strings "2023/01/05" > "2023-03-01"; as dates it is older.
        df = pd.DataFrame(
            {
                "customer_id": [7, 7],
                "email": ["older@x.com", "newer@x.com"],
                "signup_date": ["2023/01/05", "2023-03-01 09:00:00"],
            }
        )
        assert deduplicate_customers(df)["email"].item() == "newer@x.com"

    def test_dated_record_preferred_over_missing_date(self):
        df = pd.DataFrame(
            {"customer_id": [3, 3], "email": ["dated@x.com", "undated@x.com"],
             "signup_date": ["2021-01-01", None]}
        )
        assert deduplicate_customers(df)["email"].item() == "dated@x.com"

    def test_missing_required_column_raises(self):
        with pytest.raises(DataValidationError, match="signup_date"):
            deduplicate_customers(pd.DataFrame({"customer_id": [1]}))


def test_transform_customers_end_to_end():
    raw = pd.DataFrame(
        {
            "customer_id": [1, 1, 2],
            "first_name": ["Ann", "Ann", " Ben "],
            "last_name": ["Lee", "Lee", "Tan"],
            "email": ["ann@x.com", None, ""],
            "phone": ["+1 (555) 000-1111", "555.000.1111", None],
            "signup_date": ["2022-01-01", "2023/02/15", "2022-07-01 08:00:00"],
        }
    )
    result = transform_customers(raw)

    assert result["customer_id"].tolist() == [1, 2]
    assert result["full_name"].tolist() == ["Ann Lee", "Ben Tan"]
    # latest record for customer 1 had no email -> placeholder
    assert result["email"].tolist() == [UNKNOWN_EMAIL, UNKNOWN_EMAIL]
    assert result["phone"].tolist() == ["5550001111", None]
    assert result["signup_date"].tolist() == ["2023-02-15", "2022-07-01"]
