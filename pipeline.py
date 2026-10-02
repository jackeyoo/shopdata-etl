"""ShopData ETL pipeline.

Extracts raw customer, order and exchange-rate data from the legacy SQLite views in
``shopdata.db``, applies the business cleaning rules, and loads the result into
``analytics.db`` as ``dim_customers`` and ``fct_orders``.

The module is split in two layers:

* **Pure transformation functions** (``standardize_phone``, ``deduplicate_customers``,
  ``convert_to_usd`` ...). They take and return plain values / DataFrames, do no I/O
  and are unit-tested in isolation (see ``tests/``).
* **Prefect tasks and the flow** that wrap those functions with I/O, logging,
  retries and error handling.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

import pandas as pd

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

UNKNOWN_EMAIL = "unknown@domain.com"
BASE_CURRENCY = "USD"

CUSTOMERS_VIEW = "vw_raw_customers"
ORDERS_VIEW = "vw_raw_orders"
RATES_VIEW = "vw_exchange_rates"

# Accepted source column names -> canonical name used throughout the pipeline.
# The first candidate that exists in the extracted frame wins.
RATE_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "rate_date": ("rate_date", "date", "exchange_date", "effective_date"),
    "currency": ("currency", "currency_code", "from_currency"),
    "rate_to_usd": ("rate_to_usd", "usd_rate", "exchange_rate", "rate"),
}

_PLACEHOLDER_STRINGS = {"", "null", "none", "nan", "n/a", "na"}


class DataValidationError(ValueError):
    """Raised when source data is missing columns the pipeline relies on."""


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def require_columns(df: pd.DataFrame, columns: Iterable[str], frame_name: str) -> None:
    """Fail fast with a clear message if ``df`` lacks any of ``columns``."""
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise DataValidationError(
            f"{frame_name} is missing required column(s) {missing}; "
            f"available columns: {list(df.columns)}"
        )


def rename_to_canonical(df: pd.DataFrame, aliases: dict[str, tuple[str, ...]]) -> pd.DataFrame:
    """Rename the first matching alias of each canonical column to its canonical name."""
    mapping = {}
    for canonical, candidates in aliases.items():
        for candidate in candidates:
            if candidate in df.columns:
                mapping[candidate] = canonical
                break
    return df.rename(columns=mapping)


def parse_dates(values: pd.Series) -> pd.Series:
    """Parse a column of mixed-format date strings into ``datetime64``.

    Handles ISO dates, ``YYYY/MM/DD`` and full timestamps. Unparseable values
    become ``NaT`` rather than raising.
    """
    return pd.to_datetime(values, errors="coerce", format="mixed")


def to_numeric_amount(values: pd.Series) -> pd.Series:
    """Coerce an amount column to float, tolerating ``"$1,234.50"``-style strings."""
    if pd.api.types.is_numeric_dtype(values):
        return values.astype("float64")
    cleaned = values.astype("string").str.replace(r"[^0-9.\-]", "", regex=True)
    return pd.to_numeric(cleaned, errors="coerce")


# ---------------------------------------------------------------------------
# Customer transformations
# ---------------------------------------------------------------------------

def standardize_phone(value: object) -> str | None:
    """Strip every non-digit character from a phone number.

    >>> standardize_phone("+1 (555) 123-4567")
    '15551234567'

    Returns ``None`` for missing values or values that contain no digits.
    Numbers that SQLite surfaced as floats (``15551234567.0``) are handled.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    digits = re.sub(r"\D", "", str(value))
    return digits or None


def clean_email(values: pd.Series) -> pd.Series:
    """Trim and lower-case emails; replace missing/blank ones with ``UNKNOWN_EMAIL``."""
    emails = values.astype("string").str.strip()
    is_missing = emails.isna() | emails.str.lower().isin(_PLACEHOLDER_STRINGS)
    return emails.str.lower().mask(is_missing, UNKNOWN_EMAIL).astype(object)


def build_full_name(df: pd.DataFrame) -> pd.Series:
    """Return a whitespace-normalised ``full_name`` series.

    Uses an existing ``full_name`` column when present, otherwise joins
    ``first_name`` and ``last_name``.
    """
    if "full_name" in df.columns:
        names = df["full_name"].astype("string")
    elif {"first_name", "last_name"} <= set(df.columns):
        names = (
            df["first_name"].astype("string").fillna("")
            + " "
            + df["last_name"].astype("string").fillna("")
        )
    else:
        raise DataValidationError(
            "customers need either a full_name column or first_name + last_name"
        )
    names = names.str.replace(r"\s+", " ", regex=True).str.strip()
    return names.mask(names == "").astype(object)


def deduplicate_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Keep one row per ``customer_id`` — the one with the most recent ``signup_date``.

    Rows with an unparseable/missing signup date sort first, so a dated record is
    always preferred. Ties keep the row that appeared last in the source.
    """
    require_columns(df, ["customer_id", "signup_date"], "customers")
    ranked = df.assign(_signup_ts=parse_dates(df["signup_date"]), _row=range(len(df)))
    ranked = ranked.sort_values(
        ["customer_id", "_signup_ts", "_row"], na_position="first", kind="mergesort"
    )
    latest = ranked.drop_duplicates(subset="customer_id", keep="last")
    return latest.drop(columns=["_signup_ts", "_row"]).reset_index(drop=True)


def transform_customers(raw: pd.DataFrame) -> pd.DataFrame:
    """Apply all customer cleaning rules and return the ``dim_customers`` frame."""
    require_columns(raw, ["customer_id", "signup_date", "phone", "email"], "customers")
    df = raw.dropna(subset=["customer_id"]).copy()
    df = deduplicate_customers(df)
    df["full_name"] = build_full_name(df)
    df["phone"] = df["phone"].map(standardize_phone)
    df["email"] = clean_email(df["email"])
    df["signup_date"] = parse_dates(df["signup_date"]).dt.strftime("%Y-%m-%d")

    leading = ["customer_id", "full_name", "email", "phone", "signup_date"]
    rest = [c for c in df.columns if c not in leading]
    return df[leading + rest].sort_values("customer_id").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Order transformations
# ---------------------------------------------------------------------------

def filter_valid_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Drop orders whose ``total_amount`` is missing, zero or negative.

    Non-positive totals are system errors per the business rule; a missing total
    cannot be valued either, so it is dropped too.
    """
    require_columns(df, ["total_amount"], "orders")
    amounts = to_numeric_amount(df["total_amount"])
    return df.assign(total_amount=amounts).loc[amounts > 0].reset_index(drop=True)


def normalize_currency(values: pd.Series) -> pd.Series:
    """Upper-case and trim currency codes; missing/blank codes become ``USD``."""
    codes = values.astype("string").str.strip().str.upper()
    is_missing = codes.isna() | codes.str.lower().isin(_PLACEHOLDER_STRINGS)
    return codes.mask(is_missing, BASE_CURRENCY).astype(object)


def prepare_rates(rates: pd.DataFrame) -> pd.DataFrame:
    """Clean the exchange-rate table to one positive rate per (currency, date)."""
    rates = rename_to_canonical(rates, RATE_COLUMN_ALIASES)
    require_columns(rates, ["rate_date", "currency", "rate_to_usd"], "exchange rates")
    out = pd.DataFrame(
        {
            "currency": normalize_currency(rates["currency"]),
            "rate_date": parse_dates(rates["rate_date"]).dt.normalize(),
            "rate_to_usd": pd.to_numeric(rates["rate_to_usd"], errors="coerce"),
        }
    )
    out = out.dropna(subset=["rate_date", "rate_to_usd"])
    out = out.loc[out["rate_to_usd"] > 0]
    return out.drop_duplicates(subset=["currency", "rate_date"], keep="last")


def convert_to_usd(orders: pd.DataFrame, rates: pd.DataFrame) -> pd.DataFrame:
    """Add ``exchange_rate`` and ``usd_amount`` columns to ``orders``.

    Each order is matched to the rate for its currency on its ``order_date``
    (``usd_amount = total_amount * rate_to_usd``). If the currency is missing, or
    there is no rate for that currency on that date, the amount is assumed to
    already be in USD (rate = 1.0). A boolean ``fx_rate_missing`` column flags
    non-USD orders that fell back to USD so they can be audited.
    """
    require_columns(orders, ["order_date", "total_amount"], "orders")
    df = orders.copy()
    if "currency" not in df.columns:
        df["currency"] = None
    df["currency"] = normalize_currency(df["currency"])
    df["_order_day"] = parse_dates(df["order_date"]).dt.normalize()

    clean_rates = prepare_rates(rates)
    df = df.merge(
        clean_rates,
        how="left",
        left_on=["currency", "_order_day"],
        right_on=["currency", "rate_date"],
        validate="many_to_one",
    )

    is_usd = df["currency"] == BASE_CURRENCY
    df["fx_rate_missing"] = (~is_usd) & df["rate_to_usd"].isna()
    df["exchange_rate"] = df["rate_to_usd"].where(~is_usd, 1.0).fillna(1.0)
    df["usd_amount"] = (to_numeric_amount(df["total_amount"]) * df["exchange_rate"]).round(2)
    return df.drop(columns=["_order_day", "rate_date", "rate_to_usd"])


def transform_orders(raw: pd.DataFrame, rates: pd.DataFrame) -> pd.DataFrame:
    """Apply all order cleaning rules and return the ``fct_orders`` frame."""
    require_columns(raw, ["order_id", "customer_id", "order_date", "total_amount"], "orders")
    df = raw.drop_duplicates()                                   # exact re-loaded rows
    df = filter_valid_orders(df)
    df = df.drop_duplicates(subset="order_id", keep="last")      # conflicting re-loads
    df = convert_to_usd(df, rates)
    df["order_date"] = parse_dates(df["order_date"]).dt.strftime("%Y-%m-%d")

    leading = ["order_id", "customer_id", "order_date", "currency",
               "total_amount", "exchange_rate", "usd_amount", "fx_rate_missing"]
    rest = [c for c in df.columns if c not in leading]
    return df[leading + rest].sort_values("order_id").reset_index(drop=True)
