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

import argparse
import re
import sqlite3
from collections.abc import Iterable
from pathlib import Path

import pandas as pd
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_SOURCE_DB = Path("shopdata.db")
DEFAULT_TARGET_DB = Path("analytics.db")
DEFAULT_FALLBACK_DIR = Path("output")

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
    # Build as an object column so missing phones stay None (NULL in SQLite).
    df["phone"] = pd.Series([standardize_phone(v) for v in df["phone"]],
                            index=df.index, dtype=object)
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


# ---------------------------------------------------------------------------
# Prefect tasks
# ---------------------------------------------------------------------------
# DataFrames are passed between tasks in memory; NO_CACHE stops Prefect from
# trying to hash them for its result cache.

@task(name="extract-view", retries=2, retry_delay_seconds=2, cache_policy=NO_CACHE)
def extract_view(db_path: Path, view_name: str) -> pd.DataFrame:
    """Read a whole view from the source database, opened read-only."""
    logger = get_run_logger()
    db_path = Path(db_path)
    if not db_path.is_file():
        raise FileNotFoundError(f"Source database not found: {db_path.resolve()}")
    try:
        with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as con:
            df = pd.read_sql_query(f'SELECT * FROM "{view_name}"', con)
    except sqlite3.Error as exc:
        logger.error("Failed to read %s from %s: %s", view_name, db_path, exc)
        raise
    logger.info("Extracted %s rows from %s", f"{len(df):,}", view_name)
    return df


@task(name="transform-customers", cache_policy=NO_CACHE)
def clean_customers(raw: pd.DataFrame) -> pd.DataFrame:
    """Task wrapper around :func:`transform_customers` with data-quality logging."""
    logger = get_run_logger()
    try:
        clean = transform_customers(raw)
    except DataValidationError:
        logger.exception("Customer data failed validation")
        raise
    logger.info(
        "Customers: %d raw rows -> %d unique customers (%d duplicate rows removed)",
        len(raw), len(clean), len(raw) - len(clean),
    )
    logger.info("Customers: %d emails defaulted to %s; %d with no phone",
                int((clean["email"] == UNKNOWN_EMAIL).sum()), UNKNOWN_EMAIL,
                int(clean["phone"].isna().sum()))
    if clean["signup_date"].isna().any():
        logger.warning("Customers: %d rows have an unparseable signup_date",
                       int(clean["signup_date"].isna().sum()))
    return clean


@task(name="transform-orders", cache_policy=NO_CACHE)
def clean_orders(raw: pd.DataFrame, rates: pd.DataFrame) -> pd.DataFrame:
    """Task wrapper around :func:`transform_orders` with data-quality logging."""
    logger = get_run_logger()
    try:
        clean = transform_orders(raw, rates)
    except DataValidationError:
        logger.exception("Order data failed validation")
        raise
    logger.info("Orders: %d raw rows -> %d valid orders (%d duplicate/invalid rows removed)",
                len(raw), len(clean), len(raw) - len(clean))
    missing_fx = clean.loc[clean["fx_rate_missing"], "currency"].value_counts()
    if not missing_fx.empty:
        logger.warning("Orders: %d non-USD orders had no exchange rate and were "
                       "treated as USD: %s", int(missing_fx.sum()), missing_fx.to_dict())
    return clean


@task(name="check-referential-integrity", cache_policy=NO_CACHE)
def check_orphan_orders(customers: pd.DataFrame, orders: pd.DataFrame) -> int:
    """Log (but keep) orders whose customer_id is not in dim_customers."""
    logger = get_run_logger()
    orphans = int((~orders["customer_id"].isin(customers["customer_id"])).sum())
    if orphans:
        logger.warning("%d orders reference unknown customer_ids; kept in fct_orders "
                       "but they will not appear in the CLV report", orphans)
    return orphans


@task(name="load-analytics", cache_policy=NO_CACHE)
def load_tables(customers: pd.DataFrame, orders: pd.DataFrame,
                target_db: Path, fallback_dir: Path) -> str:
    """Write ``dim_customers`` and ``fct_orders`` to ``target_db`` in one transaction.

    If the SQLite write fails, falls back to ``clean_customers.csv`` and
    ``clean_orders.csv`` in ``fallback_dir``. Returns where the data was written.
    """
    logger = get_run_logger()
    target_db = Path(target_db)
    try:
        target_db.parent.mkdir(parents=True, exist_ok=True)
        con = sqlite3.connect(target_db)
        try:
            with con:  # commits on success, rolls back on error
                customers.to_sql("dim_customers", con, if_exists="replace", index=False)
                orders.to_sql("fct_orders", con, if_exists="replace", index=False)
                con.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_customers_id "
                            "ON dim_customers(customer_id)")
                con.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_fct_orders_id "
                            "ON fct_orders(order_id)")
                con.execute("CREATE INDEX IF NOT EXISTS ix_fct_orders_customer "
                            "ON fct_orders(customer_id)")
        finally:
            con.close()
        logger.info("Loaded %d customers and %d orders into %s",
                    len(customers), len(orders), target_db)
        return str(target_db)
    except (sqlite3.Error, OSError) as exc:
        logger.error("Could not write %s (%s); falling back to CSV", target_db, exc)
        fallback_dir = Path(fallback_dir)
        fallback_dir.mkdir(parents=True, exist_ok=True)
        customers.to_csv(fallback_dir / "clean_customers.csv", index=False)
        orders.to_csv(fallback_dir / "clean_orders.csv", index=False)
        logger.info("Wrote CSV fallback to %s", fallback_dir.resolve())
        return str(fallback_dir)


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="shopdata-etl", log_prints=True)
def shopdata_etl(source_db: Path = DEFAULT_SOURCE_DB,
                 target_db: Path = DEFAULT_TARGET_DB,
                 fallback_dir: Path = DEFAULT_FALLBACK_DIR) -> dict:
    """Extract raw views, clean them and load the analytics tables."""
    logger = get_run_logger()
    logger.info("Starting ShopData ETL: %s -> %s", source_db, target_db)

    # Extract: the three reads are independent, so submit them concurrently.
    customers_f = extract_view.submit(source_db, CUSTOMERS_VIEW)
    orders_f = extract_view.submit(source_db, ORDERS_VIEW)
    rates_f = extract_view.submit(source_db, RATES_VIEW)

    # Transform
    customers = clean_customers(customers_f.result())
    orders = clean_orders(orders_f.result(), rates_f.result())
    orphans = check_orphan_orders(customers, orders)

    # Load
    destination = load_tables(customers, orders, target_db, fallback_dir)

    summary = {
        "customers_loaded": len(customers),
        "orders_loaded": len(orders),
        "orders_without_fx_rate": int(orders["fx_rate_missing"].sum()),
        "orphan_orders": orphans,
        "destination": destination,
    }
    logger.info("ETL finished: %s", summary)
    return summary


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the ShopData ETL flow.")
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    parser.add_argument("--target-db", type=Path, default=DEFAULT_TARGET_DB)
    parser.add_argument("--fallback-dir", type=Path, default=DEFAULT_FALLBACK_DIR)
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    shopdata_etl(args.source_db, args.target_db, args.fallback_dir)
