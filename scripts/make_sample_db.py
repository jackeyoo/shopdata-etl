"""Generate a *synthetic* ``shopdata.db`` for local development.

The real ``shopdata.db`` is supplied by ShopData Inc. and is not committed to this
repository. This script builds a small stand-in with the same three views and the
kinds of defects described in the brief (duplicates, nulls, negative amounts,
inconsistent formats) so the pipeline and SQL can be exercised end to end.

Usage:
    python scripts/make_sample_db.py            # writes ./shopdata.db
    python scripts/make_sample_db.py out.db     # custom path

Never point this at the real database path: it overwrites the target file.
"""

from __future__ import annotations

import random
import sqlite3
import sys
from datetime import date, timedelta
from pathlib import Path

SEED = 42
FIRST_NAMES = ["Ann", "Ben", "Chai", "Dao", "Eve", "Fah", "Gus", "Hana", "Ian", "June",
               "Krit", "Lena", "Mali", "Nat", "Oak", "Pim", "Quinn", "Rin", "Som", "Tara"]
LAST_NAMES = ["Smith", "Wong", "Saelim", "Tan", "Garcia", "Chen", "Suk", "Lee", "Kaew", "Brown"]
CURRENCIES = ["USD", "EUR", "THB", "GBP", "JPY"]
BASE_RATES = {"EUR": 1.08, "THB": 0.028, "GBP": 1.27, "JPY": 0.0068}


def _fmt_phone(rng: random.Random, digits: str) -> str:
    """Render the same phone number in one of several messy legacy formats."""
    style = rng.randint(0, 4)
    if style == 0:
        return f"+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    if style == 1:
        return f"{digits[1:4]}.{digits[4:7]}.{digits[7:]}"
    if style == 2:
        return f"1-{digits[1:4]}-{digits[4:7]}-{digits[7:]}"
    if style == 3:
        return f" {digits} "
    return f"({digits[1:4]}) {digits[4:7]} {digits[7:]} ext"


def _fmt_date(rng: random.Random, d: date) -> str:
    """Mostly ISO dates, with some legacy slash/timestamp formats mixed in."""
    style = rng.random()
    if style < 0.8:
        return d.isoformat()
    if style < 0.9:
        return d.strftime("%Y/%m/%d")
    return f"{d.isoformat()} 10:15:00"


def build(path: Path) -> None:
    rng = random.Random(SEED)
    if path.exists():
        path.unlink()
    con = sqlite3.connect(path)
    cur = con.cursor()
    cur.executescript(
        """
        CREATE TABLE raw_customers (
            customer_id INTEGER, first_name TEXT, last_name TEXT,
            email TEXT, phone TEXT, signup_date TEXT, country TEXT
        );
        CREATE TABLE raw_orders (
            order_id INTEGER, customer_id INTEGER, order_date TEXT,
            total_amount REAL, currency TEXT, status TEXT
        );
        CREATE TABLE raw_exchange_rates (
            rate_date TEXT, currency TEXT, rate_to_usd REAL
        );
        """
    )

    # --- customers ---------------------------------------------------------
    customers = []
    start = date(2022, 1, 1)
    for cid in range(1, 61):
        fn, ln = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        digits = "1" + "".join(str(rng.randint(0, 9)) for _ in range(10))
        signup = start + timedelta(days=rng.randint(0, 900))
        email = f"{fn}.{ln}{cid}@example.com".lower()
        roll = rng.random()
        if roll < 0.10:
            email = None
        elif roll < 0.15:
            email = ""
        elif roll < 0.20:
            email = email.upper()
        phone = _fmt_phone(rng, digits) if rng.random() > 0.05 else None
        customers.append((cid, fn, ln, email, phone, _fmt_date(rng, signup), "US"))
        # ~20% of customers have an older, stale duplicate record
        if rng.random() < 0.2:
            older = signup - timedelta(days=rng.randint(30, 300))
            customers.append((cid, fn, ln, None, _fmt_phone(rng, digits),
                              older.isoformat(), "US"))
    rng.shuffle(customers)
    cur.executemany("INSERT INTO raw_customers VALUES (?,?,?,?,?,?,?)", customers)

    # --- exchange rates (one row per currency per day, with gaps) ----------
    rates = []
    d = date(2022, 1, 1)
    while d <= date(2024, 12, 31):
        for ccy, base in BASE_RATES.items():
            if rng.random() < 0.03:  # ~3% of days have no published rate
                continue
            rates.append((d.isoformat(), ccy, round(base * rng.uniform(0.97, 1.03), 6)))
        d += timedelta(days=1)
    cur.executemany("INSERT INTO raw_exchange_rates VALUES (?,?,?)", rates)

    # --- orders ------------------------------------------------------------
    orders = []
    for oid in range(1001, 1401):
        cid = rng.randint(1, 60) if rng.random() > 0.02 else rng.randint(900, 999)  # orphans
        od = date(2022, 3, 1) + timedelta(days=rng.randint(0, 1000))
        ccy = rng.choice(CURRENCIES)
        amount = round(rng.uniform(5, 500) * (1 / BASE_RATES.get(ccy, 1)), 2)
        roll = rng.random()
        if roll < 0.04:
            amount = -amount           # refunds / system errors
        elif roll < 0.06:
            amount = 0.0
        elif roll < 0.08:
            amount = None
        croll = rng.random()
        if croll < 0.05:
            ccy = None
        elif croll < 0.10:
            ccy = ccy.lower() + " "
        elif croll < 0.12:
            ccy = "CHF"                # currency with no rate table
        orders.append((oid, cid, _fmt_date(rng, od), amount, ccy,
                       rng.choice(["completed", "completed", "shipped", "cancelled"])))
    orders += rng.sample(orders, 8)    # exact duplicate rows from a double load
    cur.executemany("INSERT INTO raw_orders VALUES (?,?,?,?,?,?)", orders)

    cur.executescript(
        """
        CREATE VIEW vw_raw_customers AS SELECT * FROM raw_customers;
        CREATE VIEW vw_raw_orders AS SELECT * FROM raw_orders;
        CREATE VIEW vw_exchange_rates AS SELECT * FROM raw_exchange_rates;
        """
    )
    con.commit()
    con.close()
    print(f"Wrote synthetic source database to {path}")


if __name__ == "__main__":
    build(Path(sys.argv[1]) if len(sys.argv) > 1 else Path("shopdata.db"))
