# ShopData ETL

An ETL pipeline, orchestrated with Prefect, that cleans ShopData Inc.'s legacy order data
(SQLite views) and loads it into an analytics database for Customer Lifetime Value (CLV)
reporting.

```
shopdata.db (read-only views)            analytics.db
 ├─ vw_raw_customers ──┐                  ├─ dim_customers
 ├─ vw_raw_orders ─────┼──► pipeline.py ─►└─ fct_orders ──► clv_report.sql
 └─ vw_exchange_rates ─┘    (Prefect flow)
```

## Repository layout

| Path | Purpose |
|---|---|
| `exploration.sql` | Part 1: data-quality profiling queries on the raw views |
| `pipeline.py` | Part 2: transformation functions, Prefect tasks and the `shopdata-etl` flow |
| `tests/` | Part 3: pytest unit tests for the transformation logic (no database needed) |
| `clv_report.sql` | Part 4: CLV report query on `dim_customers` / `fct_orders` |
| `scripts/make_sample_db.py` | Dev helper: builds a larger synthetic source DB for load testing |
| `requirements.txt` | Python dependencies |

## Setup

Requires Python 3.12+.

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Put the provided `shopdata.db` in the project root. (It is git-ignored and not part of
the repository.) For a larger load test, `python scripts/make_sample_db.py sample.db`
generates a synthetic database with the same schema and similar defects, which you can
run with `python pipeline.py --source-db sample.db`.

## Running the flow

```bash
python pipeline.py
# or with explicit paths
python pipeline.py --source-db path/to/shopdata.db --target-db analytics.db --fallback-dir output
```

Prefect 3 starts a temporary local API server automatically, so no `prefect server start`
is needed. To view runs in the Prefect UI, start `prefect server start` in another
terminal first and set `PREFECT_API_URL=http://127.0.0.1:4200/api`.

The flow writes `dim_customers` and `fct_orders` to `analytics.db`. If the database can't
be written, it falls back to `output/clean_customers.csv` and `output/clean_orders.csv`.

### Flow structure

| Task | What it does |
|---|---|
| `extract-view` (×3, concurrent) | Reads each view over a **read-only** SQLite connection; retries twice on failure |
| `transform-customers` | Dedupes customers, cleans phone/email, builds `full_name`, normalises dates |
| `transform-orders` | Removes duplicate and invalid orders, converts amounts to USD |
| `check-referential-integrity` | Warns about orders whose `customer_id` is not in `dim_customers` |
| `load-analytics` | Writes both tables in one transaction and adds indexes; CSV fallback on error |

Each task logs row counts in and out, plus every data-quality fix it applied (emails
defaulted, orders missing an FX rate, orphan orders). Validation failures raise a
`DataValidationError` that names the missing columns, so a schema change fails loudly
instead of producing silently wrong output.

## Running the tests

```bash
pytest
```

The 31 tests build small in-memory DataFrames and call the pure transformation functions
directly. They never open a database. Coverage includes:

- **Phone standardiser**: all legacy formats, integer/float values, missing values
- **Email cleaning**: null/blank/`"NULL"` placeholders, trimming and lower-casing
- **Customer dedup**: latest `signup_date` wins, compared as dates rather than strings
  (`2023/01/05` vs `2023-03-01`), a dated record preferred over an undated one
- **Order filtering**: zero, negative, null and text amounts such as `"$1,250.50"`
- **Currency conversion**: rate matched on the order date (including timestamps), missing
  currency → USD, missing rate → USD and flagged, duplicate rate rows don't fan out orders

## Running the CLV report

```bash
sqlite3 analytics.db < clv_report.sql
```

The output columns are `clv_rank`, `customer_id`, `full_name`, `total_orders_placed`,
`lifetime_value_usd` and `customer_cohort` (`YYYY-MM`), sorted by `lifetime_value_usd`
descending. Orders are aggregated per customer before the join, and customers with no
valid orders are kept with a CLV of 0.

Output on the provided `shopdata.db`:

| clv_rank | customer_id | full_name | total_orders_placed | lifetime_value_usd | customer_cohort |
|---|---|---|---|---|---|
| 1 | 3 | Charlie Brown | 1 | 25,000.00 | 2023-03 |
| 2 | 1 | Alice Smith | 3 | 1,686.00 | 2023-06 |
| 3 | 6 | Fiona Gallagher | 2 | 525.00 | 2023-05 |
| 4 | 4 | Diana Prince | 2 | 389.50 | 2023-04 |
| 5 | 2 | Bob Jones | 2 | 275.00 | 2023-09 |
| 6 | 5 | Evan Wright | 2 | 219.99 | 2023-04 |
| 7 | 8 | Hannah Abbott | 1 | 89.00 | 2023-07 |
| 8 | 7 | George Costanza | 2 | 55.99 | 2023-06 |
| 9 | 9 | Ian Malcolm | 0 | 0.00 | 2023-08 |
| 9 | 10 | Jane Doe | 0 | 0.00 | 2023-09 |

Charlie Brown's rank comes from the missing-JPY-rate fallback described in the
exploration findings below.

## Part 1: Data exploration findings

Run with `sqlite3 shopdata.db < exploration.sql`. The query IDs below (C1, O1, …) refer
to sections of that file. Counts are from the provided `shopdata.db`
(12 customer rows, 20 order rows, 15 exchange-rate rows).

| # | Issue found | Query | Handling in the pipeline |
|---|---|---|---|
| 1 | **Duplicate customers.** 2 customers appear twice: Alice Smith (id 1) and Bob Jones (id 2). The newer row carries an updated email. | C1, C1b | Keep the row with the latest `signup_date` per `customer_id` (12 rows → 10 customers) |
| 2 | **Missing emails.** 2 rows have a `NULL` email: Bob's stale row and Hannah Abbott (id 8). | C2 | Replace with `unknown@domain.com`. After dedup, only Hannah needs the default. |
| 3 | **Phone numbers in 8 different formats**, e.g. `+1 (555) 123-4567`, `555-987-6543`, `(555) 333 4444`, `+44 20 7123 1234`. 2 are `NULL`. | C3, C3b | Strip every non-digit character |
| 4 | **Phones that aren't usable numbers:** `Ext 444` and the vanity number `1-800-555-DINO` | C3 | Digit-stripping (per the brief) yields `444` and `1800555`. Both are logged as suspicious (< 10 digits) for follow-up. |
| 5 | **Non-positive order amounts:** orders 103 (-50) and 113 (-100) have status `SYSTEM_ERROR`; order 114 is `0`. | O1, O8 | Drop orders with `total_amount <= 0` (20 → 17 orders) |
| 6 | **Missing currency** on orders 107 and 116 | O3 | Treated as `USD` |
| 7 | **Exchange rates only cover 2023-05-01 to 2023-05-05**, but orders run to 2023-05-14. Six raw non-USD orders have no rate; five of them survive filtering. | O4, R1 | Rule from the brief: treat the amount as USD. Rows are flagged with `fx_rate_missing` (see note below). |
| 8 | **Orphan orders:** 106 and 118 reference `customer_id` 99, which doesn't exist | O5 | Kept in `fct_orders` with a warning in the logs. They drop out of the CLV report because of the join. |
| 9 | **Missing order date:** order 117 has `NULL` `order_date` | O7 | Kept (it is USD, so no conversion is needed) and logged |
| 10 | **Mixed order statuses:** `COMPLETED`, `PENDING`, `CANCELLED`, `SYSTEM_ERROR` | O8 | See assumptions below |

### Note on the "missing rate → USD" rule

The rule in the brief is applied as written, but in this dataset it materially distorts
CLV. Order 115 is **¥25,000 (about $175)**, dated 2023-05-10, after the last JPY rate.
Under the rule it is counted as **$25,000**, which makes Charlie Brown the top customer by
a wide margin. Likewise Alice Smith's £1,200 order (111) is counted as $1,200 instead of
about $1,500.

To keep this auditable, `fct_orders` stores `currency`, `total_amount`, `exchange_rate`
and an `fx_rate_missing` flag, and the flow logs how many orders fell back. In production
I would use the most recent prior rate for that currency (an "as-of" join, e.g.
`pd.merge_asof`) or quarantine those rows. I'd confirm the right choice with the business
before changing the rule.

### Other assumptions

- The brief defines invalid orders only as `total_amount <= 0`, so `CANCELLED` (109) and
  `PENDING` (112) orders with a positive amount still count towards CLV. In practice I'd
  ask whether CLV should include only `COMPLETED` orders. Filtering by status would be a
  one-line change once the business defines what a "valid" order is.
- `total_amount` is assumed to be in the order's own currency, and `rate_to_usd` is a
  multiplier (`usd = amount × rate`).
- Column-name aliases for the exchange-rate view (`date`/`rate_date`, `rate`/`rate_to_usd`, …)
  are resolved automatically. Any other schema mismatch raises an error that lists the
  columns that were actually found.

## Git workflow

Each part was built on its own branch (`feature/data-exploration`, `feature/etl-pipeline`,
`feature/unit-tests`, `feature/clv-report`, `docs/readme`) and merged into `main` with
`--no-ff`, so the history shows how the work progressed one feature at a time. A follow-up branch
(`fix/align-with-source-data`) adjusts the SQL, logging and documentation after profiling
the provided `shopdata.db`.
