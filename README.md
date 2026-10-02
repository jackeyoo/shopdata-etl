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
| `scripts/make_sample_db.py` | Dev helper: builds a synthetic `shopdata.db` for local runs |
| `requirements.txt` | Python dependencies |

## Setup

Requires Python 3.12+.

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Put the provided `shopdata.db` in the project root. (If you don't have it, run
`python scripts/make_sample_db.py` to generate a synthetic one with the same views.)

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

## Part 1: Data exploration findings

Run with `sqlite3 shopdata.db < exploration.sql`. The query IDs below (C1, O1, …) refer
to sections of that file.

| # | Issue | Query | Handling in the pipeline |
|---|---|---|---|
| 1 | **Duplicate customers.** The same `customer_id` appears several times with different `signup_date`s. The older rows are stale (often without an email). | C1, C1b | Keep the row with the latest parsed `signup_date` per `customer_id` |
| 2 | **Missing and inconsistent emails.** Values are `NULL` or blank, and some are upper-case. | C2 | Trim and lower-case; replace missing values with `unknown@domain.com` |
| 3 | **Phone numbers in many formats**, e.g. `+1 (555) 123-4567`, `555.123.4567`, `1-555-…`, a trailing `ext`, padded whitespace | C3, C3b | Strip every non-digit character; a value with no digits becomes `NULL` |
| 4 | **Invalid order amounts**: `NULL`, `0` and negative `total_amount` | O1 | Drop any order where `total_amount` is not > 0 |
| 5 | **Duplicate order rows** (the same `order_id` loaded twice) | O2 | Drop exact duplicates, then keep one row per `order_id` |
| 6 | **Dirty currency codes**: `NULL`, lower-case and padded values (`'eur '`) | O3 | Trim and upper-case; a missing code becomes `USD` |
| 7 | **Missing exchange rates.** Some currency/date pairs have no rate, and some currencies (e.g. `CHF`) have none at all. | O4 | Rule from the brief: treat the amount as USD. Rows are flagged with `fx_rate_missing` (see note below) |
| 8 | **Mixed date formats** (`YYYY-MM-DD`, `YYYY/MM/DD`, full timestamps) | C4, O6 | Parse with a format-tolerant parser and store as ISO `YYYY-MM-DD` |
| 9 | **Orphan orders** whose `customer_id` is missing from the customer view | O5 | Kept in `fct_orders` with a warning in the logs. They drop out of the CLV report because of the join. |

> The issues above were found by running `exploration.sql` against the database. Exact
> row counts depend on the data, so run the script to see the current numbers.

### Note on the "missing rate → USD" rule

The rule in the brief is applied as written, but it can badly inflate revenue. A
¥15,000 order with no JPY rate for its date is counted as **$15,000** instead of
about $100. To make this auditable, `fct_orders` keeps `currency`, `total_amount`,
`exchange_rate` and an `fx_rate_missing` flag, and the flow logs how many orders fell back.
In production I would use the most recent prior rate for that currency (an "as-of" join)
or quarantine those rows. I'd confirm the right choice with the business before changing
the rule.

### Other assumptions

- The cleaning rules treat order `status` (for example `cancelled`) as out of scope, so
  every positive-amount order counts towards CLV. Filtering by status would be a
  one-line change once the business defines what a "valid" order is.
- `total_amount` is assumed to be in the order's own currency, and `rate_to_usd` is a
  multiplier (`usd = amount × rate`).
- Column-name aliases for the exchange-rate view (`date`/`rate_date`, `rate`/`rate_to_usd`, …)
  are resolved automatically. Any other schema mismatch raises an error that lists the
  columns that were actually found.

## Git workflow

Each part was built on its own branch (`feature/data-exploration`, `feature/etl-pipeline`,
`feature/unit-tests`, `feature/clv-report`, `docs/readme`) and merged into `main` with
`--no-ff`, so the history shows how the work progressed one feature at a time.
