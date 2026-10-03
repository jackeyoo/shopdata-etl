-- =============================================================================
-- exploration.sql
-- Data-quality profiling of the raw ShopData views, run before any Python.
-- Each query is independent; run with:  sqlite3 shopdata.db < exploration.sql
-- =============================================================================

.headers on
.mode column

-- -----------------------------------------------------------------------------
-- 0. Schema & volume
-- -----------------------------------------------------------------------------
SELECT 'vw_raw_customers'  AS view_name, COUNT(*) AS row_count FROM vw_raw_customers
UNION ALL
SELECT 'vw_raw_orders',     COUNT(*) FROM vw_raw_orders
UNION ALL
SELECT 'vw_exchange_rates', COUNT(*) FROM vw_exchange_rates;

-- =============================================================================
-- CUSTOMERS
-- =============================================================================

-- C1. Duplicate customer_id values (same customer stored more than once)
SELECT
    COUNT(*)                         AS total_rows,
    COUNT(DISTINCT customer_id)      AS distinct_customers,
    COUNT(*) - COUNT(DISTINCT customer_id) AS duplicate_rows
FROM vw_raw_customers;

-- C1b. Examples of duplicated customers and how their records differ
SELECT customer_id, email, phone, signup_date
FROM vw_raw_customers
WHERE customer_id IN (
    SELECT customer_id FROM vw_raw_customers
    GROUP BY customer_id HAVING COUNT(*) > 1
)
ORDER BY customer_id, signup_date
LIMIT 20;

-- C2. Missing / blank emails, and emails that are not lower-case
SELECT
    SUM(email IS NULL)                         AS null_email,
    SUM(TRIM(email) = '')                      AS blank_email,
    SUM(email IS NOT NULL AND email <> LOWER(email)) AS mixed_case_email,
    SUM(email IS NOT NULL AND TRIM(email) <> ''
        AND email NOT LIKE '%_@_%._%')         AS malformed_email
FROM vw_raw_customers;

-- C3. Phone numbers that contain non-digit characters (inconsistent formats)
SELECT
    SUM(phone IS NULL)                                     AS null_phone,
    SUM(phone GLOB '*[^0-9]*')                             AS phone_with_non_digits,
    SUM(phone GLOB '*[A-Za-z]*')                           AS phone_with_letters,
    COUNT(DISTINCT LENGTH(phone))                          AS distinct_raw_lengths
FROM vw_raw_customers;

-- C3b. Sample of the distinct phone "shapes" (digits replaced with 9)
SELECT shape, COUNT(*) AS n
FROM (
    SELECT REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
           phone,'0','9'),'1','9'),'2','9'),'3','9'),'4','9'),'5','9'),'6','9'),'7','9'),'8','9'),'9','9') AS shape
    FROM vw_raw_customers
    WHERE phone IS NOT NULL
)
GROUP BY shape
ORDER BY n DESC;

-- C4. signup_date stored in more than one format (or unparseable)
SELECT
    CASE
        WHEN signup_date IS NULL                                 THEN 'NULL'
        WHEN signup_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'   THEN 'YYYY-MM-DD'
        WHEN signup_date GLOB '[0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9]'   THEN 'YYYY/MM/DD'
        WHEN signup_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] *' THEN 'YYYY-MM-DD hh:mm:ss'
        ELSE 'OTHER'
    END AS date_format,
    COUNT(*) AS n
FROM vw_raw_customers
GROUP BY date_format
ORDER BY n DESC;

-- =============================================================================
-- ORDERS
-- =============================================================================

-- O1. Invalid amounts: NULL, zero and negative totals (system errors)
SELECT
    SUM(total_amount IS NULL) AS null_amount,
    SUM(total_amount = 0)     AS zero_amount,
    SUM(total_amount < 0)     AS negative_amount,
    MIN(total_amount)         AS min_amount,
    MAX(total_amount)         AS max_amount
FROM vw_raw_orders;

-- O2. Exact duplicate order rows (same order_id loaded more than once)
SELECT order_id, COUNT(*) AS copies
FROM vw_raw_orders
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY copies DESC, order_id;

-- O3. Currency codes: NULLs, inconsistent case / whitespace
SELECT
    currency                       AS raw_currency,
    UPPER(TRIM(currency))          AS normalized_currency,
    COUNT(*)                       AS n
FROM vw_raw_orders
GROUP BY currency
ORDER BY n DESC;

-- O4. Non-USD orders with no exchange rate for their order_date
--     (these will fall back to USD per the business rule — worth flagging)
SELECT
    UPPER(TRIM(o.currency)) AS currency,
    COUNT(*)                AS orders_without_rate
FROM vw_raw_orders AS o
LEFT JOIN vw_exchange_rates AS r
       ON UPPER(TRIM(r.currency)) = UPPER(TRIM(o.currency))
      AND DATE(r.date)            = DATE(REPLACE(o.order_date, '/', '-'))
WHERE o.currency IS NOT NULL
  AND UPPER(TRIM(o.currency)) <> 'USD'
  AND r.rate_to_usd IS NULL
GROUP BY UPPER(TRIM(o.currency))
ORDER BY orders_without_rate DESC;

-- O5. Orphan orders: customer_id not present in the customer view
SELECT COUNT(*) AS orphan_orders
FROM vw_raw_orders AS o
WHERE NOT EXISTS (
    SELECT 1 FROM vw_raw_customers AS c WHERE c.customer_id = o.customer_id
);

-- O6. order_date format variety (same check as C4)
SELECT
    CASE
        WHEN order_date IS NULL                                 THEN 'NULL'
        WHEN order_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'   THEN 'YYYY-MM-DD'
        WHEN order_date GLOB '[0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9]'   THEN 'YYYY/MM/DD'
        WHEN order_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] *' THEN 'YYYY-MM-DD hh:mm:ss'
        ELSE 'OTHER'
    END AS date_format,
    COUNT(*) AS n
FROM vw_raw_orders
GROUP BY date_format
ORDER BY n DESC;

-- O7. Orders with no order_date (cannot be matched to an exchange rate)
SELECT order_id, customer_id, total_amount, currency, status
FROM vw_raw_orders
WHERE order_date IS NULL OR TRIM(order_date) = '';

-- O8. Order status values (which statuses should count towards CLV?)
SELECT status, COUNT(*) AS n, SUM(total_amount <= 0) AS non_positive_amounts
FROM vw_raw_orders
GROUP BY status
ORDER BY n DESC;

-- =============================================================================
-- EXCHANGE RATES
-- =============================================================================

-- R1. Rate coverage per currency vs. the order date range.
--     Orders dated after a currency's last rate cannot be converted.
SELECT
    r.currency,
    MIN(r.date)  AS first_rate_date,
    MAX(r.date)  AS last_rate_date,
    COUNT(*)     AS rate_rows,
    (SELECT MAX(order_date) FROM vw_raw_orders) AS last_order_date
FROM vw_exchange_rates AS r
GROUP BY r.currency
ORDER BY r.currency;
