-- =============================================================================
-- clv_report.sql
-- Customer Lifetime Value report built on the cleaned analytics tables.
--   sqlite3 analytics.db < clv_report.sql
--
-- * Orders are aggregated per customer first, then joined once to
--   dim_customers (avoids aggregating a wide joined row set).
-- * LEFT JOIN keeps customers who have never placed a valid order (CLV = 0).
-- * fct_orders already excludes null/zero/negative totals and holds usd_amount,
--   so every row counted here is a valid USD-converted order.
-- * clv_rank uses RANK(): customers with equal CLV share a rank.
-- =============================================================================
WITH order_totals AS (
    SELECT
        customer_id,
        COUNT(order_id)  AS total_orders_placed,
        SUM(usd_amount)  AS lifetime_value_usd
    FROM fct_orders
    GROUP BY customer_id
)
SELECT
    RANK() OVER (ORDER BY COALESCE(o.lifetime_value_usd, 0) DESC) AS clv_rank,
    c.customer_id,
    c.full_name,
    COALESCE(o.total_orders_placed, 0)            AS total_orders_placed,
    ROUND(COALESCE(o.lifetime_value_usd, 0), 2)   AS lifetime_value_usd,
    strftime('%Y-%m', c.signup_date)              AS customer_cohort
FROM dim_customers AS c
LEFT JOIN order_totals AS o
       ON o.customer_id = c.customer_id
ORDER BY lifetime_value_usd DESC, c.customer_id;
