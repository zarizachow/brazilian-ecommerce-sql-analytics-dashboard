-- Repeat purchase metrics by cohort month

CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE TABLE gold.customer_repeat AS

WITH customer_orders AS (
    SELECT
        c.customer_unique_id,
        o.order_id,
        o.order_purchase_ts,
        DATE_TRUNC('month', o.order_purchase_ts) AS order_month
    FROM silver.orders o
    JOIN silver.customers c USING (customer_id)
    WHERE o.order_status IN ('delivered','shipped','approved','invoiced')
),

customer_counts AS (
    SELECT
        customer_unique_id,
        COUNT(*) AS total_orders,
        MIN(order_purchase_ts) AS first_order_ts
    FROM customer_orders
    GROUP BY customer_unique_id
)

SELECT
    DATE_TRUNC('month', first_order_ts) AS cohort_month,
    COUNT(*) AS customers,
    AVG(total_orders) AS avg_orders_per_customer,
    SUM(CASE WHEN total_orders >= 2 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS repeat_customer_rate
FROM customer_counts
GROUP BY cohort_month
ORDER BY cohort_month;