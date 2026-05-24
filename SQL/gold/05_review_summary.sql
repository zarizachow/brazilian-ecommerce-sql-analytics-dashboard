-- Gold: Review score trends + delivery relationship

CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE TABLE gold.review_summary AS

WITH delivered AS (
    SELECT
        order_id,
        DATE_TRUNC('month', order_purchase_ts) AS month,
        DATE_DIFF('day', order_purchase_ts, order_delivered_customer_ts) AS days_to_deliver,
        CASE
            WHEN order_delivered_customer_ts <= order_estimated_delivery_ts THEN 1
            ELSE 0
        END AS on_time_flag
    FROM silver.orders
    WHERE order_status = 'delivered'
)

SELECT
    d.month,
    COUNT(*) AS reviewed_orders,
    AVG(r.review_score) AS avg_review_score,
    AVG(d.days_to_deliver) AS avg_days_to_deliver,
    AVG(d.on_time_flag) AS on_time_rate
FROM delivered d
JOIN silver.reviews_dedup r USING (order_id)
GROUP BY d.month
ORDER BY d.month;