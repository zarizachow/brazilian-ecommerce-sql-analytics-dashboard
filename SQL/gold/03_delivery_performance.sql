-- Delivery time + on-time performance
CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE TABLE gold.delivery_performance AS
SELECT
  DATE_TRUNC('month', order_purchase_ts) AS month,
  COUNT(*) AS orders,
  AVG(DATE_DIFF('day', order_purchase_ts, order_delivered_customer_ts)) AS avg_days_to_deliver,
  AVG(DATE_DIFF('day', order_purchase_ts, order_estimated_delivery_ts)) AS avg_days_estimated,
  AVG(
    CASE
      WHEN order_delivered_customer_ts IS NULL OR order_estimated_delivery_ts IS NULL THEN NULL
      WHEN order_delivered_customer_ts <= order_estimated_delivery_ts THEN 1
      ELSE 0
    END
  ) AS on_time_rate
FROM silver.orders
WHERE order_status = 'delivered'
GROUP BY 1
ORDER BY 1;