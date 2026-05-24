-- Monthly revenue + order counts
CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE TABLE gold.monthly_revenue AS
WITH order_payments AS (
  SELECT
    order_id,
    SUM(payment_value) AS order_revenue
  FROM silver.payments
  GROUP BY 1
)
SELECT
  DATE_TRUNC('month', o.order_purchase_ts) AS month,
  COUNT(DISTINCT o.order_id) AS orders,
  SUM(op.order_revenue) AS revenue,
  AVG(op.order_revenue) AS avg_order_value
FROM silver.orders o
JOIN order_payments op USING (order_id)
WHERE o.order_status IN ('delivered', 'shipped', 'approved', 'invoiced')
GROUP BY 1
ORDER BY 1;