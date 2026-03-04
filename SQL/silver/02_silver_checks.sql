-- Silver Layer: Quality checks (keys, nulls, duplicates)

-- Row counts
SELECT 'orders' AS table_name, COUNT(*) AS row_count FROM silver.orders
UNION ALL SELECT 'customers', COUNT(*) FROM silver.customers
UNION ALL SELECT 'order_items', COUNT(*) FROM silver.order_items
UNION ALL SELECT 'payments', COUNT(*) FROM silver.payments
UNION ALL SELECT 'reviews', COUNT(*) FROM silver.reviews
UNION ALL SELECT 'products', COUNT(*) FROM silver.products
UNION ALL SELECT 'sellers', COUNT(*) FROM silver.sellers
UNION ALL SELECT 'category_translation', COUNT(*) FROM silver.category_translation
UNION ALL SELECT 'geolocation', COUNT(*) FROM silver.geolocation
ORDER BY row_count DESC;

-- Primary key uniqueness checks (should be 0 duplicates)
SELECT COUNT(*) - COUNT(DISTINCT order_id) AS dup_order_id
FROM silver.orders;

SELECT COUNT(*) - COUNT(DISTINCT customer_id) AS dup_customer_id
FROM silver.customers;

SELECT COUNT(*) - COUNT(DISTINCT review_id) AS dup_review_id
FROM silver.reviews;

-- order_items key is (order_id, order_item_id)
SELECT COUNT(*) - COUNT(DISTINCT order_id || '-' || order_item_id) AS dup_order_item_key
FROM silver.order_items;

-- Null checks for critical join keys (should be 0)
SELECT
  SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_id,
  SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_id
FROM silver.orders;

SELECT
  SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_id,
  SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS null_product_id,
  SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END) AS null_seller_id
FROM silver.order_items;

-- 4) Date sanity: purchase timestamp should exist for most orders
SELECT
  SUM(CASE WHEN order_purchase_ts IS NULL THEN 1 ELSE 0 END) AS null_purchase_ts,
  COUNT(*) AS total_orders
FROM silver.orders;

-- Quick join sanity (should not explode unexpectedly)
SELECT COUNT(*) AS joined_rows
FROM silver.orders o
JOIN silver.order_items i USING (order_id);