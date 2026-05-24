-- Revenue by product category

CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE TABLE gold.category_revenue AS

WITH order_payments AS (
    SELECT
        order_id,
        SUM(payment_value) AS order_revenue
    FROM silver.payments
    GROUP BY order_id
),

item_value AS (
    SELECT
        order_id,
        product_id,
        (price + freight_value) AS item_gmv
    FROM silver.order_items
),

order_gmv AS (
    SELECT
        order_id,
        SUM(item_gmv) AS order_gmv
    FROM item_value
    GROUP BY order_id
),

items_with_weights AS (
    SELECT
        iv.order_id,
        iv.product_id,
        iv.item_gmv,
        og.order_gmv,
        op.order_revenue,
        (iv.item_gmv / og.order_gmv) * op.order_revenue AS allocated_revenue
    FROM item_value iv
    JOIN order_gmv og USING (order_id)
    JOIN order_payments op USING (order_id)
)

SELECT
    COALESCE(ct.product_category_name_english, p.product_category_name) AS category,
    COUNT(DISTINCT iww.order_id) AS orders,
    COUNT(*) AS items,
    SUM(iww.allocated_revenue) AS revenue_allocated
FROM items_with_weights iww
JOIN silver.products p USING (product_id)
LEFT JOIN silver.category_translation ct
    ON p.product_category_name = ct.product_category_name
GROUP BY category
ORDER BY revenue_allocated DESC;