-- ============================================================================
-- BRONZE LAYER: Data Quality Checks (DuckDB)
-- ============================================================================
-- Purpose: Validate data integrity and quality in Bronze layer
-- Layer: Bronze (Raw/Landing Zone)
-- Database: DuckDB
-- Author: Zariza Chowdhury
-- Date: 2026-02-25
-- ============================================================================

-- ============================================================================
-- CHECK 1: Record Counts Summary
-- ============================================================================
SELECT 'RECORD COUNTS SUMMARY' AS check_type;

SELECT 
    'bronze_customers' AS table_name,
    COUNT(*) AS total_records,
    COUNT(DISTINCT customer_id) AS unique_keys
FROM bronze_customers
UNION ALL
SELECT 
    'bronze_orders',
    COUNT(*),
    COUNT(DISTINCT order_id)
FROM bronze_orders
UNION ALL
SELECT 
    'bronze_order_items',
    COUNT(*),
    COUNT(DISTINCT CONCAT(order_id, '-', order_item_id))
FROM bronze_order_items
UNION ALL
SELECT 
    'bronze_products',
    COUNT(*),
    COUNT(DISTINCT product_id)
FROM bronze_products
UNION ALL
SELECT 
    'bronze_sellers',
    COUNT(*),
    COUNT(DISTINCT seller_id)
FROM bronze_sellers
UNION ALL
SELECT 
    'bronze_order_payments',
    COUNT(*),
    COUNT(DISTINCT CONCAT(order_id, '-', payment_sequential))
FROM bronze_order_payments
UNION ALL
SELECT 
    'bronze_order_reviews',
    COUNT(*),
    COUNT(DISTINCT CONCAT(review_id, '-', order_id))
FROM bronze_order_reviews
UNION ALL
SELECT 
    'bronze_geolocation',
    COUNT(*),
    COUNT(DISTINCT geolocation_zip_code_prefix)
FROM bronze_geolocation
UNION ALL
SELECT 
    'bronze_product_category_translation',
    COUNT(*),
    COUNT(DISTINCT product_category_name)
FROM bronze_product_category_translation
ORDER BY table_name;

-- ============================================================================
-- CHECK 2: NULL Values Analysis
-- ============================================================================
SELECT 'NULL VALUES ANALYSIS' AS check_type;

-- Customers nulls
SELECT 
    'bronze_customers' AS table_name,
    COUNT(*) FILTER (WHERE customer_id IS NULL) AS null_customer_id,
    COUNT(*) FILTER (WHERE customer_unique_id IS NULL) AS null_unique_id,
    COUNT(*) FILTER (WHERE customer_zip_code_prefix IS NULL) AS null_zip_code,
    COUNT(*) FILTER (WHERE customer_city IS NULL) AS null_city,
    COUNT(*) FILTER (WHERE customer_state IS NULL) AS null_state
FROM bronze_customers;

-- Orders nulls
SELECT 
    'bronze_orders' AS table_name,
    COUNT(*) FILTER (WHERE order_id IS NULL) AS null_order_id,
    COUNT(*) FILTER (WHERE customer_id IS NULL) AS null_customer_id,
    COUNT(*) FILTER (WHERE order_status IS NULL) AS null_status,
    COUNT(*) FILTER (WHERE order_purchase_timestamp IS NULL) AS null_purchase_date,
    COUNT(*) FILTER (WHERE order_delivered_customer_date IS NULL) AS null_delivery_date
FROM bronze_orders;

-- Order Items nulls
SELECT 
    'bronze_order_items' AS table_name,
    COUNT(*) FILTER (WHERE order_id IS NULL) AS null_order_id,
    COUNT(*) FILTER (WHERE product_id IS NULL) AS null_product_id,
    COUNT(*) FILTER (WHERE seller_id IS NULL) AS null_seller_id,
    COUNT(*) FILTER (WHERE price IS NULL) AS null_price,
    COUNT(*) FILTER (WHERE freight_value IS NULL) AS null_freight
FROM bronze_order_items;

-- Products nulls
SELECT 
    'bronze_products' AS table_name,
    COUNT(*) FILTER (WHERE product_id IS NULL) AS null_product_id,
    COUNT(*) FILTER (WHERE product_category_name IS NULL) AS null_category,
    COUNT(*) FILTER (WHERE product_weight_g IS NULL) AS null_weight,
    COUNT(*) FILTER (WHERE product_length_cm IS NULL) AS null_length
FROM bronze_products;

-- ============================================================================
-- CHECK 3: Duplicate Records
-- ============================================================================
SELECT 'DUPLICATE RECORDS CHECK' AS check_type;

-- Check duplicate customer IDs
SELECT 
    'bronze_customers' AS table_name,
    'customer_id' AS key_column,
    COUNT(*) - COUNT(DISTINCT customer_id) AS duplicate_count
FROM bronze_customers;

-- Check duplicate order IDs
SELECT 
    'bronze_orders' AS table_name,
    'order_id' AS key_column,
    COUNT(*) - COUNT(DISTINCT order_id) AS duplicate_count
FROM bronze_orders;

-- Check duplicate product IDs
SELECT 
    'bronze_products' AS table_name,
    'product_id' AS key_column,
    COUNT(*) - COUNT(DISTINCT product_id) AS duplicate_count
FROM bronze_products;

-- Check duplicate seller IDs
SELECT 
    'bronze_sellers' AS table_name,
    'seller_id' AS key_column,
    COUNT(*) - COUNT(DISTINCT seller_id) AS duplicate_count
FROM bronze_sellers;

-- ============================================================================
-- CHECK 4: Referential Integrity
-- ============================================================================
SELECT 'REFERENTIAL INTEGRITY CHECK' AS check_type;

-- Orders without customers
SELECT 
    'Orders without matching customers' AS integrity_check,
    COUNT(*) AS violation_count
FROM bronze_orders o
LEFT JOIN bronze_customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Order items without orders
SELECT 
    'Order items without matching orders' AS integrity_check,
    COUNT(*) AS violation_count
FROM bronze_order_items oi
LEFT JOIN bronze_orders o ON oi.order_id = o.order_id
WHERE o.order_id IS NULL;

-- Order items without products
SELECT 
    'Order items without matching products' AS integrity_check,
    COUNT(*) AS violation_count
FROM bronze_order_items oi
LEFT JOIN bronze_products p ON oi.product_id = p.product_id
WHERE p.product_id IS NULL;

-- Order items without sellers
SELECT 
    'Order items without matching sellers' AS integrity_check,
    COUNT(*) AS violation_count
FROM bronze_order_items oi
LEFT JOIN bronze_sellers s ON oi.seller_id = s.seller_id
WHERE s.seller_id IS NULL;

-- Payments without orders
SELECT 
    'Payments without matching orders' AS integrity_check,
    COUNT(*) AS violation_count
FROM bronze_order_payments op
LEFT JOIN bronze_orders o ON op.order_id = o.order_id
WHERE o.order_id IS NULL;

-- Reviews without orders
SELECT 
    'Reviews without matching orders' AS integrity_check,
    COUNT(*) AS violation_count
FROM bronze_order_reviews r
LEFT JOIN bronze_orders o ON r.order_id = o.order_id
WHERE o.order_id IS NULL;

-- ============================================================================
-- CHECK 5: Data Range Validation
-- ============================================================================
SELECT 'DATA RANGE VALIDATION' AS check_type;

-- Order date ranges
SELECT 
    'Order Dates' AS validation_type,
    MIN(order_purchase_timestamp) AS min_date,
    MAX(order_purchase_timestamp) AS max_date,
    COUNT(DISTINCT DATE_TRUNC('day', order_purchase_timestamp)) AS unique_days
FROM bronze_orders
WHERE order_purchase_timestamp IS NOT NULL;

-- Price ranges
SELECT 
    'Order Item Prices' AS validation_type,
    MIN(price) AS min_price,
    MAX(price) AS max_price,
    ROUND(AVG(price), 2) AS avg_price,
    COUNT(*) AS total_items
FROM bronze_order_items
WHERE price IS NOT NULL;

-- Review score ranges
SELECT 
    'Review Scores' AS validation_type,
    MIN(review_score) AS min_score,
    MAX(review_score) AS max_score,
    ROUND(AVG(review_score), 2) AS avg_score,
    COUNT(*) AS total_reviews
FROM bronze_order_reviews
WHERE review_score IS NOT NULL;

-- ============================================================================
-- CHECK 6: Order Status Distribution
-- ============================================================================
SELECT 'ORDER STATUS DISTRIBUTION' AS check_type;

SELECT 
    order_status,
    COUNT(*) AS order_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM bronze_orders
WHERE order_status IS NOT NULL
GROUP BY order_status
ORDER BY order_count DESC;

-- ============================================================================
-- CHECK 7: Geographic Coverage
-- ============================================================================
SELECT 'GEOGRAPHIC COVERAGE - TOP 10 CUSTOMER STATES' AS check_type;

SELECT 
    customer_state,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM bronze_customers
WHERE customer_state IS NOT NULL
GROUP BY customer_state
ORDER BY customer_count DESC
LIMIT 10;

SELECT 'GEOGRAPHIC COVERAGE - TOP 10 SELLER STATES' AS check_type;

SELECT 
    seller_state,
    COUNT(*) AS seller_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM bronze_sellers
WHERE seller_state IS NOT NULL
GROUP BY seller_state
ORDER BY seller_count DESC
LIMIT 10;

-- ============================================================================
-- CHECK 8: Payment Type Distribution
-- ============================================================================
SELECT 'PAYMENT TYPE DISTRIBUTION' AS check_type;

SELECT 
    payment_type,
    COUNT(*) AS payment_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage,
    ROUND(AVG(payment_value), 2) AS avg_payment_value,
    ROUND(SUM(payment_value), 2) AS total_payment_value
FROM bronze_order_payments
WHERE payment_type IS NOT NULL
GROUP BY payment_type
ORDER BY payment_count DESC;

-- ============================================================================
-- CHECK 9: Product Category Coverage
-- ============================================================================
SELECT 'PRODUCT CATEGORY COVERAGE' AS check_type;

SELECT 
    COUNT(*) AS total_products,
    COUNT(DISTINCT product_category_name) AS unique_categories,
    COUNT(*) FILTER (WHERE product_category_name IS NULL) AS products_without_category,
    ROUND(COUNT(*) FILTER (WHERE product_category_name IS NULL) * 100.0 / COUNT(*), 2) AS null_percentage
FROM bronze_products;

SELECT 'TOP 10 PRODUCT CATEGORIES' AS check_type;

SELECT 
    p.product_category_name,
    COALESCE(t.product_category_name_english, 'Unknown') AS category_english,
    COUNT(*) AS product_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM bronze_products p
LEFT JOIN bronze_product_category_translation t 
    ON p.product_category_name = t.product_category_name
WHERE p.product_category_name IS NOT NULL
GROUP BY p.product_category_name, t.product_category_name_english
ORDER BY product_count DESC
LIMIT 10;

-- ============================================================================
-- CHECK 10: Delivery Performance Overview
-- ============================================================================
SELECT 'DELIVERY PERFORMANCE OVERVIEW' AS check_type;

SELECT 
    COUNT(*) AS total_orders,
    COUNT(*) FILTER (WHERE order_delivered_customer_date IS NOT NULL) AS delivered_orders,
    COUNT(*) FILTER (WHERE order_delivered_customer_date IS NULL) AS pending_orders,
    ROUND(COUNT(*) FILTER (WHERE order_delivered_customer_date IS NOT NULL) * 100.0 / COUNT(*), 2) AS delivery_rate,
    ROUND(AVG(DATE_DIFF('day', order_purchase_timestamp, order_delivered_customer_date)), 2) AS avg_delivery_days
FROM bronze_orders
WHERE order_status = 'delivered';

-- ============================================================================
-- CHECK 11: Data Freshness
-- ============================================================================
SELECT 'DATA FRESHNESS CHECK' AS check_type;

SELECT 
    table_name,
    min_load_time,
    max_load_time,
    DATE_DIFF('second', min_load_time, max_load_time) AS load_duration_seconds
FROM (
    SELECT 
        'bronze_customers' AS table_name,
        MIN(loaded_at) AS min_load_time,
        MAX(loaded_at) AS max_load_time
    FROM bronze_customers
    UNION ALL
    SELECT 
        'bronze_orders',
        MIN(loaded_at),
        MAX(loaded_at)
    FROM bronze_orders
    UNION ALL
    SELECT 
        'bronze_order_items',
        MIN(loaded_at),
        MAX(loaded_at)
    FROM bronze_order_items
)
ORDER BY table_name;

-- ============================================================================
-- CHECK 12: Overall Data Quality Score
-- ============================================================================
SELECT 'OVERALL DATA QUALITY SCORE' AS check_type;

WITH quality_metrics AS (
    SELECT 
        -- Completeness: % of non-null critical fields
        (COUNT(*) FILTER (WHERE customer_id IS NOT NULL) * 1.0 / COUNT(*)) AS customer_completeness,
        1 AS weight
    FROM bronze_customers
    UNION ALL
    SELECT 
        (COUNT(*) FILTER (WHERE order_id IS NOT NULL AND order_purchase_timestamp IS NOT NULL) * 1.0 / COUNT(*)),
        1
    FROM bronze_orders
    UNION ALL
    SELECT 
        (COUNT(*) FILTER (WHERE price IS NOT NULL) * 1.0 / COUNT(*)),
        1
    FROM bronze_order_items
)
SELECT 
    ROUND(AVG(customer_completeness) * 100, 2) AS data_quality_score_pct,
    CASE 
        WHEN AVG(customer_completeness) >= 0.95 THEN 'Excellent'
        WHEN AVG(customer_completeness) >= 0.85 THEN 'Good'
        WHEN AVG(customer_completeness) >= 0.70 THEN 'Fair'
        ELSE 'Poor'
    END AS quality_rating
FROM quality_metrics;

-- ============================================================================
-- End of Bronze Data Quality Checks
-- ============================================================================
