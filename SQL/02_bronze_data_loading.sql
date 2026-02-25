-- ============================================================================
-- BRONZE LAYER: Data Loading (DuckDB)
-- ============================================================================
-- Purpose: Load raw CSV files into Bronze layer tables using DuckDB
-- Layer: Bronze (Raw/Landing Zone)
-- Database: DuckDB
-- Author: Zariza Chowdhury
-- Date: 2026-02-25
-- ============================================================================

-- ============================================================================
-- LOAD 1: Customers Data
-- ============================================================================
INSERT INTO bronze_customers (
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
)
SELECT 
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM read_csv_auto('../Data/Raw/olist_customers_dataset.csv', 
    header=true,
    nullstr='',
    ignore_errors=false
);

SELECT 'Customers loaded' AS status, COUNT(*) AS record_count FROM bronze_customers;

-- ============================================================================
-- LOAD 2: Orders Data
-- ============================================================================
INSERT INTO bronze_orders (
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date
)
SELECT 
    order_id,
    customer_id,
    order_status,
    TRY_CAST(order_purchase_timestamp AS TIMESTAMP),
    TRY_CAST(order_approved_at AS TIMESTAMP),
    TRY_CAST(order_delivered_carrier_date AS TIMESTAMP),
    TRY_CAST(order_delivered_customer_date AS TIMESTAMP),
    TRY_CAST(order_estimated_delivery_date AS TIMESTAMP)
FROM read_csv_auto('../Data/Raw/olist_orders_dataset.csv',
    header=true,
    nullstr='',
    ignore_errors=false
);

SELECT 'Orders loaded' AS status, COUNT(*) AS record_count FROM bronze_orders;

-- ============================================================================
-- LOAD 3: Order Items Data
-- ============================================================================
INSERT INTO bronze_order_items (
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value
)
SELECT 
    order_id,
    order_item_id,
    product_id,
    seller_id,
    TRY_CAST(shipping_limit_date AS TIMESTAMP),
    TRY_CAST(price AS DECIMAL(10,2)),
    TRY_CAST(freight_value AS DECIMAL(10,2))
FROM read_csv_auto('../Data/Raw/olist_order_items_dataset.csv',
    header=true,
    nullstr='',
    ignore_errors=false
);

SELECT 'Order Items loaded' AS status, COUNT(*) AS record_count FROM bronze_order_items;

-- ============================================================================
-- LOAD 4: Products Data
-- ============================================================================
INSERT INTO bronze_products (
    product_id,
    product_category_name,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
)
SELECT 
    product_id,
    product_category_name,
    TRY_CAST(product_name_lenght AS INTEGER),  -- Note: typo in original CSV
    TRY_CAST(product_description_lenght AS INTEGER),  -- Note: typo in original CSV
    TRY_CAST(product_photos_qty AS INTEGER),
    TRY_CAST(product_weight_g AS INTEGER),
    TRY_CAST(product_length_cm AS INTEGER),
    TRY_CAST(product_height_cm AS INTEGER),
    TRY_CAST(product_width_cm AS INTEGER)
FROM read_csv_auto('../Data/Raw/olist_products_dataset.csv',
    header=true,
    nullstr='',
    ignore_errors=false
);

SELECT 'Products loaded' AS status, COUNT(*) AS record_count FROM bronze_products;

-- ============================================================================
-- LOAD 5: Sellers Data
-- ============================================================================
INSERT INTO bronze_sellers (
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
)
SELECT 
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
FROM read_csv_auto('../Data/Raw/olist_sellers_dataset.csv',
    header=true,
    nullstr='',
    ignore_errors=false
);

SELECT 'Sellers loaded' AS status, COUNT(*) AS record_count FROM bronze_sellers;

-- ============================================================================
-- LOAD 6: Order Payments Data
-- ============================================================================
INSERT INTO bronze_order_payments (
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
)
SELECT 
    order_id,
    payment_sequential,
    payment_type,
    TRY_CAST(payment_installments AS INTEGER),
    TRY_CAST(payment_value AS DECIMAL(10,2))
FROM read_csv_auto('../Data/Raw/olist_order_payments_dataset.csv',
    header=true,
    nullstr='',
    ignore_errors=false
);

SELECT 'Order Payments loaded' AS status, COUNT(*) AS record_count FROM bronze_order_payments;

-- ============================================================================
-- LOAD 7: Order Reviews Data
-- ============================================================================
INSERT INTO bronze_order_reviews (
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
)
SELECT 
    review_id,
    order_id,
    TRY_CAST(review_score AS INTEGER),
    review_comment_title,
    review_comment_message,
    TRY_CAST(review_creation_date AS TIMESTAMP),
    TRY_CAST(review_answer_timestamp AS TIMESTAMP)
FROM read_csv_auto('../Data/Raw/olist_order_reviews_dataset.csv',
    header=true,
    nullstr='',
    ignore_errors=false
);

SELECT 'Order Reviews loaded' AS status, COUNT(*) AS record_count FROM bronze_order_reviews;

-- ============================================================================
-- LOAD 8: Geolocation Data
-- ============================================================================
INSERT INTO bronze_geolocation (
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state
)
SELECT 
    geolocation_zip_code_prefix,
    TRY_CAST(geolocation_lat AS DECIMAL(10,8)),
    TRY_CAST(geolocation_lng AS DECIMAL(11,8)),
    geolocation_city,
    geolocation_state
FROM read_csv_auto('../Data/Raw/olist_geolocation_dataset.csv',
    header=true,
    nullstr='',
    ignore_errors=false
);

SELECT 'Geolocation loaded' AS status, COUNT(*) AS record_count FROM bronze_geolocation;

-- ============================================================================
-- LOAD 9: Product Category Translation Data
-- ============================================================================
INSERT INTO bronze_product_category_translation (
    product_category_name,
    product_category_name_english
)
SELECT 
    "product_category_name",
    "product_category_name_english"
FROM read_csv_auto('../Data/Raw/product_category_name_translation.csv',
    header=true,
    nullstr='',
    ignore_errors=false
);

SELECT 'Product Category Translation loaded' AS status, COUNT(*) AS record_count 
FROM bronze_product_category_translation;

-- ============================================================================
-- Data Loading Summary
-- ============================================================================
SELECT 
    'bronze_customers' AS table_name,
    COUNT(*) AS row_count,
    MIN(loaded_at) AS first_load,
    MAX(loaded_at) AS last_load
FROM bronze_customers
UNION ALL
SELECT 
    'bronze_orders',
    COUNT(*),
    MIN(loaded_at),
    MAX(loaded_at)
FROM bronze_orders
UNION ALL
SELECT 
    'bronze_order_items',
    COUNT(*),
    MIN(loaded_at),
    MAX(loaded_at)
FROM bronze_order_items
UNION ALL
SELECT 
    'bronze_products',
    COUNT(*),
    MIN(loaded_at),
    MAX(loaded_at)
FROM bronze_products
UNION ALL
SELECT 
    'bronze_sellers',
    COUNT(*),
    MIN(loaded_at),
    MAX(loaded_at)
FROM bronze_sellers
UNION ALL
SELECT 
    'bronze_order_payments',
    COUNT(*),
    MIN(loaded_at),
    MAX(loaded_at)
FROM bronze_order_payments
UNION ALL
SELECT 
    'bronze_order_reviews',
    COUNT(*),
    MIN(loaded_at),
    MAX(loaded_at)
FROM bronze_order_reviews
UNION ALL
SELECT 
    'bronze_geolocation',
    COUNT(*),
    MIN(loaded_at),
    MAX(loaded_at)
FROM bronze_geolocation
UNION ALL
SELECT 
    'bronze_product_category_translation',
    COUNT(*),
    MIN(loaded_at),
    MAX(loaded_at)
FROM bronze_product_category_translation
ORDER BY table_name;

-- ============================================================================
-- Export database size and statistics
-- ============================================================================
SELECT 
    table_name,
    estimated_size AS size_bytes,
    ROUND(estimated_size / 1024.0 / 1024.0, 2) AS size_mb,
    column_count,
    index_count
FROM duckdb_tables()
WHERE schema_name = 'main'
ORDER BY estimated_size DESC;

-- ============================================================================
-- End of Bronze Data Loading
-- ============================================================================
