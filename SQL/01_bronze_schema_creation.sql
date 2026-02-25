-- ============================================================================
-- BRONZE LAYER: Schema Creation (DuckDB)
-- ============================================================================
-- Purpose: Create raw tables to ingest data from Olist Brazilian E-Commerce dataset
-- Layer: Bronze (Raw/Landing Zone)
-- Database: DuckDB
-- Author: Zariza Chowdhury
-- Date: 2026-02-25
-- ============================================================================

-- ============================================================================
-- TABLE 1: Bronze Customers
-- ============================================================================
CREATE OR REPLACE TABLE bronze_customers (
    customer_id VARCHAR PRIMARY KEY,
    customer_unique_id VARCHAR NOT NULL,
    customer_zip_code_prefix VARCHAR,
    customer_city VARCHAR,
    customer_state VARCHAR,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_customer_unique_id ON bronze_customers(customer_unique_id);
CREATE INDEX idx_customer_state ON bronze_customers(customer_state);

-- ============================================================================
-- TABLE 2: Bronze Orders
-- ============================================================================
CREATE OR REPLACE TABLE bronze_orders (
    order_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR NOT NULL,
    order_status VARCHAR,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_order_customer_id ON bronze_orders(customer_id);
CREATE INDEX idx_order_status ON bronze_orders(order_status);
CREATE INDEX idx_order_purchase_timestamp ON bronze_orders(order_purchase_timestamp);

-- ============================================================================
-- TABLE 3: Bronze Order Items
-- ============================================================================
CREATE OR REPLACE TABLE bronze_order_items (
    order_id VARCHAR NOT NULL,
    order_item_id INTEGER NOT NULL,
    product_id VARCHAR NOT NULL,
    seller_id VARCHAR NOT NULL,
    shipping_limit_date TIMESTAMP,
    price DECIMAL(10,2),
    freight_value DECIMAL(10,2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id, order_item_id)
);

CREATE INDEX idx_order_items_product_id ON bronze_order_items(product_id);
CREATE INDEX idx_order_items_seller_id ON bronze_order_items(seller_id);

-- ============================================================================
-- TABLE 4: Bronze Products
-- ============================================================================
CREATE OR REPLACE TABLE bronze_products (
    product_id VARCHAR PRIMARY KEY,
    product_category_name VARCHAR,
    product_name_length INTEGER,
    product_description_length INTEGER,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_product_category ON bronze_products(product_category_name);

-- ============================================================================
-- TABLE 5: Bronze Sellers
-- ============================================================================
CREATE OR REPLACE TABLE bronze_sellers (
    seller_id VARCHAR PRIMARY KEY,
    seller_zip_code_prefix VARCHAR,
    seller_city VARCHAR,
    seller_state VARCHAR,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_seller_state ON bronze_sellers(seller_state);

-- ============================================================================
-- TABLE 6: Bronze Order Payments
-- ============================================================================
CREATE OR REPLACE TABLE bronze_order_payments (
    order_id VARCHAR NOT NULL,
    payment_sequential INTEGER NOT NULL,
    payment_type VARCHAR,
    payment_installments INTEGER,
    payment_value DECIMAL(10,2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id, payment_sequential)
);

CREATE INDEX idx_payment_type ON bronze_order_payments(payment_type);

-- ============================================================================
-- TABLE 7: Bronze Order Reviews
-- ============================================================================
CREATE OR REPLACE TABLE bronze_order_reviews (
    review_id VARCHAR NOT NULL,
    order_id VARCHAR NOT NULL,
    review_score INTEGER,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (review_id, order_id)
);

CREATE INDEX idx_review_order_id ON bronze_order_reviews(order_id);
CREATE INDEX idx_review_score ON bronze_order_reviews(review_score);

-- ============================================================================
-- TABLE 8: Bronze Geolocation
-- ============================================================================
CREATE OR REPLACE TABLE bronze_geolocation (
    geolocation_zip_code_prefix VARCHAR NOT NULL,
    geolocation_lat DECIMAL(10,8),
    geolocation_lng DECIMAL(11,8),
    geolocation_city VARCHAR,
    geolocation_state VARCHAR,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_geolocation_zip ON bronze_geolocation(geolocation_zip_code_prefix);
CREATE INDEX idx_geolocation_state ON bronze_geolocation(geolocation_state);

-- ============================================================================
-- TABLE 9: Bronze Product Category Translation
-- ============================================================================
CREATE OR REPLACE TABLE bronze_product_category_translation (
    product_category_name VARCHAR PRIMARY KEY,
    product_category_name_english VARCHAR,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- Verification Queries
-- ============================================================================

-- Show all created tables
SHOW TABLES;

-- Display table information
SELECT 
    table_name,
    estimated_size
FROM duckdb_tables()
WHERE schema_name = 'main'
ORDER BY table_name;

-- ============================================================================
-- End of Bronze Schema Creation
-- ============================================================================
