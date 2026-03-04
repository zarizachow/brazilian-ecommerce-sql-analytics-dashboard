-- Silver Layer: Clean + typed + join-ready tables

CREATE SCHEMA IF NOT EXISTS silver;

-- Orders
CREATE OR REPLACE TABLE silver.orders AS
SELECT
  order_id,
  customer_id,
  order_status,
  CAST(order_purchase_timestamp AS TIMESTAMP)      AS order_purchase_ts,
  CAST(order_approved_at AS TIMESTAMP)             AS order_approved_ts,
  CAST(order_delivered_carrier_date AS TIMESTAMP)  AS order_delivered_carrier_ts,
  CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_ts,
  CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_ts
FROM bronze.orders;

-- Customers
CREATE OR REPLACE TABLE silver.customers AS
SELECT
  customer_id,
  customer_unique_id,
  customer_zip_code_prefix,
  customer_city
  customer_state
FROM bronze.customers;

-- Order items
CREATE OR REPLACE TABLE silver.order_items AS
SELECT
  order_id,
  order_item_id,
  product_id,
  seller_id,
  CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_ts,
  price,
  freight_value
FROM bronze.order_items;

-- Payments
CREATE OR REPLACE TABLE silver.payments AS
SELECT
  order_id,
  payment_sequential,
  payment_type,
  payment_installments,
  payment_value
FROM bronze.payments;

-- Reviews
CREATE OR REPLACE TABLE silver.reviews AS
SELECT
  review_id,
  order_id,
  review_score,
  review_comment_title,
  review_comment_message,
  CAST(review_creation_date AS TIMESTAMP) AS review_creation_ts,
  CAST(review_answer_timestamp AS TIMESTAMP) AS review_answer_ts
FROM bronze.reviews;

-- Products
CREATE OR REPLACE TABLE silver.products AS
SELECT
  product_id,
  product_category_name,
  product_name_lenght,
  product_description_lenght,
  product_photos_qty,
  product_weight_g,
  product_length_cm,
  product_height_cm,
  product_width_cm
FROM bronze.products;

-- Sellers
CREATE OR REPLACE TABLE silver.sellers AS
SELECT
  seller_id,
  seller_zip_code_prefix,
  seller_city,
  seller_state
FROM bronze.sellers;

-- Category translation
CREATE OR REPLACE TABLE silver.category_translation AS
SELECT
  product_category_name,
  product_category_name_english
FROM bronze.category_translation;

-- Geolocation (keep as-is for now; we’ll aggregate later)
CREATE OR REPLACE TABLE silver.geolocation AS
SELECT *
FROM bronze.geolocation;