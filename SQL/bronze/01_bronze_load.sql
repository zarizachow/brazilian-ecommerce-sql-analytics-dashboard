-- Purpose: Load raw Olist CSVs into Bronze tables

-- Recommended: keep bronze in its own schema
CREATE SCHEMA IF NOT EXISTS bronze;

-- Customers
CREATE OR REPLACE TABLE bronze.customers AS
SELECT * FROM read_csv_auto('Data/Raw/olist_customers_dataset.csv');

-- Orders
CREATE OR REPLACE TABLE bronze.orders AS
SELECT * FROM read_csv_auto('Data/Raw/olist_orders_dataset.csv');

-- Order Items
CREATE OR REPLACE TABLE bronze.order_items AS
SELECT * FROM read_csv_auto('Data/Raw/olist_order_items_dataset.csv');

-- Products
CREATE OR REPLACE TABLE bronze.products AS
SELECT * FROM read_csv_auto('Data/Raw/olist_products_dataset.csv');

-- Sellers
CREATE OR REPLACE TABLE bronze.sellers AS
SELECT * FROM read_csv_auto('Data/Raw/olist_sellers_dataset.csv');

-- Payments
CREATE OR REPLACE TABLE bronze.payments AS
SELECT * FROM read_csv_auto('Data/Raw/olist_order_payments_dataset.csv');

-- Reviews
CREATE OR REPLACE TABLE bronze.reviews AS
SELECT * FROM read_csv_auto('Data/Raw/olist_order_reviews_dataset.csv');

-- Category translation
CREATE OR REPLACE TABLE bronze.category_translation AS
SELECT * FROM read_csv_auto('Data/Raw/product_category_name_translation.csv');

-- Geolocation (this file is large)
CREATE OR REPLACE TABLE bronze.geolocation AS
SELECT * FROM read_csv_auto('Data/Raw/olist_geolocation_dataset.csv');