# Bronze Layer

The **Bronze layer** represents the first stage of the data pipeline.  
In this layer, the raw Olist CSV datasets are ingested directly into DuckDB tables with **minimal or no transformation**. The goal is to preserve the original structure of the data while making it accessible for downstream processing.

All tables in this layer mirror the raw data files and serve as the foundation for the **Silver layer**, where data cleaning and standardization take place.

---

## Bronze Tables

### 1. `bronze.customers`
**Purpose:** Store raw customer information.

**Columns (examples)**
- `customer_id`
- `customer_unique_id`
- `customer_zip_code_prefix`
- `customer_city`
- `customer_state`

**Source File**
- `olist_customers_dataset.csv`

---

### 2. `bronze.orders`
**Purpose:** Store raw order-level transaction data.

**Columns (examples)**
- `order_id`
- `customer_id`
- `order_status`
- `order_purchase_timestamp`
- `order_approved_at`
- `order_delivered_carrier_date`
- `order_delivered_customer_date`
- `order_estimated_delivery_date`

**Source File**
- `olist_orders_dataset.csv`

---

### 3. `bronze.order_items`
**Purpose:** Store individual items belonging to each order.

**Columns (examples)**
- `order_id`
- `order_item_id`
- `product_id`
- `seller_id`
- `shipping_limit_date`
- `price`
- `freight_value`

**Source File**
- `olist_order_items_dataset.csv`

---

### 4. `bronze.products`
**Purpose:** Store product catalog information.

**Columns (examples)**
- `product_id`
- `product_category_name`
- `product_name_lenght`
- `product_description_lenght`
- `product_photos_qty`
- `product_weight_g`
- `product_length_cm`
- `product_height_cm`
- `product_width_cm`

**Source File**
- `olist_products_dataset.csv`

---

### 5. `bronze.sellers`
**Purpose:** Store seller information and locations.

**Columns (examples)**
- `seller_id`
- `seller_zip_code_prefix`
- `seller_city`
- `seller_state`

**Source File**
- `olist_sellers_dataset.csv`

---

### 6. `bronze.payments`
**Purpose:** Store payment transaction details for each order.

**Columns (examples)**
- `order_id`
- `payment_sequential`
- `payment_type`
- `payment_installments`
- `payment_value`

**Source File**
- `olist_order_payments_dataset.csv`

---

### 7. `bronze.reviews`
**Purpose:** Store raw customer review data.

**Columns (examples)**
- `review_id`
- `order_id`
- `review_score`
- `review_comment_title`
- `review_comment_message`
- `review_creation_date`
- `review_answer_timestamp`

**Source File**
- `olist_order_reviews_dataset.csv`

---

### 8. `bronze.category_translation`
**Purpose:** Translate Portuguese product categories into English.

**Columns**
- `product_category_name`
- `product_category_name_english`

**Source File**
- `product_category_name_translation.csv`

---

### 9. `bronze.geolocation`
**Purpose:** Store geolocation information for Brazilian zip codes.

**Columns (examples)**
- `geolocation_zip_code_prefix`
- `geolocation_lat`
- `geolocation_lng`
- `geolocation_city`
- `geolocation_state`

**Source File**
- `olist_geolocation_dataset.csv`

---

## SQL Scripts

### `01_bronze_load.sql`
Loads raw CSV files into DuckDB tables inside the `bronze` schema using `read_csv_auto()`.  
This script ingests the raw data exactly as it appears in the source files.

### `02_bronze_checks.sql`
Performs quick sanity checks on the Bronze tables, including:
- Row count verification for each table
- Sample queries to inspect key columns
- Basic validation that ingestion succeeded

---

## Purpose of the Bronze Layer

The Bronze layer preserves **raw source data** and acts as the entry point for the pipeline.

1. Raw Data (CSV files)  
2. Bronze Layer – raw data ingestion  
3. Silver Layer – cleaned and structured tables  
4. Gold Layer – analytics and dashboard-ready tables

By maintaining the raw datasets in this layer, downstream transformations can always be reproduced from the original source data.