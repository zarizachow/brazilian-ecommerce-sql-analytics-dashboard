# Silver Layer

The **Silver layer** contains cleaned, standardized, and join-ready tables derived from the raw Bronze layer.  
In this stage, the raw datasets are transformed into a consistent structure that supports reliable joins and analytics.

Key transformations performed in this layer include:
- Standardizing column names
- Converting date and timestamp fields
- Selecting relevant columns
- Ensuring primary keys and join keys are clean
- Removing duplicate records where necessary

The Silver layer acts as the **structured foundation** for the Gold analytics tables.

---

## Silver Tables

### 1. `silver.orders`
**Purpose:** Cleaned order-level dataset with standardized timestamps.

**Columns**
- `order_id`
- `customer_id`
- `order_status`
- `order_purchase_ts`
- `order_approved_ts`
- `order_delivered_carrier_ts`
- `order_delivered_customer_ts`
- `order_estimated_delivery_ts`

**Source Table**
- `bronze.orders`

---

### 2. `silver.customers`
**Purpose:** Clean customer dataset for linking orders to unique customers.

**Columns**
- `customer_id`
- `customer_unique_id`
- `customer_zip_code_prefix`
- `customer_city`
- `customer_state`

**Source Table**
- `bronze.customers`

---

### 3. `silver.order_items`
**Purpose:** Clean item-level dataset for each order.

**Columns**
- `order_id`
- `order_item_id`
- `product_id`
- `seller_id`
- `shipping_limit_ts`
- `price`
- `freight_value`

**Source Table**
- `bronze.order_items`

---

### 4. `silver.payments`
**Purpose:** Payment transactions associated with each order.

**Columns**
- `order_id`
- `payment_sequential`
- `payment_type`
- `payment_installments`
- `payment_value`

**Source Table**
- `bronze.payments`

---

### 5. `silver.reviews`
**Purpose:** Raw review data with timestamps converted to proper datetime format.

**Columns**
- `review_id`
- `order_id`
- `review_score`
- `review_comment_title`
- `review_comment_message`
- `review_creation_ts`
- `review_answer_ts`

**Source Table**
- `bronze.reviews`

---

### 6. `silver.products`
**Purpose:** Product catalog dataset used to connect items to categories.

**Columns**
- `product_id`
- `product_category_name`
- `product_name_lenght`
- `product_description_lenght`
- `product_photos_qty`
- `product_weight_g`
- `product_length_cm`
- `product_height_cm`
- `product_width_cm`

**Source Table**
- `bronze.products`

---

### 7. `silver.sellers`
**Purpose:** Seller location and identification dataset.

**Columns**
- `seller_id`
- `seller_zip_code_prefix`
- `seller_city`
- `seller_state`

**Source Table**
- `bronze.sellers`

---

### 8. `silver.category_translation`
**Purpose:** Translate Portuguese product categories into English.

**Columns**
- `product_category_name`
- `product_category_name_english`

**Source Table**
- `bronze.category_translation`

---

### 9. `silver.geolocation`
**Purpose:** Raw geolocation dataset for Brazilian zip codes.  
This dataset is preserved for potential aggregation or geographic analysis in later stages.

**Source Table**
- `bronze.geolocation`

---

### 10. `silver.reviews_dedup`
**Purpose:** Deduplicated version of the reviews table where only one record per `review_id` is retained.

Duplicates are resolved by keeping the most recent review record based on the review answer timestamp.

**Source Table**
- `silver.reviews`

---

## SQL Scripts

### `01_silver_build.sql`
Creates the Silver schema and builds cleaned tables from the Bronze layer.  
Key transformations include timestamp conversion and column standardization.

### `02_silver_checks.sql`
Performs quality checks on the Silver tables, including:

- Row count validation  
- Primary key uniqueness checks  
- Null checks on critical join keys  
- Timestamp sanity checks  
- Join integrity validation

### `03_silver_dedup.sql`
Creates a deduplicated version of the reviews table to ensure each `review_id` appears only once.

### `04_silver_dedup_check.sql`
Validates that duplicates have been successfully removed from `silver.reviews_dedup`.

---

## Purpose of the Silver Layer

The Silver layer ensures the data is **clean, standardized, and reliable for analytics** before building aggregated metrics.

**Data Pipeline**

1. Raw Data (CSV files)  
2. Bronze Layer – raw data ingestion  
3. Silver Layer – cleaned and structured tables  
4. Gold Layer – analytics and dashboard-ready tables