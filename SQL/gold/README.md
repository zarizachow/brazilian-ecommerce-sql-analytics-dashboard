# Gold Layer

The **Gold layer** contains dashboard-ready analytical tables derived from the cleaned **Silver layer**.  
These tables aggregate and transform transactional data into metrics that are easy to use for **business intelligence dashboards and reporting**.

Each table in this folder is created through a dedicated SQL script and stored in the `gold` schema of the DuckDB database.

---

## Gold Tables

### 1. `gold.monthly_revenue`
**Purpose:** Analyze revenue trends over time.

**Columns**
- `month` – Month of purchase
- `orders` – Number of orders
- `revenue` – Total revenue from payments
- `avg_order_value` – Average revenue per order

**Use Case**
- Revenue growth analysis
- Marketplace performance tracking
- Time series visualizations

---

### 2. `gold.category_revenue`
**Purpose:** Evaluate product category performance.

**Columns**
- `category` – Product category (translated to English when available)
- `orders` – Number of orders containing the category
- `items` – Total number of items sold
- `revenue_allocated` – Revenue allocated to the category

**Use Case**
- Top-performing product categories
- Category contribution to overall revenue
- Product mix analysis

---

### 3. `gold.delivery_performance`
**Purpose:** Measure logistics efficiency and delivery reliability.

**Columns**
- `month` – Order purchase month
- `orders` – Total delivered orders
- `avg_days_to_deliver` – Average delivery time
- `avg_days_estimated` – Average estimated delivery time
- `on_time_rate` – Percentage of orders delivered on or before estimated date

**Use Case**
- Logistics performance monitoring
- Delivery time trend analysis
- Operational efficiency insights

---

### 4. `gold.customer_repeat`
**Purpose:** Analyze customer retention and repeat purchase behavior.

**Columns**
- `cohort_month` – Month of the customer's first purchase
- `customers` – Number of customers in the cohort
- `avg_orders_per_customer` – Average orders per customer
- `repeat_customer_rate` – Percentage of customers with multiple purchases

**Use Case**
- Customer retention analysis
- Cohort analysis
- Repeat purchase behavior insights

---

### 5. `gold.review_summary`
**Purpose:** Understand customer satisfaction and its relationship with delivery performance.

**Columns**
- `month` – Order purchase month
- `reviewed_orders` – Number of orders with reviews
- `avg_review_score` – Average review rating
- `avg_days_to_deliver` – Average delivery time
- `on_time_rate` – Percentage of orders delivered on time

**Use Case**
- Customer satisfaction tracking
- Impact of delivery speed on reviews
- Service quality monitoring

---

## Purpose of the Gold Layer

The Gold layer represents the **final stage of the data pipeline**:
1. Raw Data (CSV files)  
2. Bronze Layer – raw data ingestion  
3. Silver Layer – cleaned and structured tables  
4. Gold Layer – analytics and dashboard-ready tables

These tables are designed to build **Power BI or Tableau dashboards**, enabling fast and reliable business insights.