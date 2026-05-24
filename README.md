# Brazilian E-Commerce SQL Analytics Dashboard

> **Note:** This is a personal portfolio project demonstrating end-to-end SQL analytics and data pipeline design using the Brazilian Olist e-commerce dataset.

This project builds a structured data pipeline (Bronze → Silver → Gold) in DuckDB and prepares dashboard-ready analytical tables for business intelligence reporting. The analysis covers revenue trends, product category performance, delivery logistics, customer retention, and review satisfaction.

---

## Project Overview

The analysis is structured around five business questions:

1. How has monthly revenue and order volume changed over time?
2. Which product categories contribute the most to revenue?
3. How efficient is the delivery process, and what is the on-time rate?
4. What share of customers make repeat purchases, and how does this vary by cohort?
5. Is there a relationship between delivery speed and customer review scores?

---

## Repository Structure

```text
brazilian-ecommerce-sql-analytics-dashboard/
├── README.md
├── LICENSE
├── .gitignore
├── Data/
│   ├── README.md                          # Data source and download instructions
│   └── Raw/                               # Raw Olist CSV files (gitignored)
├── Database/
│   └── olist.duckdb                       # DuckDB database (gitignored)
└── SQL/
    ├── bronze/
    │   ├── 01_bronze_load.sql             # Load CSVs into bronze schema
    │   ├── 02_bronze_checks.sql           # Row counts and sample checks
    │   └── README.md
    ├── silver/
    │   ├── 01_silver_build.sql            # Clean, type-cast, standardise
    │   ├── 02_silver_checks.sql           # PK uniqueness, null checks, join tests
    │   ├── 03_silver_reviews_dedup.sql    # Deduplicate reviews
    │   ├── 04_silver_reviews_dedup_check.sql
    │   └── README.md
    └── gold/
        ├── 01_monthly_revenue.sql         # Monthly revenue and AOV
        ├── 02_category_revenue.sql        # Revenue by product category
        ├── 03_delivery_performance.sql    # Delivery time and on-time rate
        ├── 04_customer_repeat.sql         # Cohort repeat purchase rates
        ├── 05_review_summary.sql          # Review scores vs delivery speed
        ├── 99_gold_preview.sql            # Quick preview of all gold tables
        └── README.md
```

---

## Data

The raw dataset files are **not included** in this repository due to file size.

- **Dataset:** Brazilian E-Commerce Public Dataset by Olist
- **Source:** [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- **Files:** 9 CSV files covering orders, customers, products, sellers, payments, reviews, and geolocation

To reproduce the analysis, download the dataset from Kaggle and place all CSV files in `Data/Raw/`. Full instructions are in `Data/README.md`.

---

## Data Pipeline

The project follows a three-layer medallion architecture in DuckDB:

**Bronze** — Raw CSV ingestion into DuckDB tables with no transformation. Preserves the original source data.

**Silver** — Cleaned and standardised tables with proper timestamp casting, column selection, primary key validation, null checks, and review deduplication.

**Gold** — Aggregated analytical tables ready for dashboarding:

| Gold Table | Description |
|---|---|
| `gold.monthly_revenue` | Monthly orders, total revenue, and average order value |
| `gold.category_revenue` | Revenue allocated proportionally across product categories |
| `gold.delivery_performance` | Average delivery days and on-time delivery rate by month |
| `gold.customer_repeat` | Cohort-based repeat purchase rates |
| `gold.review_summary` | Average review scores alongside delivery performance metrics |

Each layer has its own README with full column-level documentation.

---

## Methods

- Revenue allocation in `02_category_revenue.sql` uses proportional weighting: each item's share of the order GMV is used to allocate the actual payment value across product categories. This avoids double-counting in multi-item orders.
- Customer repeat analysis in `04_customer_repeat.sql` uses `customer_unique_id` (not `customer_id`) to track individuals across multiple orders.
- Review deduplication in the Silver layer keeps one record per `review_id` using `ROW_NUMBER()` ordered by the most recent answer timestamp.
- Orders are filtered to valid statuses (`delivered`, `shipped`, `approved`, `invoiced`) where relevant.

---

## How to Reproduce

### 1. Clone the repository

```bash
git clone https://github.com/zarizachow/brazilian-ecommerce-sql-analytics-dashboard.git
cd brazilian-ecommerce-sql-analytics-dashboard
```

### 2. Download the data

Follow the instructions in `Data/README.md` to download the Olist dataset from Kaggle and place all CSV files in `Data/Raw/`.

### 3. Run the SQL scripts

Install DuckDB if needed, then run the scripts in order:

```bash
# Start DuckDB with the project database
duckdb Database/olist.duckdb

# Run in order
.read SQL/bronze/01_bronze_load.sql
.read SQL/bronze/02_bronze_checks.sql
.read SQL/silver/01_silver_build.sql
.read SQL/silver/02_silver_checks.sql
.read SQL/silver/03_silver_reviews_dedup.sql
.read SQL/silver/04_silver_reviews_dedup_check.sql
.read SQL/gold/01_monthly_revenue.sql
.read SQL/gold/02_category_revenue.sql
.read SQL/gold/03_delivery_performance.sql
.read SQL/gold/04_customer_repeat.sql
.read SQL/gold/05_review_summary.sql
.read SQL/gold/99_gold_preview.sql
```

---

## Limitations

- The Olist dataset covers 2016–2018 and reflects a specific Brazilian marketplace, so patterns may not generalise to other markets or time periods.
- Revenue is based on payment values, not net revenue (refunds and cancellations are excluded by status filter but chargebacks are not visible in the data).
- Customer repeat rates may undercount returning buyers if they used different accounts not linked by `customer_unique_id`.
- Geolocation data is available but not yet used in the analysis.

---

## Future Improvements

- Build interactive dashboards in Power BI or Tableau using the gold tables
- Add seller performance analysis (delivery speed by seller, seller concentration)
- Incorporate geolocation data for regional revenue and delivery time breakdowns
- Add a written report with key findings and business recommendations

---

## Tools

- DuckDB (SQL engine and local database)
- SQL (data modeling, cleaning, analysis)
- Git and GitHub (version control)

---

## License

See `LICENSE`.