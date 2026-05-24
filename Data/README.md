# Data

The raw dataset files are **not included** in this repository due to file size. Follow the instructions below to download and set up the data.

---

## Source

- **Dataset:** Brazilian E-Commerce Public Dataset by Olist
- **Platform:** Kaggle
- **URL:** https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
- **License:** CC BY-NC-SA 4.0

---

## Files

Download the following 9 CSV files and place them in `Data/Raw/`:

| File | Description |
|---|---|
| `olist_orders_dataset.csv` | Order-level data with timestamps and status |
| `olist_order_items_dataset.csv` | Item-level details per order (price, freight, seller) |
| `olist_order_payments_dataset.csv` | Payment method and value per order |
| `olist_order_reviews_dataset.csv` | Customer review scores and comments |
| `olist_customers_dataset.csv` | Customer IDs and location info |
| `olist_products_dataset.csv` | Product catalog with category and dimensions |
| `olist_sellers_dataset.csv` | Seller IDs and location info |
| `olist_geolocation_dataset.csv` | Zip code level latitude and longitude |
| `product_category_name_translation.csv` | Portuguese to English category name mapping |

---

## Setup

1. Go to the Kaggle dataset page linked above
2. Click **Download** to get the zip file
3. Extract all CSV files into `Data/Raw/`
4. Run `SQL/bronze/01_bronze_load.sql` to load the data into DuckDB
