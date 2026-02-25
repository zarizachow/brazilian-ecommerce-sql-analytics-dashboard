# Bronze Layer Documentation (DuckDB)

## Overview
The **Bronze Layer** represents the raw data ingestion zone using **DuckDB** - an in-process SQL OLAP database management system. This layer stores data in its original format from the Olist Brazilian E-Commerce dataset with minimal transformation.

## Why DuckDB?

✅ **Fast Analytics** - Optimized for analytical queries (OLAP)  
✅ **Embedded** - No server setup required, runs in-process  
✅ **Efficient CSV Loading** - Built-in `read_csv_auto()` function  
✅ **SQL Standards** - Full SQL support with analytical functions  
✅ **Portable** - Single file database, easy to share  
✅ **Python Integration** - Seamless integration with data science workflows  
✅ **Free & Open Source** - MIT licensed  

## Architecture Pattern
Following the **Medallion Architecture** (Bronze → Silver → Gold):
- **Bronze**: Raw data ingestion (current layer) ✅
- **Silver**: Cleaned and validated data ⏭️
- **Gold**: Business-level aggregates ⏭️

## Files in This Layer

### 1. `00_setup_duckdb.py`
**Purpose**: Python script to orchestrate the entire Bronze layer setup.

**Features**:
- Verifies all CSV files exist
- Creates DuckDB database
- Executes SQL scripts in correct order
- Displays comprehensive summary
- Error handling and progress reporting

**Usage**:
```bash
# Install DuckDB
pip install duckdb

# Run setup script
python 00_setup_duckdb.py
```

### 2. `01_bronze_schema_creation.sql`
**Purpose**: Creates all bronze tables with proper schema definition.

**Tables Created**:
- `bronze_customers` - Customer information
- `bronze_orders` - Order transactions
- `bronze_order_items` - Individual items within orders
- `bronze_products` - Product catalog
- `bronze_sellers` - Seller information
- `bronze_order_payments` - Payment details
- `bronze_order_reviews` - Customer reviews
- `bronze_geolocation` - Geographic coordinates
- `bronze_product_category_translation` - Category name translations

**Key Features**:
- DuckDB-optimized data types
- Primary keys and indexes
- Timestamp tracking with `loaded_at` field
- `CREATE OR REPLACE` for idempotency

### 3. `02_bronze_data_loading.sql`
**Purpose**: Loads CSV data using DuckDB's `read_csv_auto()` function.

**Features**:
- Automatic schema detection
- Relative path support (`../Data/Raw/`)
- `TRY_CAST` for safe type conversion
- NULL handling with `nullstr=''`
- Immediate load confirmation
- Comprehensive loading summary

**Advantages over Traditional LOAD**:
- No file permission issues
- Automatic delimiter detection
- Better error messages
- Handles malformed data gracefully

### 4. `03_bronze_data_quality_checks.sql`
**Purpose**: Validates data integrity and quality after loading.

**Validation Checks** (12 comprehensive checks):
1. **Record Counts** - Verify all data loaded successfully
2. **NULL Analysis** - Identify missing values using `COUNT(*) FILTER`
3. **Duplicate Detection** - Check for duplicate primary keys
4. **Referential Integrity** - Validate foreign key relationships
5. **Data Ranges** - Check for outliers and invalid values
6. **Order Status Distribution** - Analyze order statuses
7. **Geographic Coverage** - Verify location data
8. **Payment Types** - Analyze payment methods
9. **Product Categories** - Check categorization
10. **Delivery Performance** - Analyze delivery metrics
11. **Data Freshness** - Check load timestamps
12. **Quality Score** - Overall data quality metric

## Execution Methods

### Method 1: Python Script (Recommended ⭐)
```bash
# Install DuckDB
pip install duckdb

# Run the complete setup
python 00_setup_duckdb.py
```

### Method 2: DuckDB CLI
```bash
# Install DuckDB CLI
# macOS: brew install duckdb
# Or download from: https://duckdb.org/docs/installation/

# Create database and run scripts
duckdb olist_ecommerce.duckdb < 01_bronze_schema_creation.sql
duckdb olist_ecommerce.duckdb < 02_bronze_data_loading.sql
duckdb olist_ecommerce.duckdb < 03_bronze_data_quality_checks.sql
```

### Method 3: Interactive Python
```python
import duckdb

# Connect to database
conn = duckdb.connect('olist_ecommerce.duckdb')

# Execute scripts
with open('01_bronze_schema_creation.sql', 'r') as f:
    conn.execute(f.read())

with open('02_bronze_data_loading.sql', 'r') as f:
    conn.execute(f.read())

with open('03_bronze_data_quality_checks.sql', 'r') as f:
    results = conn.execute(f.read()).fetchall()
    for row in results:
        print(row)

# Query the data
df = conn.execute("SELECT * FROM bronze_orders LIMIT 10").df()
print(df)

conn.close()
```

### Method 4: Jupyter Notebook
```python
import duckdb
import pandas as pd

# Connect
conn = duckdb.connect('olist_ecommerce.duckdb')

# Query as DataFrame
orders_df = conn.execute("""
    SELECT * FROM bronze_orders 
    WHERE order_status = 'delivered'
    LIMIT 1000
""").df()

# Analyze with pandas
print(orders_df.describe())

# Visualize
orders_df.groupby('order_status').size().plot(kind='bar')
```

## Expected Results

### Record Counts (Approximate):
- **Customers**: ~99,441
- **Orders**: ~99,441
- **Order Items**: ~112,650
- **Products**: ~32,951
- **Sellers**: ~3,095
- **Payments**: ~103,886
- **Reviews**: ~99,224
- **Geolocation**: ~1,000,163
- **Category Translations**: 71

### Data Quality Expectations:
- ✅ No duplicate primary keys
- ✅ All foreign keys reference valid records (high integrity)
- ⚠️ Some NULL values expected in optional fields (delivery dates, reviews)
- ✅ Date ranges: September 2016 - October 2018
- ✅ Review scores: 1-5
- ✅ Geographic coverage: 27 Brazilian states
- ✅ Payment types: credit_card, boleto, voucher, debit_card

### Database Size:
- Total size: ~150-200 MB (compressed in DuckDB)
- Query performance: Sub-second for most analytical queries

## DuckDB-Specific Features Used

### 1. `read_csv_auto()` Function
```sql
FROM read_csv_auto('../Data/Raw/file.csv', 
    header=true,
    nullstr='',
    ignore_errors=false
)
```
- Automatically detects delimiter, types, and encoding
- Efficient columnar reading
- Memory-efficient streaming for large files

### 2. `FILTER` Clause (SQL:2003 Standard)
```sql
COUNT(*) FILTER (WHERE condition)
```
- More readable than `SUM(CASE WHEN...)`
- Better optimization by query planner

### 3. Window Functions
```sql
COUNT(*) * 100.0 / SUM(COUNT(*)) OVER()
```
- Calculate percentages without subqueries
- Efficient analytical operations

### 4. `TRY_CAST` for Safe Type Conversion
```sql
TRY_CAST(price AS DECIMAL(10,2))
```
- Returns NULL instead of error on conversion failure
- Handles dirty data gracefully

## Troubleshooting

### Issue: Python module not found
**Solution**: 
```bash
pip install duckdb
# or
pip3 install duckdb
```

### Issue: File path not found
**Solution**: 
- Use relative paths from SQL directory: `../Data/Raw/`
- Or use absolute paths
- Verify CSV files exist in `Data/Raw/`

### Issue: Memory error with large files
**Solution**: 
DuckDB handles large files efficiently, but if needed:
```python
# Increase memory limit
conn.execute("SET memory_limit='4GB'")
```

### Issue: "Table already exists"
**Solution**: 
Scripts use `CREATE OR REPLACE TABLE` - safe to re-run

### Issue: Character encoding problems
**Solution**: 
DuckDB auto-detects encoding. If issues persist:
```sql
FROM read_csv_auto('file.csv', encoding='UTF-8')
```

## Performance Tips

1. **Indexes**: Already created on foreign keys and frequent filter columns
2. **Statistics**: DuckDB automatically maintains statistics
3. **Memory**: DuckDB uses available memory intelligently
4. **Parallel Processing**: Automatically parallelizes queries
5. **Storage**: Columnar storage optimizes analytical queries

## Querying the Data

```sql
-- Connect to database
duckdb olist_ecommerce.duckdb

-- Basic query
SELECT COUNT(*) FROM bronze_orders;

-- Join query
SELECT 
    o.order_id,
    c.customer_city,
    c.customer_state,
    o.order_status
FROM bronze_orders o
JOIN bronze_customers c ON o.customer_id = c.customer_id
LIMIT 10;

-- Analytical query
SELECT 
    DATE_TRUNC('month', order_purchase_timestamp) AS month,
    COUNT(*) AS orders,
    COUNT(DISTINCT customer_id) AS customers
FROM bronze_orders
GROUP BY month
ORDER BY month;
```

## Exporting Data

```sql
-- Export to CSV
COPY (SELECT * FROM bronze_orders) TO 'orders_export.csv' (HEADER, DELIMITER ',');

-- Export to Parquet (more efficient)
COPY bronze_orders TO 'orders_export.parquet' (FORMAT PARQUET);

-- Export to JSON
COPY (SELECT * FROM bronze_orders LIMIT 1000) TO 'orders_export.json';
```

## Integration with Python/Pandas

```python
import duckdb
import pandas as pd

conn = duckdb.connect('olist_ecommerce.duckdb')

# Method 1: Query to DataFrame
df = conn.execute("SELECT * FROM bronze_orders").df()

# Method 2: Register DataFrame as table
new_data = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
conn.register('temp_table', new_data)
conn.execute("SELECT * FROM temp_table")

# Method 3: Direct SQL on DataFrames
result = duckdb.query("SELECT * FROM df WHERE col1 > 1").df()
```

## Next Steps

After completing the Bronze layer:
1. ✅ **Bronze Layer Complete** - Raw data ingested
2. ⏭️ **Silver Layer** - Data cleaning, type conversion, deduplication
3. ⏭️ **Gold Layer** - Business metrics, aggregations, KPIs
4. ⏭️ **Analytics Queries** - RFM analysis, cohort analysis, trends
5. ⏭️ **Dashboard Prep** - Create views and export for BI tools

## Data Lineage

```
Source: Kaggle Olist Dataset (9 CSV files)
    ↓
Bronze Layer (Raw ingestion - DuckDB) ✅
    ↓
Silver Layer (Cleaned & validated)
    ↓
Gold Layer (Business metrics)
    ↓
Dashboards & Analytics (Power BI/Tableau)
```

## Maintenance

**Regular Tasks**:
- Monitor database size: `SELECT * FROM duckdb_tables()`
- Rebuild statistics: `ANALYZE;`
- Export backups: DuckDB file is portable
- Version control: Track SQL scripts in Git

**Audit Trail**:
- All tables include `loaded_at` timestamp
- Track data freshness and lineage
- Monitor load durations

## Additional Resources

- **DuckDB Documentation**: https://duckdb.org/docs/
- **DuckDB GitHub**: https://github.com/duckdb/duckdb
- **Kaggle Dataset**: https://www.kaggle.com/olistbr/brazilian-ecommerce

---

**Author**: Zariza Chowdhury  
**Created**: 2026-02-25  
**Version**: 1.0  
**Database**: DuckDB 1.0+  
**Layer**: Bronze (Raw/Landing)  
**Status**: Production Ready ✅
