#!/usr/bin/env python3
"""
============================================================================
DuckDB Setup and Execution Script
============================================================================
Purpose: Initialize DuckDB database and execute Bronze layer SQL scripts
Database: DuckDB
Author: Zariza Chowdhury
Date: 2026-02-25
============================================================================
"""

import duckdb
import os
from pathlib import Path

# Configuration
DB_FILE = 'olist_ecommerce.duckdb'
SQL_DIR = Path(__file__).parent
DATA_DIR = SQL_DIR.parent / 'Data' / 'Raw'

# SQL Scripts to execute in order
SQL_SCRIPTS = [
    '01_bronze_schema_creation.sql',
    '02_bronze_data_loading.sql',
    '03_bronze_data_quality_checks.sql'
]

def create_database():
    """Create or connect to DuckDB database"""
    print(f"📊 Connecting to DuckDB database: {DB_FILE}")
    conn = duckdb.connect(DB_FILE)
    print("✅ Database connection established\n")
    return conn

def execute_sql_file(conn, sql_file):
    """Execute a SQL file and print results"""
    sql_path = SQL_DIR / sql_file
    
    if not sql_path.exists():
        print(f"❌ SQL file not found: {sql_path}")
        return False
    
    print(f"🔧 Executing: {sql_file}")
    print("=" * 80)
    
    try:
        # Read SQL file
        with open(sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Split by semicolon and execute each statement
        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        
        for i, statement in enumerate(statements, 1):
            # Skip comments and empty statements
            if statement.startswith('--') or not statement:
                continue
            
            try:
                result = conn.execute(statement).fetchall()
                
                # Print results if there are any
                if result:
                    print(f"\n📋 Query {i} Results:")
                    for row in result:
                        print(row)
            
            except Exception as e:
                # Some statements don't return results (CREATE, INSERT, etc.)
                if "nothing to fetch" not in str(e).lower():
                    print(f"⚠️  Statement {i}: {str(e)}")
        
        print(f"\n✅ Completed: {sql_file}\n")
        return True
    
    except Exception as e:
        print(f"\n❌ Error executing {sql_file}: {str(e)}\n")
        return False

def verify_data_files():
    """Check if all required CSV files exist"""
    print("🔍 Verifying CSV data files...")
    
    required_files = [
        'olist_customers_dataset.csv',
        'olist_orders_dataset.csv',
        'olist_order_items_dataset.csv',
        'olist_products_dataset.csv',
        'olist_sellers_dataset.csv',
        'olist_order_payments_dataset.csv',
        'olist_order_reviews_dataset.csv',
        'olist_geolocation_dataset.csv',
        'product_category_name_translation.csv'
    ]
    
    missing_files = []
    for file in required_files:
        file_path = DATA_DIR / file
        if file_path.exists():
            file_size = file_path.stat().st_size / (1024 * 1024)  # Size in MB
            print(f"  ✓ {file} ({file_size:.2f} MB)")
        else:
            print(f"  ✗ {file} - NOT FOUND")
            missing_files.append(file)
    
    if missing_files:
        print(f"\n❌ Missing {len(missing_files)} required CSV file(s)")
        return False
    
    print(f"\n✅ All {len(required_files)} CSV files found\n")
    return True

def display_summary(conn):
    """Display final database summary"""
    print("\n" + "=" * 80)
    print("📊 DATABASE SUMMARY")
    print("=" * 80)
    
    # Get table counts
    tables_query = """
    SELECT 
        table_name,
        estimated_size,
        ROUND(estimated_size / 1024.0 / 1024.0, 2) AS size_mb
    FROM duckdb_tables()
    WHERE schema_name = 'main'
    ORDER BY table_name;
    """
    
    try:
        result = conn.execute(tables_query).fetchall()
        print("\nTables Created:")
        for row in result:
            print(f"  • {row[0]}: {row[2]} MB")
    except Exception as e:
        print(f"Error getting summary: {e}")
    
    print("\n✅ Bronze layer setup complete!")
    print(f"📁 Database file: {os.path.abspath(DB_FILE)}")
    print("\n" + "=" * 80)

def main():
    """Main execution function"""
    print("\n" + "=" * 80)
    print("🚀 OLIST E-COMMERCE - BRONZE LAYER SETUP (DuckDB)")
    print("=" * 80 + "\n")
    
    # Step 1: Verify data files
    if not verify_data_files():
        print("\n❌ Setup aborted due to missing data files")
        return
    
    # Step 2: Create/connect to database
    conn = create_database()
    
    # Step 3: Execute SQL scripts
    success_count = 0
    for sql_file in SQL_SCRIPTS:
        if execute_sql_file(conn, sql_file):
            success_count += 1
        else:
            print(f"\n⚠️  Warning: {sql_file} had errors but continuing...")
    
    # Step 4: Display summary
    display_summary(conn)
    
    # Step 5: Close connection
    conn.close()
    
    print(f"\n✨ Setup completed: {success_count}/{len(SQL_SCRIPTS)} scripts executed successfully")
    print("\n💡 Next steps:")
    print("  1. Review the data quality check results above")
    print("  2. Proceed to Silver layer (data cleaning & transformation)")
    print("  3. Build analytical queries and dashboards\n")

if __name__ == "__main__":
    main()
