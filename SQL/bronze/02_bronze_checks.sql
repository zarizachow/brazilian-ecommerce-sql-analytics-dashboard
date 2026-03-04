-- Bronze Layer: Quick sanity checks

-- Row counts for each table
SELECT 'customers' AS table_name, COUNT(*) AS row_count FROM bronze.customers
UNION ALL SELECT 'orders', COUNT(*) FROM bronze.orders
UNION ALL SELECT 'order_items', COUNT(*) FROM bronze.order_items
UNION ALL SELECT 'products', COUNT(*) FROM bronze.products
UNION ALL SELECT 'sellers', COUNT(*) FROM bronze.sellers
UNION ALL SELECT 'payments', COUNT(*) FROM bronze.payments
UNION ALL SELECT 'reviews', COUNT(*) FROM bronze.reviews
UNION ALL SELECT 'category_translation', COUNT(*) FROM bronze.category_translation
UNION ALL SELECT 'geolocation', COUNT(*) FROM bronze.geolocation
ORDER BY row_count DESC;

-- Quick check of key columns
SELECT * FROM bronze.orders LIMIT 5;
SELECT * FROM bronze.order_items LIMIT 5;
SELECT * FROM bronze.customers LIMIT 5;