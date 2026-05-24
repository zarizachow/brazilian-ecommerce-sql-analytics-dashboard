-- Preview gold tables

SELECT 'monthly_revenue' AS table_name, * 
FROM gold.monthly_revenue
LIMIT 5;

SELECT 'category_revenue' AS table_name, *
FROM gold.category_revenue
LIMIT 5;

SELECT 'delivery_performance' AS table_name, *
FROM gold.delivery_performance
LIMIT 5;

SELECT 'customer_repeat' AS table_name, *
FROM gold.customer_repeat
LIMIT 5;

SELECT 'review_summary' AS table_name, *
FROM gold.review_summary
LIMIT 5;