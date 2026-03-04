SELECT COUNT(*) - COUNT(DISTINCT review_id) AS dup_review_id
FROM silver.reviews_dedup;