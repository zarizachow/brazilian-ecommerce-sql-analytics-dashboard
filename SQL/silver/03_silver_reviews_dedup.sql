-- Silver: Deduplicate reviews
-- Keep 1 row per review_id (latest answer timestamp)

CREATE SCHEMA IF NOT EXISTS silver;

CREATE OR REPLACE TABLE silver.reviews_dedup AS
SELECT *
FROM (
  SELECT
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_ts,
    review_answer_ts,
    ROW_NUMBER() OVER (
      PARTITION BY review_id
      ORDER BY review_answer_ts DESC NULLS LAST, review_creation_ts DESC
    ) AS rn
  FROM silver.reviews
)
WHERE rn = 1;