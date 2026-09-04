CREATE OR REPLACE TABLE
  `gcp-ecommerce-de-2026.ecommerce_silver.products_clean` AS
SELECT
  product_id,
  TRIM(product_name) AS product_name,
  TRIM(category) AS category,
  CAST(price AS NUMERIC) AS price
FROM
  `gcp-ecommerce-de-2026.ecommerce_bronze.products`
WHERE
  product_id IS NOT NULL
  AND TRIM(product_id) != ''
  AND price IS NOT NULL
  AND price >= 0;
