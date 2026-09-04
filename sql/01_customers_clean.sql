CREATE OR REPLACE TABLE
  `gcp-ecommerce-de-2026.ecommerce_silver.customers_clean` AS
SELECT
  customer_id,
  TRIM(customer_name) AS customer_name,
  LOWER(TRIM(email)) AS email,
  TRIM(city) AS city,
  TRIM(state) AS state,
  signup_date
FROM
  `gcp-ecommerce-de-2026.ecommerce_bronze.customers`
WHERE
  customer_id IS NOT NULL
  AND TRIM(customer_id) != '';
