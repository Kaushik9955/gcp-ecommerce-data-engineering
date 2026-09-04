CREATE OR REPLACE TABLE
  `gcp-ecommerce-de-2026.ecommerce_silver.orders_clean` AS
SELECT
  order_id,
  customer_id,
  product_id,
  order_date,
  CAST(quantity AS INT64) AS quantity,
  CAST(unit_price AS NUMERIC) AS unit_price,
  CAST(total_amount AS NUMERIC) AS total_amount,
  TRIM(payment_method) AS payment_method,
  TRIM(order_status) AS order_status
FROM
  `gcp-ecommerce-de-2026.ecommerce_bronze.orders`
WHERE
  order_id IS NOT NULL
  AND TRIM(order_id) != ''
  AND customer_id IS NOT NULL
  AND product_id IS NOT NULL
  AND order_date IS NOT NULL
  AND quantity > 0
  AND unit_price >= 0
  AND total_amount >= 0;
