SELECT
  COUNT(*) AS new_rows,
  COUNT(DISTINCT order_id) AS unique_orders,
  MIN(order_date) AS min_order_date,
  MAX(order_date) AS max_order_date
FROM `gcp-ecommerce-de-2026.ecommerce_staging.orders_new`;
