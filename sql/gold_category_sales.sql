CREATE OR REPLACE TABLE
  `gcp-ecommerce-de-2026.ecommerce_gold.category_sales` AS
SELECT
  p.category,
  COUNT(DISTINCT o.order_id) AS total_orders,
  SUM(o.quantity) AS total_items,
  SUM(o.total_amount) AS total_revenue,
  AVG(o.total_amount) AS average_order_value
FROM
  `gcp-ecommerce-de-2026.ecommerce_silver.orders_clean` AS o
INNER JOIN
  `gcp-ecommerce-de-2026.ecommerce_silver.products_clean` AS p
ON
  o.product_id = p.product_id
WHERE
  o.order_status = 'Completed'
GROUP BY
  p.category
ORDER BY
  total_revenue DESC;
