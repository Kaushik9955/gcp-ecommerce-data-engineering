CREATE OR REPLACE TABLE
  `gcp-ecommerce-de-2026.ecommerce_gold.product_performance` AS
SELECT
  p.product_id,
  p.product_name,
  p.category,
  COUNT(DISTINCT o.order_id) AS total_orders,
  SUM(o.quantity) AS units_sold,
  SUM(o.total_amount) AS revenue,
  AVG(o.unit_price) AS average_selling_price
FROM
  `gcp-ecommerce-de-2026.ecommerce_silver.orders_clean` AS o
INNER JOIN
  `gcp-ecommerce-de-2026.ecommerce_silver.products_clean` AS p
ON
  o.product_id = p.product_id
WHERE
  o.order_status = 'Completed'
GROUP BY
  p.product_id,
  p.product_name,
  p.category
ORDER BY
  revenue DESC;
