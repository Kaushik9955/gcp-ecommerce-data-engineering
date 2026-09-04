MERGE `gcp-ecommerce-de-2026.ecommerce_bronze.orders` AS target
USING `gcp-ecommerce-de-2026.ecommerce_staging.orders_new` AS source
ON target.order_id = source.order_id

WHEN MATCHED THEN
  UPDATE SET
    customer_id = source.customer_id,
    product_id = source.product_id,
    order_date = source.order_date,
    quantity = source.quantity,
    unit_price = source.unit_price,
    total_amount = source.total_amount,
    payment_method = source.payment_method,
    order_status = source.order_status

WHEN NOT MATCHED THEN
  INSERT (
    order_id,
    customer_id,
    product_id,
    order_date,
    quantity,
    unit_price,
    total_amount,
    payment_method,
    order_status
  )
  VALUES (
    source.order_id,
    source.customer_id,
    source.product_id,
    source.order_date,
    source.quantity,
    source.unit_price,
    source.total_amount,
    source.payment_method,
    source.order_status
  );
