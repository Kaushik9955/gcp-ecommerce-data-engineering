
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

PROJECT = "gcp-ecommerce-de-2026"
BUCKET = "gcp-ecommerce-de-2026-data"
REGION = "asia-south1"

# Daily scheduled processing.
# The 02:00 IST run processes the previous day's data.
PROCESS_DATE = "{{ dag_run.conf.get('process_date') or data_interval_start.in_timezone('Asia/Kolkata').strftime('%Y-%m-%d') }}"
OBJECT = f"raw/orders/{PROCESS_DATE}/orders.csv"

default_args = {
    "owner": "ecommerce-data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ecommerce_daily_pipeline",
    description="Daily incremental e-commerce orders pipeline",
    start_date=pendulum.datetime(2026, 9, 2, 2, 0, tz="Asia/Kolkata"),
    schedule="0 2 * * *",
    catchup=False,
    default_args=default_args,
    tags=["ecommerce", "incremental", "gcp"],
) as dag:

    wait_for_file = GCSObjectExistenceSensor(
        task_id="wait_for_daily_orders_file",
        bucket=BUCKET,
        object=OBJECT,
        poke_interval=30,
        timeout=600,
        mode="reschedule",
    )

    load_staging = GCSToBigQueryOperator(
        task_id="load_incremental_orders_to_staging",
        bucket=BUCKET,
        source_objects=[OBJECT],
        destination_project_dataset_table=f"{PROJECT}.ecommerce_staging.orders_new",
        source_format="CSV",
        skip_leading_rows=1,
        field_delimiter=",",
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        location=REGION,
    )

    validate = BigQueryCheckOperator(
        task_id="validate_increment",
        sql=f"""
        SELECT
          COUNT(*) > 0
          AND COUNT(*) = COUNT(DISTINCT order_id)
          AND COUNTIF(
            order_id IS NULL
            OR customer_id IS NULL
            OR product_id IS NULL
            OR order_date IS NULL
            OR quantity <= 0
            OR unit_price < 0
            OR total_amount < 0
          ) = 0
        FROM `{PROJECT}.ecommerce_staging.orders_new`
        """,
        use_legacy_sql=False,
        location=REGION,
    )

    merge_bronze = BigQueryInsertJobOperator(
        task_id="merge_orders_into_bronze",
        configuration={
            "query": {
                "query": f"""
                MERGE `{PROJECT}.ecommerce_bronze.orders` AS t
                USING `{PROJECT}.ecommerce_staging.orders_new` AS s
                ON t.order_id = s.order_id
                WHEN MATCHED THEN UPDATE SET
                  customer_id = s.customer_id,
                  product_id = s.product_id,
                  order_date = s.order_date,
                  quantity = s.quantity,
                  unit_price = s.unit_price,
                  total_amount = s.total_amount,
                  payment_method = s.payment_method,
                  order_status = s.order_status
                WHEN NOT MATCHED THEN INSERT (
                  order_id, customer_id, product_id, order_date, quantity,
                  unit_price, total_amount, payment_method, order_status
                ) VALUES (
                  s.order_id, s.customer_id, s.product_id, s.order_date,
                  s.quantity, s.unit_price, s.total_amount,
                  s.payment_method, s.order_status
                )
                """,
                "useLegacySql": False,
            }
        },
        location=REGION,
    )

    merge_silver = BigQueryInsertJobOperator(
        task_id="merge_incremental_orders_into_silver",
        configuration={
            "query": {
                "query": f"""
                MERGE `{PROJECT}.ecommerce_silver.orders_clean` AS t
                USING (
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
                  FROM `{PROJECT}.ecommerce_staging.orders_new`
                  WHERE order_id IS NOT NULL
                    AND TRIM(order_id) != ''
                    AND customer_id IS NOT NULL
                    AND product_id IS NOT NULL
                    AND order_date IS NOT NULL
                    AND quantity > 0
                    AND unit_price >= 0
                    AND total_amount >= 0
                ) AS s
                ON t.order_id = s.order_id
                WHEN MATCHED THEN UPDATE SET
                  customer_id = s.customer_id,
                  product_id = s.product_id,
                  order_date = s.order_date,
                  quantity = s.quantity,
                  unit_price = s.unit_price,
                  total_amount = s.total_amount,
                  payment_method = s.payment_method,
                  order_status = s.order_status
                WHEN NOT MATCHED THEN INSERT (
                  order_id, customer_id, product_id, order_date, quantity,
                  unit_price, total_amount, payment_method, order_status
                ) VALUES (
                  s.order_id, s.customer_id, s.product_id, s.order_date,
                  s.quantity, s.unit_price, s.total_amount,
                  s.payment_method, s.order_status
                )
                """,
                "useLegacySql": False,
            }
        },
        location=REGION,
    )

    refresh_gold = BigQueryInsertJobOperator(
        task_id="refresh_gold_tables",
        configuration={
            "query": {
                "query": f"""
                CREATE OR REPLACE TABLE `{PROJECT}.ecommerce_gold.daily_sales` AS
                SELECT
                  order_date,
                  COUNT(DISTINCT order_id) AS total_orders,
                  SUM(quantity) AS total_items,
                  SUM(total_amount) AS revenue,
                  AVG(total_amount) AS average_order_value
                FROM `{PROJECT}.ecommerce_silver.orders_clean`
                WHERE order_status = 'Completed'
                GROUP BY order_date;

                CREATE OR REPLACE TABLE `{PROJECT}.ecommerce_gold.product_performance` AS
SELECT
  o.order_date,
  p.product_id,
  p.product_name,
  p.category,
  COUNT(DISTINCT o.order_id) AS total_orders,
  SUM(o.quantity) AS units_sold,
  SUM(o.total_amount) AS revenue,
  AVG(o.unit_price) AS average_selling_price
FROM `{PROJECT}.ecommerce_silver.orders_clean` AS o
JOIN `{PROJECT}.ecommerce_silver.products_clean` AS p
  ON o.product_id = p.product_id
WHERE o.order_status = 'Completed'
GROUP BY
  o.order_date,
  p.product_id,
  p.product_name,
  p.category;

                CREATE OR REPLACE TABLE `{PROJECT}.ecommerce_gold.customer_sales` AS
SELECT
  o.order_date,
  c.customer_id,
  c.customer_name,
  c.city,
  c.state,
  COUNT(DISTINCT o.order_id) AS total_orders,
  SUM(o.quantity) AS total_items,
  SUM(o.total_amount) AS total_revenue,
  AVG(o.total_amount) AS average_order_value
FROM `{PROJECT}.ecommerce_silver.customers_clean` AS c
JOIN `{PROJECT}.ecommerce_silver.orders_clean` AS o
  ON c.customer_id = o.customer_id
WHERE o.order_status = 'Completed'
GROUP BY
  o.order_date,
  c.customer_id,
  c.customer_name,
  c.city,
  c.state;

                CREATE OR REPLACE TABLE `{PROJECT}.ecommerce_gold.category_sales` AS
SELECT
  o.order_date,
  p.category,
  COUNT(DISTINCT o.order_id) AS total_orders,
  SUM(o.quantity) AS total_items,
  SUM(o.total_amount) AS total_revenue,
  AVG(o.total_amount) AS average_order_value
FROM `{PROJECT}.ecommerce_silver.orders_clean` AS o
JOIN `{PROJECT}.ecommerce_silver.products_clean` AS p
  ON o.product_id = p.product_id
WHERE o.order_status = 'Completed'
GROUP BY
  o.order_date,
  p.category;
                """,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location=REGION,
    )

    wait_for_file >> load_staging >> validate >> merge_bronze >> merge_silver >> refresh_gold
