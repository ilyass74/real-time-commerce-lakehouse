from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CommerceSilverOrders")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.session.timeZone", "UTC")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config(
        "spark.sql.catalog.lakehouse",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config("spark.sql.catalog.lakehouse.warehouse", "/data/warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.silver.orders (
        order_id BIGINT,
        customer_id BIGINT,
        status STRING,
        amount DECIMAL(12,2),
        currency STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        source_partition INT,
        source_offset BIGINT
    )
    USING iceberg
    TBLPROPERTIES ('format-version' = '2')
""")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW order_events AS
    SELECT
        CAST(get_json_object(event_key, '$.order_id') AS BIGINT)
            AS order_id,
        get_json_object(payload, '$.op') AS op,
        CAST(get_json_object(payload, '$.after.customer_id') AS BIGINT)
            AS customer_id,
        get_json_object(payload, '$.after.status') AS status,
        CAST(get_json_object(payload, '$.after.amount') AS DECIMAL(12,2))
            AS amount,
        get_json_object(payload, '$.after.currency') AS currency,
        CAST(get_json_object(payload, '$.after.created_at') AS TIMESTAMP)
            AS created_at,
        CAST(get_json_object(payload, '$.after.updated_at') AS TIMESTAMP)
            AS updated_at,
        partition AS source_partition,
        offset AS source_offset
    FROM lakehouse.bronze.commerce_events
    WHERE topic = 'shop.commerce.orders'
      AND payload IS NOT NULL
""")

invalid = spark.sql("""
    SELECT COUNT(*) AS n
    FROM order_events
    WHERE order_id IS NULL
       OR op IS NULL
       OR op NOT IN ('r', 'c', 'u', 'd')
       OR (
           op <> 'd' AND (
               customer_id IS NULL
               OR status IS NULL
               OR status NOT IN ('CREATED', 'PAID', 'SHIPPED', 'CANCELLED')
               OR amount IS NULL OR amount < 0
               OR currency IS NULL OR currency <> 'MAD'
               OR created_at IS NULL OR updated_at IS NULL
           )
       )
""").first()["n"]

if invalid:
    raise RuntimeError(f"Invalid order events: {invalid}")

moved_keys = spark.sql("""
    SELECT order_id
    FROM order_events
    GROUP BY order_id
    HAVING COUNT(DISTINCT source_partition) > 1
""").count()

if moved_keys:
    raise RuntimeError("An order appears in multiple Kafka partitions")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW latest_orders AS
    SELECT order_id, customer_id, status, amount, currency,
           created_at, updated_at, source_partition, source_offset, op
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY order_id
                   ORDER BY source_offset DESC
               ) AS rn
        FROM order_events
    ) ranked
    WHERE rn = 1
""")

spark.sql("""
    MERGE INTO lakehouse.silver.orders t
    USING latest_orders s
    ON t.order_id = s.order_id

    WHEN MATCHED AND s.op = 'd' THEN DELETE

    WHEN MATCHED AND s.source_offset > t.source_offset
    THEN UPDATE SET
        customer_id = s.customer_id,
        status = s.status,
        amount = s.amount,
        currency = s.currency,
        created_at = s.created_at,
        updated_at = s.updated_at,
        source_partition = s.source_partition,
        source_offset = s.source_offset

    WHEN NOT MATCHED AND s.op <> 'd'
    THEN INSERT (
        order_id, customer_id, status, amount, currency,
        created_at, updated_at, source_partition, source_offset
    )
    VALUES (
        s.order_id, s.customer_id, s.status, s.amount, s.currency,
        s.created_at, s.updated_at, s.source_partition, s.source_offset
    )
""")

print("SILVER ORDERS COMPLETE", flush=True)

spark.sql("""
    SELECT COUNT(*) AS orders,
           COUNT(DISTINCT order_id) AS unique_order_ids,
           SUM(amount) AS total_order_amount
    FROM lakehouse.silver.orders
""").show()

spark.sql("""
    SELECT status, currency, COUNT(*) AS orders, SUM(amount) AS amount
    FROM lakehouse.silver.orders
    GROUP BY status, currency
    ORDER BY status, currency
""").show()

spark.stop()
