from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("InjectInvalidOrder")
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

# Remove the previous copy if this test is rerun.
spark.sql("""
    DELETE FROM lakehouse.bronze.commerce_events
    WHERE topic = 'shop.commerce.orders'
      AND partition = 99
      AND offset = 999999999
""")

spark.sql("""
    INSERT INTO lakehouse.bronze.commerce_events (
        topic,
        partition,
        offset,
        kafka_timestamp,
        event_key,
        payload,
        ingestion_time
    )
    VALUES (
        'shop.commerce.orders',
        99,
        999999999,
        current_timestamp(),
        '{"order_id":999999999}',
        '{"op":"c","after":{"order_id":999999999,"customer_id":null,"status":"CREATED","amount":100.00,"currency":"MAD","created_at":"2026-09-25T10:00:00Z","updated_at":"2026-09-25T10:00:00Z"}}',
        current_timestamp()
    )
""")

print("PASS INVALID ORDER EVENT INJECTED", flush=True)

spark.stop()
