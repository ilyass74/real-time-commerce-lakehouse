from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("InjectInvalidTestEvent")
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

spark.sql("""
INSERT INTO lakehouse.bronze.commerce_events
SELECT
    'shop.commerce.orders' AS topic,
    99 AS partition,
    CAST(999999999 AS BIGINT) AS offset,
    '{"order_id":999999999}' AS event_key,
    '{
        "op":"c",
        "after":{
            "order_id":999999999,
            "customer_id":null,
            "status":"CREATED",
            "amount":100.00,
            "created_at":"2026-09-25T10:00:00Z",
            "updated_at":"2026-09-25T10:00:00Z"
        }
    }' AS payload,
    current_timestamp() AS kafka_timestamp,
    current_timestamp() AS ingestion_timestamp
""")

print("PASS INVALID TEST EVENT INJECTED", flush=True)

spark.stop()
