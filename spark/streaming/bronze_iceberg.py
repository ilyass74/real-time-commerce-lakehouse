from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = (
    SparkSession.builder
    .appName("CommerceBronze")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.session.timeZone", "UTC")
    .config(
        "spark.sql.catalog.lakehouse",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config(
        "spark.sql.catalog.lakehouse.warehouse",
        "/data/warehouse"
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")

spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.bronze.commerce_events (
        topic STRING,
        partition INT,
        offset BIGINT,
        kafka_timestamp TIMESTAMP,
        event_key STRING,
        payload STRING,
        ingestion_time TIMESTAMP
    )
    USING iceberg
    TBLPROPERTIES ('format-version' = '2')
""")

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", ",".join(
        f"shop.commerce.{table}"
        for table in [
            "customers", "products", "orders",
            "order_items", "payments", "inventory"
        ]
    ))
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "2000")
    .load()
)

bronze = (
    raw.selectExpr(
        "topic",
        "partition",
        "offset",
        "timestamp AS kafka_timestamp",
        "CAST(key AS STRING) AS event_key",
        "CAST(value AS STRING) AS payload"
    )
    .withColumn("ingestion_time", current_timestamp())
)

query = (
    bronze.writeStream
    .format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", "/data/checkpoints/commerce_bronze")
    .trigger(availableNow=True)
    .toTable("lakehouse.bronze.commerce_events")
)
query.awaitTermination()

print("BRONZE INGESTION COMPLETE", flush=True)

spark.sql("""
    SELECT topic, COUNT(*) AS event_count
    FROM lakehouse.bronze.commerce_events
    GROUP BY topic
    ORDER BY topic
""").show(truncate=False)

spark.sql("""
    SELECT COUNT(*) AS duplicate_kafka_positions
    FROM (
        SELECT topic, partition, offset
        FROM lakehouse.bronze.commerce_events
        GROUP BY topic, partition, offset
        HAVING COUNT(*) > 1
    ) duplicates
""").show()

spark.stop()
