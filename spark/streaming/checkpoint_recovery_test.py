import os
import shutil

from pyspark.sql import SparkSession

CHECKPOINT = "/data/checkpoints/orders_recovery_test"
FAIL_MARKER = "/data/checkpoints/orders_recovery_test_failed.marker"
TARGET = "lakehouse.test.checkpoint_orders"

RESET = os.environ.get("RESET_TEST", "0") == "1"
FAIL_ONCE = os.environ.get("FAIL_ONCE", "0") == "1"

spark = (
    SparkSession.builder
    .appName("CommerceCheckpointRecoveryTest")
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

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.test")

if RESET:
    spark.sql(f"DROP TABLE IF EXISTS {TARGET}")
    shutil.rmtree(CHECKPOINT, ignore_errors=True)

    if os.path.exists(FAIL_MARKER):
        os.remove(FAIL_MARKER)

    print("RESET TEST STATE COMPLETE", flush=True)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET} (
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

stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "shop.commerce.orders")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "100")
    .load()
    .selectExpr(
        "topic",
        "partition",
        "offset",
        "timestamp AS kafka_timestamp",
        "CAST(key AS STRING) AS event_key",
        "CAST(value AS STRING) AS payload"
    )
)

def process_batch(batch_df, batch_id):
    count = batch_df.count()

    if count == 0:
        return

    batch_df.createOrReplaceGlobalTempView("recovery_batch")

    spark.sql(f"""
        MERGE INTO {TARGET} t

        USING (
            SELECT
                topic,
                partition,
                offset,
                kafka_timestamp,
                event_key,
                payload,
                current_timestamp() AS ingestion_time
            FROM global_temp.recovery_batch
        ) s

        ON t.topic = s.topic
           AND t.partition = s.partition
           AND t.offset = s.offset

        WHEN NOT MATCHED THEN
        INSERT (
            topic,
            partition,
            offset,
            kafka_timestamp,
            event_key,
            payload,
            ingestion_time
        )
        VALUES (
            s.topic,
            s.partition,
            s.offset,
            s.kafka_timestamp,
            s.event_key,
            s.payload,
            s.ingestion_time
        )
    """)

    total = spark.sql(f"""
        SELECT COUNT(*) AS n
        FROM {TARGET}
    """).first()["n"]

    print(
        f"BATCH {batch_id}: input={count}, target_rows={total}",
        flush=True
    )

    # Simulate Spark dying after data was written,
    # but before this micro-batch is successfully committed.
    if FAIL_ONCE and not os.path.exists(FAIL_MARKER):
        os.makedirs("/data/checkpoints", exist_ok=True)

        with open(FAIL_MARKER, "w") as f:
            f.write("failed")

        print(
            "INTENTIONAL CRASH: testing checkpoint recovery",
            flush=True
        )

        raise RuntimeError(
            "Intentional failure for checkpoint recovery test"
        )

query = (
    stream.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

result = spark.sql(f"""
    SELECT
        COUNT(*) AS rows,
        COUNT(
            DISTINCT CONCAT(
                topic, ':',
                CAST(partition AS STRING), ':',
                CAST(offset AS STRING)
            )
        ) AS unique_kafka_positions
    FROM {TARGET}
""").first()

if result["rows"] != result["unique_kafka_positions"]:
    raise RuntimeError(
        "Duplicate Kafka positions detected after recovery"
    )

print(
    f'PASS CHECKPOINT RECOVERY: rows={result["rows"]}, '
    f'unique_kafka_positions={result["unique_kafka_positions"]}',
    flush=True
)

spark.stop()
