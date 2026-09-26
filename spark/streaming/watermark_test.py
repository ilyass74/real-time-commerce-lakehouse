import os
import json
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType
)

INPUT = "/data/watermark_input"
CHECKPOINT = "/data/checkpoints/watermark_test"
TARGET = "lakehouse.test.watermark_events"

RESET = os.environ.get("RESET_TEST", "0") == "1"
STAGE = os.environ.get("STAGE", "1")

spark = (
    SparkSession.builder
    .appName("CommerceWatermarkTest")
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
    shutil.rmtree(INPUT, ignore_errors=True)
    shutil.rmtree(CHECKPOINT, ignore_errors=True)

    print("RESET WATERMARK TEST COMPLETE", flush=True)

os.makedirs(INPUT, exist_ok=True)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET} (
        event_id STRING,
        event_time TIMESTAMP,
        description STRING
    )
    USING iceberg
    TBLPROPERTIES ('format-version' = '2')
""")

def write_events(filename, events):
    path = os.path.join(INPUT, filename)

    if os.path.exists(path):
        return

    with open(path, "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")


if STAGE == "1":

    # Establish the maximum event time at 12:00.
    write_events(
        "stage1.json",
        [
            {
                "event_id": "anchor",
                "event_time": "2026-09-25T12:00:00Z",
                "description": "reference event"
            }
        ]
    )

elif STAGE == "2":

    # After stage 1, the intended watermark is approximately:
    # 12:00 - 20 minutes = 11:40.
    #
    # 11:55 -> 5 minutes late  -> acceptable
    # 11:45 -> 15 minutes late -> acceptable
    # 11:30 -> 30 minutes late -> too late

    write_events(
        "stage2.json",
        [
            {
                "event_id": "late_5m",
                "event_time": "2026-09-25T11:55:00Z",
                "description": "5 minutes late"
            },
            {
                "event_id": "late_15m",
                "event_time": "2026-09-25T11:45:00Z",
                "description": "15 minutes late"
            },
            {
                "event_id": "late_30m",
                "event_time": "2026-09-25T11:30:00Z",
                "description": "30 minutes late"
            }
        ]
    )

else:
    raise RuntimeError(f"Unknown STAGE={STAGE}")

schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_time", TimestampType(), False),
    StructField("description", StringType(), False),
])

stream = (
    spark.readStream
    .schema(schema)
    .json(INPUT)
)

watermarked = (
    stream
    .withWatermark("event_time", "20 minutes")
    .dropDuplicatesWithinWatermark(["event_id"])
)

def process_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    batch_df.createOrReplaceGlobalTempView(
        "watermark_batch"
    )

    spark.sql(f"""
        MERGE INTO {TARGET} t

        USING global_temp.watermark_batch s

        ON t.event_id = s.event_id

        WHEN NOT MATCHED THEN
        INSERT (
            event_id,
            event_time,
            description
        )
        VALUES (
            s.event_id,
            s.event_time,
            s.description
        )
    """)

    print(
        f"WATERMARK BATCH {batch_id} COMPLETE",
        flush=True
    )

query = (
    watermarked.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

spark.sql(f"""
    SELECT
        event_id,
        event_time,
        description
    FROM {TARGET}
    ORDER BY event_time DESC
""").show(truncate=False)

if STAGE == "1":

    n = spark.sql(f"""
        SELECT COUNT(*) AS n
        FROM {TARGET}
    """).first()["n"]

    if n != 1:
        raise RuntimeError(
            f"Stage 1 expected 1 event, found {n}"
        )

    print(
        "PASS WATERMARK STAGE 1: reference event processed",
        flush=True
    )

elif STAGE == "2":

    accepted_5m = spark.sql(f"""
        SELECT COUNT(*) AS n
        FROM {TARGET}
        WHERE event_id = 'late_5m'
    """).first()["n"]

    accepted_15m = spark.sql(f"""
        SELECT COUNT(*) AS n
        FROM {TARGET}
        WHERE event_id = 'late_15m'
    """).first()["n"]

    accepted_30m = spark.sql(f"""
        SELECT COUNT(*) AS n
        FROM {TARGET}
        WHERE event_id = 'late_30m'
    """).first()["n"]

    total = spark.sql(f"""
        SELECT COUNT(*) AS n
        FROM {TARGET}
    """).first()["n"]

    if accepted_5m != 1:
        raise RuntimeError(
            "5-minute late event was not accepted"
        )

    if accepted_15m != 1:
        raise RuntimeError(
            "15-minute late event was not accepted"
        )

    if accepted_30m != 0:
        raise RuntimeError(
            "30-minute late event should have been dropped"
        )

    if total != 3:
        raise RuntimeError(
            f"Expected 3 accepted events, found {total}"
        )

    print(
        "PASS: 5-minute late event accepted",
        flush=True
    )

    print(
        "PASS: 15-minute late event accepted",
        flush=True
    )

    print(
        "PASS: 30-minute late event dropped by watermark",
        flush=True
    )

    print(
        "WATERMARK / LATE-DATA TEST COMPLETE",
        flush=True
    )

spark.stop()
