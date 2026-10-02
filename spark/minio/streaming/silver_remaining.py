import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import get_spark
from pyspark.sql import SparkSession

spark = get_spark("Minio_silver_remaining")
spark.sparkContext.setLogLevel("WARN")

# Table definitions are fixed here, not supplied by external input.
tables = {
    "products": {
        "key": "product_id",
        "fields": {
            "sku": "STRING",
            "product_name": "STRING",
            "category": "STRING",
            "price": "DECIMAL(12,2)",
            "created_at": "TIMESTAMP",
            "updated_at": "TIMESTAMP",
        },
        "invalid": "price < 0",
    },
    "order_items": {
        "key": "order_item_id",
        "fields": {
            "order_id": "BIGINT",
            "product_id": "BIGINT",
            "quantity": "INT",
            "unit_price": "DECIMAL(12,2)",
            "created_at": "TIMESTAMP",
            "updated_at": "TIMESTAMP",
        },
        "invalid": "quantity <= 0 OR unit_price < 0",
    },
    "payments": {
        "key": "payment_id",
        "fields": {
            "order_id": "BIGINT",
            "payment_reference": "STRING",
            "amount": "DECIMAL(12,2)",
            "currency": "STRING",
            "method": "STRING",
            "status": "STRING",
            "created_at": "TIMESTAMP",
            "updated_at": "TIMESTAMP",
        },
        "invalid": """
            amount <= 0 OR currency <> 'MAD'
            OR method NOT IN ('CARD', 'TRANSFER', 'CASH')
            OR status NOT IN ('PENDING', 'SUCCESS', 'FAILED', 'REFUNDED')
        """,
    },
    "inventory": {
        "key": "product_id",
        "fields": {
            "quantity": "INT",
            "updated_at": "TIMESTAMP",
        },
        "invalid": "quantity < 0",
    },
}

spark.sql("CREATE NAMESPACE IF NOT EXISTS minio_lake.silver")

for table, spec in tables.items():
    key = spec["key"]
    fields = spec["fields"]
    target = f"minio_lake.silver.{table}"

    definitions = ", ".join(
        f"{name} {dtype}" for name, dtype in fields.items()
    )
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target} (
            {key} BIGINT,
            {definitions},
            source_partition INT,
            source_offset BIGINT
        )
        USING iceberg
        TBLPROPERTIES ('format-version' = '2')
    """)

    parsed_fields = ", ".join(
        f"TRY_CAST(get_json_object(payload, '$.after.{name}') "
        f"AS {dtype}) AS {name}"
        for name, dtype in fields.items()
    )

    events = spark.sql(f"""
        SELECT
            TRY_CAST(get_json_object(event_key, '$.{key}') AS BIGINT)
                AS {key},
            get_json_object(payload, '$.op') AS op,
            {parsed_fields},
            partition AS source_partition,
            offset AS source_offset
        FROM minio_lake.bronze.commerce_events
        WHERE topic = 'shop.commerce.{table}'
          AND payload IS NOT NULL
    """).cache()
    events.createOrReplaceTempView("events_to_process")

    if events.count() == 0:
        raise RuntimeError(
            f"No events for {table}: check Bronze ingestion first"
        )

    null_checks = " OR ".join(f"{name} IS NULL" for name in fields)
    invalid = spark.sql(f"""
        SELECT COUNT(*) AS n
        FROM events_to_process
        WHERE {key} IS NULL
           OR op IS NULL
           OR op NOT IN ('r', 'c', 'u', 'd')
           OR (
               op <> 'd'
               AND ({null_checks} OR {spec["invalid"]})
           )
    """).first()["n"]

    if invalid:
        raise RuntimeError(f"{table}: {invalid} invalid events")

    moved_keys = spark.sql(f"""
        SELECT {key}
        FROM events_to_process
        GROUP BY {key}
        HAVING COUNT(DISTINCT source_partition) > 1
    """).count()

    if moved_keys:
        raise RuntimeError(f"{table}: keys span multiple partitions")

    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW latest_events AS
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY {key}
                       ORDER BY source_offset DESC
                   ) AS rn
            FROM events_to_process
        ) ranked
        WHERE rn = 1
    """)

    columns = [key, *fields, "source_partition", "source_offset"]
    updates = ", ".join(
        f"{name} = s.{name}" for name in columns if name != key
    )
    insert_columns = ", ".join(columns)
    insert_values = ", ".join(f"s.{name}" for name in columns)

    spark.sql(f"""
        MERGE INTO {target} t
        USING latest_events s
        ON t.{key} = s.{key}

        WHEN MATCHED AND s.op = 'd' THEN DELETE

        WHEN MATCHED AND s.source_offset > t.source_offset
        THEN UPDATE SET {updates}

        WHEN NOT MATCHED AND s.op <> 'd'
        THEN INSERT ({insert_columns})
        VALUES ({insert_values})
    """)

    result = spark.sql(f"""
        SELECT COUNT(*) AS rows,
               COUNT(DISTINCT {key}) AS unique_ids
        FROM {target}
    """).first()

    if result["rows"] != result["unique_ids"]:
        raise RuntimeError(f"{table}: duplicate IDs in Silver")

    print(
        f'PASS {table}: rows={result["rows"]}, '
        f'unique_ids={result["unique_ids"]}',
        flush=True
    )
    events.unpersist()

print("REMAINING SILVER TABLES COMPLETE", flush=True)
spark.stop()
