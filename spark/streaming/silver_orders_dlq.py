import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CommerceSilverOrdersDLQ")
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

# DLQ / quarantine table
spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.silver.dlq (
        source_topic STRING,
        event_key STRING,
        payload STRING,
        source_partition INT,
        source_offset BIGINT,
        event_timestamp TIMESTAMP,
        error_type STRING,
        error_message STRING,
        quarantined_at TIMESTAMP
    )
    USING iceberg
    TBLPROPERTIES ('format-version' = '2')
""")

# Parse Bronze order events while preserving the raw event.
spark.sql("""
    CREATE OR REPLACE TEMP VIEW order_events AS
    SELECT
        topic,
        event_key,
        payload,
        kafka_timestamp,

        TRY_CAST(
            get_json_object(event_key, '$.order_id')
            AS BIGINT
        ) AS order_id,

        get_json_object(payload, '$.op') AS op,

        TRY_CAST(
            get_json_object(payload, '$.after.customer_id')
            AS BIGINT
        ) AS customer_id,

        get_json_object(
            payload,
            '$.after.status'
        ) AS status,

        TRY_CAST(
            get_json_object(payload, '$.after.amount')
            AS DECIMAL(12,2)
        ) AS amount,

        get_json_object(
            payload,
            '$.after.currency'
        ) AS currency,

        TRY_CAST(
            get_json_object(payload, '$.after.created_at')
            AS TIMESTAMP
        ) AS created_at,

        TRY_CAST(
            get_json_object(payload, '$.after.updated_at')
            AS TIMESTAMP
        ) AS updated_at,

        partition AS source_partition,
        offset AS source_offset

    FROM lakehouse.bronze.commerce_events
    WHERE topic = 'shop.commerce.orders'
      AND payload IS NOT NULL
""")

# Assign one validation error to every invalid event.
spark.sql("""
    CREATE OR REPLACE TEMP VIEW classified_order_events AS
    SELECT *,
        CASE
            WHEN order_id IS NULL
                THEN 'order_id is missing or invalid'

            WHEN op IS NULL
                THEN 'CDC operation is missing'

            WHEN op NOT IN ('r', 'c', 'u', 'd')
                THEN CONCAT('Unsupported CDC operation: ', op)

            WHEN op <> 'd' AND customer_id IS NULL
                THEN 'customer_id is missing or invalid'

            WHEN op <> 'd' AND status IS NULL
                THEN 'status is missing'

            WHEN op <> 'd'
                 AND status NOT IN (
                     'CREATED',
                     'PAID',
                     'SHIPPED',
                     'CANCELLED'
                 )
                THEN CONCAT('Invalid status: ', status)

            WHEN op <> 'd' AND amount IS NULL
                THEN 'amount is missing or invalid'

            WHEN op <> 'd' AND amount < 0
                THEN 'amount cannot be negative'

            WHEN op <> 'd' AND currency IS NULL
                THEN 'currency is missing'

            WHEN op <> 'd' AND currency <> 'MAD'
                THEN CONCAT('Unsupported currency: ', currency)

            WHEN op <> 'd' AND created_at IS NULL
                THEN 'created_at is missing or invalid'

            WHEN op <> 'd' AND updated_at IS NULL
                THEN 'updated_at is missing or invalid'

            ELSE NULL
        END AS validation_error

    FROM order_events
""")

# Store invalid events exactly once according to Kafka position.
spark.sql("""
    MERGE INTO lakehouse.silver.dlq d

    USING (
        SELECT
            topic AS source_topic,
            event_key,
            payload,
            source_partition,
            source_offset,
            kafka_timestamp AS event_timestamp,
            'VALIDATION_ERROR' AS error_type,
            validation_error AS error_message
        FROM classified_order_events
        WHERE validation_error IS NOT NULL
    ) s

    ON d.source_topic = s.source_topic
       AND d.source_partition = s.source_partition
       AND d.source_offset = s.source_offset

    WHEN NOT MATCHED THEN INSERT (
        source_topic,
        event_key,
        payload,
        source_partition,
        source_offset,
        event_timestamp,
        error_type,
        error_message,
        quarantined_at
    )
    VALUES (
        s.source_topic,
        s.event_key,
        s.payload,
        s.source_partition,
        s.source_offset,
        s.event_timestamp,
        s.error_type,
        s.error_message,
        current_timestamp()
    )
""")

# Only clean events may reach Silver.
spark.sql("""
    CREATE OR REPLACE TEMP VIEW valid_order_events AS
    SELECT
        order_id,
        op,
        customer_id,
        status,
        amount,
        currency,
        created_at,
        updated_at,
        source_partition,
        source_offset
    FROM classified_order_events
    WHERE validation_error IS NULL
""")

moved_keys = spark.sql("""
    SELECT order_id
    FROM valid_order_events
    GROUP BY order_id
    HAVING COUNT(DISTINCT source_partition) > 1
""").count()

if moved_keys:
    raise RuntimeError(
        "A valid order appears in multiple Kafka partitions"
    )

spark.sql("""
    CREATE OR REPLACE TEMP VIEW latest_orders AS
    SELECT
        order_id,
        customer_id,
        status,
        amount,
        currency,
        created_at,
        updated_at,
        source_partition,
        source_offset,
        op
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY order_id
                   ORDER BY source_offset DESC
               ) AS rn
        FROM valid_order_events
    ) ranked
    WHERE rn = 1
""")

spark.sql("""
    MERGE INTO lakehouse.silver.orders t
    USING latest_orders s
    ON t.order_id = s.order_id

    WHEN MATCHED AND s.op = 'd'
    THEN DELETE

    WHEN MATCHED
         AND s.source_offset > t.source_offset
    THEN UPDATE SET
        customer_id = s.customer_id,
        status = s.status,
        amount = s.amount,
        currency = s.currency,
        created_at = s.created_at,
        updated_at = s.updated_at,
        source_partition = s.source_partition,
        source_offset = s.source_offset

    WHEN NOT MATCHED
         AND s.op <> 'd'
    THEN INSERT (
        order_id,
        customer_id,
        status,
        amount,
        currency,
        created_at,
        updated_at,
        source_partition,
        source_offset
    )
    VALUES (
        s.order_id,
        s.customer_id,
        s.status,
        s.amount,
        s.currency,
        s.created_at,
        s.updated_at,
        s.source_partition,
        s.source_offset
    )
""")

silver = spark.sql("""
    SELECT
        COUNT(*) AS rows,
        COUNT(DISTINCT order_id) AS unique_ids
    FROM lakehouse.silver.orders
""").first()


if silver["rows"] != silver["unique_ids"]:
    raise RuntimeError("Duplicate order IDs detected")

print(
    f'PASS SILVER ORDERS: rows={silver["rows"]}, '
    f'unique_ids={silver["unique_ids"]}',
    flush=True,
)

if os.environ.get("CHECK_DLQ_TEST_EVENT", "0") == "1":
    test_in_silver = spark.sql(
        "SELECT COUNT(*) FROM lakehouse.silver.orders "
        "WHERE order_id = 999999999"
    ).first()[0]

    test_in_dlq = spark.sql(
        "SELECT COUNT(*) FROM lakehouse.silver.dlq "
        "WHERE source_topic = 'shop.commerce.orders' "
        "AND source_partition = 99 "
        "AND source_offset = 999999999"
    ).first()[0]

    if test_in_silver != 0:
        raise RuntimeError("Invalid test order reached Silver")

    if test_in_dlq != 1:
        raise RuntimeError(
            f"Expected one test event in DLQ, found {test_in_dlq}"
        )

    print("PASS DLQ TEST: invalid order quarantined", flush=True)

spark.stop()
