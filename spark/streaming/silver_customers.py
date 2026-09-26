from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CommerceSilverCustomers")
    .config("spark.sql.shuffle.partitions", "2")
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
    CREATE TABLE IF NOT EXISTS lakehouse.silver.customers (
        customer_id BIGINT,
        full_name STRING,
        email STRING,
        city STRING,
        country STRING,
        source_partition INT,
        source_offset BIGINT
    )
    USING iceberg
    TBLPROPERTIES ('format-version' = '2')
""")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW customer_events AS
    SELECT
        CAST(get_json_object(event_key, '$.customer_id') AS BIGINT)
            AS customer_id,
        get_json_object(payload, '$.op') AS op,
        get_json_object(payload, '$.after.full_name') AS full_name,
        get_json_object(payload, '$.after.email') AS email,
        get_json_object(payload, '$.after.city') AS city,
        get_json_object(payload, '$.after.country') AS country,
        partition AS source_partition,
        offset AS source_offset
    FROM lakehouse.bronze.commerce_events
    WHERE topic = 'shop.commerce.customers'
      AND payload IS NOT NULL
""")

invalid = spark.sql("""
    SELECT COUNT(*) AS n
    FROM customer_events
    WHERE customer_id IS NULL
       OR op IS NULL
       OR op NOT IN ('r', 'c', 'u', 'd')
       OR (
           op <> 'd'
           AND (full_name IS NULL OR email IS NULL
                OR city IS NULL OR country IS NULL)
       )
""").first()["n"]

if invalid:
    raise RuntimeError(f"Invalid customer events: {invalid}")

# Offsets are comparable only within the same Kafka partition.
moved_keys = spark.sql("""
    SELECT customer_id
    FROM customer_events
    GROUP BY customer_id
    HAVING COUNT(DISTINCT source_partition) > 1
""").count()

if moved_keys:
    raise RuntimeError(
        "A customer appears in multiple partitions. "
        "Review event ordering before continuing."
    )

spark.sql("""
    CREATE OR REPLACE TEMP VIEW latest_customers AS
    SELECT customer_id, op, full_name, email, city, country,
           source_partition, source_offset
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY customer_id
                   ORDER BY source_offset DESC
               ) AS rn
        FROM customer_events
    ) ranked
    WHERE rn = 1
""")

spark.sql("""
    MERGE INTO lakehouse.silver.customers t
    USING latest_customers s
    ON t.customer_id = s.customer_id

    WHEN MATCHED AND s.op = 'd' THEN DELETE

    WHEN MATCHED AND s.source_offset > t.source_offset
    THEN UPDATE SET
        full_name = s.full_name,
        email = s.email,
        city = s.city,
        country = s.country,
        source_partition = s.source_partition,
        source_offset = s.source_offset

    WHEN NOT MATCHED AND s.op <> 'd'
    THEN INSERT (
        customer_id, full_name, email, city, country,
        source_partition, source_offset
    )
    VALUES (
        s.customer_id, s.full_name, s.email, s.city, s.country,
        s.source_partition, s.source_offset
    )
""")

print("SILVER CUSTOMERS COMPLETE", flush=True)

spark.sql("""
    SELECT COUNT(*) AS customers,
           COUNT(DISTINCT customer_id) AS unique_customer_ids
    FROM lakehouse.silver.customers
""").show()

spark.sql("""
    SELECT customer_id, full_name, city
    FROM lakehouse.silver.customers
    WHERE email = 'client.demo@example.com'
""").show(truncate=False)

spark.stop()
