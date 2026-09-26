from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CommerceCreateDLQ")
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
    CREATE TABLE IF NOT EXISTS lakehouse.silver.dlq (
        source_topic STRING,
        event_key STRING,
        payload STRING,
        source_partition INT,
        source_offset BIGINT,
        error_type STRING,
        error_message STRING,
        quarantined_at TIMESTAMP
    )
    USING iceberg
    TBLPROPERTIES ('format-version' = '2')
""")

print("PASS DLQ TABLE CREATED", flush=True)

spark.stop()
