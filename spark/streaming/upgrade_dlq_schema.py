from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("UpgradeDLQSchema")
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

columns = spark.table("lakehouse.silver.dlq").columns

if "event_timestamp" not in columns:
    spark.sql("""
        ALTER TABLE lakehouse.silver.dlq
        ADD COLUMN event_timestamp TIMESTAMP
    """)
    print("PASS: event_timestamp added to DLQ", flush=True)
else:
    print("PASS: event_timestamp already exists", flush=True)

spark.sql("""
    DESCRIBE lakehouse.silver.dlq
""").show(100, truncate=False)

spark.stop()
