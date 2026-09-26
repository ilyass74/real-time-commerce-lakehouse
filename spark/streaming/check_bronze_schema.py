from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("BronzeSchemaCheck")
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

spark.sql("DESCRIBE lakehouse.bronze.commerce_events").show(
    100, truncate=False
)

spark.stop()
