import os
from pyspark.sql import SparkSession

print("Starting Iceberg / MinIO test...", flush=True)

builder = (
    SparkSession.builder
    .appName("MinIOIcebergSmokeTest")
    .config("spark.sql.shuffle.partitions", "2")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
)

settings = {
    "": "org.apache.iceberg.spark.SparkCatalog",
    ".catalog-impl": "org.apache.iceberg.jdbc.JdbcCatalog",
    ".uri": "jdbc:postgresql://postgres:5432/iceberg_catalog",
    ".jdbc.user": "commerce",
    ".jdbc.password": os.environ["PG_PASSWORD"],
    ".warehouse": "s3://commerce-lakehouse/warehouse",
    ".io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    ".s3.endpoint": "http://minio:9000",
    ".s3.path-style-access": "true",
    ".client.region": "us-east-1",
}

for suffix, value in settings.items():
    builder = builder.config("spark.sql.catalog.minio_lake" + suffix, value)

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

try:
    print("Creating test namespace and table...", flush=True)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS minio_lake.validation")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS minio_lake.validation.storage_test (
            id BIGINT,
            message STRING
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '2')
    """)

    print("Writing test row...", flush=True)
    spark.sql("""
        MERGE INTO minio_lake.validation.storage_test t
        USING (SELECT CAST(1 AS BIGINT) AS id, 'MinIO works' AS message) s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET message = s.message
        WHEN NOT MATCHED THEN INSERT *
    """)

    rows = spark.sql("""
        SELECT * FROM minio_lake.validation.storage_test WHERE id = 1
    """).collect()

    if len(rows) != 1 or rows[0]["message"] != "MinIO works":
        raise RuntimeError("Read-back validation failed")

    files = spark.sql("""
        SELECT file_path
        FROM minio_lake.validation.storage_test.files
    """).collect()

    if not files or not all(
        r["file_path"].startswith("s3://commerce-lakehouse/")
        for r in files
    ):
        raise RuntimeError("Unexpected storage location")

    print("PASS: Iceberg row written and read back from MinIO.", flush=True)
    for row in files:
        print(row["file_path"], flush=True)
finally:
    spark.stop()
