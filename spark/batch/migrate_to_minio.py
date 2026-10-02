import os
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder
    .appName("CopyLakehouseToMinIO")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.session.timeZone", "UTC")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config("spark.sql.catalog.local_lake", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local_lake.type", "hadoop")
    .config("spark.sql.catalog.local_lake.warehouse", "/data/warehouse")
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

tables = {
    "bronze": ["commerce_events"],
    "silver": [
        "customers", "dlq", "inventory", "orders",
        "order_items", "payments", "products",
    ],
    "gold": [
        "customer_metrics", "daily_orders", "daily_sales",
        "dim_customer", "dim_date", "dim_location", "dim_product",
        "fact_orders", "fact_payments", "inventory_kpis",
        "product_performance", "sales_by_region",
    ],
}

def current_snapshot(table):
    spark.catalog.refreshTable(table)
    rows = spark.sql(
        f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'"
    ).collect()
    return rows[0]["snapshot_id"] if rows else None

try:
    # Capture all source snapshots before writing anything.
    snapshots = {}
    for namespace, names in tables.items():
        for name in names:
            source = f"local_lake.{namespace}.{name}"
            snapshots[source] = current_snapshot(source)

    done = 0
    for namespace, names in tables.items():
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS minio_lake.{namespace}")

        for name in names:
            source = f"local_lake.{namespace}.{name}"
            target = f"minio_lake.{namespace}.{name}"
            snapshot = snapshots[source]

            print(f"[{done + 1}/20] COPYING {namespace}.{name}", flush=True)

            if snapshot is None:
                original = spark.table(source).limit(0)
            else:
                original = (
                    spark.read
                    .option("snapshot-id", str(snapshot))
                    .table(source)
                )

            if not spark.catalog.tableExists(target):
                (
                    original.writeTo(target)
                    .using("iceberg")
                    .tableProperty("format-version", "2")
                    .create()
                )
            else:
                print("Target exists; validating without overwriting.", flush=True)

            copied = spark.table(target)
            original_schema = [(f.name, f.dataType) for f in original.schema]
            copied_schema = [(f.name, f.dataType) for f in copied.schema]
            if original_schema != copied_schema:
                raise RuntimeError(f"Schema mismatch: {target}")

            original_count = original.count()
            copied_count = copied.count()
            if original_count != copied_count:
                raise RuntimeError(
                    f"Count mismatch: {target}: "
                    f"{original_count} vs {copied_count}"
                )

            # Compare values in both directions, including duplicate rows.
            different = (
                original.exceptAll(copied)
                .union(copied.exceptAll(original))
                .limit(1)
                .count()
            )
            if different:
                raise RuntimeError(f"Row values differ: {target}")

            files = spark.sql(
                f"SELECT file_path FROM {target}.files"
            ).collect()
            if any(
                not row["file_path"].startswith("s3://commerce-lakehouse/")
                for row in files
            ):
                raise RuntimeError(f"Unexpected file location: {target}")

            done += 1
            print(
                f"PASS [{done}/20] {namespace}.{name}: "
                f"{copied_count} rows, exact data match",
                flush=True,
            )

    for source, snapshot in snapshots.items():
        if current_snapshot(source) != snapshot:
            raise RuntimeError(
                f"Source changed during migration: {source}. "
                "Do not switch the pipeline yet."
            )

    print("PASS: ALL 20 TABLES COPIED AND VALIDATED.", flush=True)
    print("Pipeline cutover is still pending.", flush=True)
finally:
    spark.stop()
