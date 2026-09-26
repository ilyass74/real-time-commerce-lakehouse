from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CommerceSilverRelationshipChecks")
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

checks = {
    "order_items -> orders": """
        SELECT COUNT(*) AS n
        FROM lakehouse.silver.order_items oi
        LEFT ANTI JOIN lakehouse.silver.orders o
            ON oi.order_id = o.order_id
    """,

    "order_items -> products": """
        SELECT COUNT(*) AS n
        FROM lakehouse.silver.order_items oi
        LEFT ANTI JOIN lakehouse.silver.products p
            ON oi.product_id = p.product_id
    """,

    "payments -> orders": """
        SELECT COUNT(*) AS n
        FROM lakehouse.silver.payments p
        LEFT ANTI JOIN lakehouse.silver.orders o
            ON p.order_id = o.order_id
    """,

    "inventory -> products": """
        SELECT COUNT(*) AS n
        FROM lakehouse.silver.inventory i
        LEFT ANTI JOIN lakehouse.silver.products p
            ON i.product_id = p.product_id
    """
}

for name, query in checks.items():
    n = spark.sql(query).first()["n"]

    if n != 0:
        raise RuntimeError(f"FAIL {name}: {n} orphan rows")

    print(f"PASS {name}: orphan_rows=0", flush=True)

print("SILVER RELATIONSHIP CHECKS COMPLETE", flush=True)

spark.stop()
