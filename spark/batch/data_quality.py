from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CommerceDataQuality")
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
    "orders_duplicate_ids": """
        SELECT COUNT(*) n
        FROM (
            SELECT order_id
            FROM lakehouse.silver.orders
            GROUP BY order_id
            HAVING COUNT(*) > 1
        )
    """,

    "orders_invalid": """
        SELECT COUNT(*) n
        FROM lakehouse.silver.orders
        WHERE order_id IS NULL
           OR customer_id IS NULL
           OR amount < 0
           OR currency <> 'MAD'
           OR status NOT IN ('CREATED','PAID','SHIPPED','CANCELLED')
    """,

    "orphan_order_items_orders": """
        SELECT COUNT(*) n
        FROM lakehouse.silver.order_items i
        LEFT ANTI JOIN lakehouse.silver.orders o
          ON i.order_id = o.order_id
    """,

    "orphan_order_items_products": """
        SELECT COUNT(*) n
        FROM lakehouse.silver.order_items i
        LEFT ANTI JOIN lakehouse.silver.products p
          ON i.product_id = p.product_id
    """,

    "orphan_payments": """
        SELECT COUNT(*) n
        FROM lakehouse.silver.payments p
        LEFT ANTI JOIN lakehouse.silver.orders o
          ON p.order_id = o.order_id
    """,

    "orphan_inventory": """
        SELECT COUNT(*) n
        FROM lakehouse.silver.inventory i
        LEFT ANTI JOIN lakehouse.silver.products p
          ON i.product_id = p.product_id
    """
}

failed = False

for name, sql in checks.items():
    value = spark.sql(sql).first()["n"]

    if value == 0:
        print(f"PASS: {name}")
    else:
        print(f"FAIL: {name} = {value}")
        failed = True

counts = spark.sql("""
SELECT 'customers' table_name, COUNT(*) rows FROM lakehouse.silver.customers
UNION ALL
SELECT 'orders', COUNT(*) FROM lakehouse.silver.orders
UNION ALL
SELECT 'products', COUNT(*) FROM lakehouse.silver.products
UNION ALL
SELECT 'order_items', COUNT(*) FROM lakehouse.silver.order_items
UNION ALL
SELECT 'payments', COUNT(*) FROM lakehouse.silver.payments
UNION ALL
SELECT 'inventory', COUNT(*) FROM lakehouse.silver.inventory
""")

counts.show(truncate=False)

if failed:
    raise RuntimeError("DATA QUALITY FAILED")

print("DATA QUALITY COMPLETE")
spark.stop()
