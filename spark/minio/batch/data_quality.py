import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import get_spark
from pyspark.sql import SparkSession

spark = get_spark("Minio_data_quality")

spark.sparkContext.setLogLevel("WARN")

checks = {
    "orders_duplicate_ids": """
        SELECT COUNT(*) n
        FROM (
            SELECT order_id
            FROM minio_lake.silver.orders
            GROUP BY order_id
            HAVING COUNT(*) > 1
        )
    """,

    "orders_invalid": """
        SELECT COUNT(*) n
        FROM minio_lake.silver.orders
        WHERE order_id IS NULL
           OR customer_id IS NULL
           OR amount < 0
           OR currency <> 'MAD'
           OR status NOT IN ('CREATED','PAID','SHIPPED','CANCELLED')
    """,

    "orphan_order_items_orders": """
        SELECT COUNT(*) n
        FROM minio_lake.silver.order_items i
        LEFT ANTI JOIN minio_lake.silver.orders o
          ON i.order_id = o.order_id
    """,

    "orphan_order_items_products": """
        SELECT COUNT(*) n
        FROM minio_lake.silver.order_items i
        LEFT ANTI JOIN minio_lake.silver.products p
          ON i.product_id = p.product_id
    """,

    "orphan_payments": """
        SELECT COUNT(*) n
        FROM minio_lake.silver.payments p
        LEFT ANTI JOIN minio_lake.silver.orders o
          ON p.order_id = o.order_id
    """,

    "orphan_inventory": """
        SELECT COUNT(*) n
        FROM minio_lake.silver.inventory i
        LEFT ANTI JOIN minio_lake.silver.products p
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
SELECT 'customers' table_name, COUNT(*) rows FROM minio_lake.silver.customers
UNION ALL
SELECT 'orders', COUNT(*) FROM minio_lake.silver.orders
UNION ALL
SELECT 'products', COUNT(*) FROM minio_lake.silver.products
UNION ALL
SELECT 'order_items', COUNT(*) FROM minio_lake.silver.order_items
UNION ALL
SELECT 'payments', COUNT(*) FROM minio_lake.silver.payments
UNION ALL
SELECT 'inventory', COUNT(*) FROM minio_lake.silver.inventory
""")

counts.show(truncate=False)

if failed:
    raise RuntimeError("DATA QUALITY FAILED")

print("DATA QUALITY COMPLETE")
spark.stop()
