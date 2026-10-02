import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import get_spark
from pyspark.sql import SparkSession

spark = get_spark("Minio_silver_relationship_checks")

spark.sparkContext.setLogLevel("WARN")

checks = {
    "order_items -> orders": """
        SELECT COUNT(*) AS n
        FROM minio_lake.silver.order_items oi
        LEFT ANTI JOIN minio_lake.silver.orders o
            ON oi.order_id = o.order_id
    """,

    "order_items -> products": """
        SELECT COUNT(*) AS n
        FROM minio_lake.silver.order_items oi
        LEFT ANTI JOIN minio_lake.silver.products p
            ON oi.product_id = p.product_id
    """,

    "payments -> orders": """
        SELECT COUNT(*) AS n
        FROM minio_lake.silver.payments p
        LEFT ANTI JOIN minio_lake.silver.orders o
            ON p.order_id = o.order_id
    """,

    "inventory -> products": """
        SELECT COUNT(*) AS n
        FROM minio_lake.silver.inventory i
        LEFT ANTI JOIN minio_lake.silver.products p
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
