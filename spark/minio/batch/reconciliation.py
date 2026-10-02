import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import get_spark
from pyspark.sql import SparkSession

spark = get_spark("Minio_reconciliation")

spark.sparkContext.setLogLevel("WARN")

checks = {
    "orders_silver_vs_daily_sales": """
        SELECT CASE
            WHEN
                (SELECT COUNT(*) FROM minio_lake.silver.orders)
                =
                (SELECT COALESCE(SUM(orders), 0)
                 FROM minio_lake.gold.daily_sales)
            THEN 0 ELSE 1
        END AS n
    """,

    "order_items_silver_vs_fact_orders": """
        SELECT CASE
            WHEN
                (SELECT COUNT(*) FROM minio_lake.silver.order_items)
                =
                (SELECT COUNT(*) FROM minio_lake.gold.fact_orders)
            THEN 0 ELSE 1
        END AS n
    """,

    "customers_silver_vs_dim_customer": """
        SELECT CASE
            WHEN
                (SELECT COUNT(*) FROM minio_lake.silver.customers)
                =
                (SELECT COUNT(*) FROM minio_lake.gold.dim_customer)
            THEN 0 ELSE 1
        END AS n
    """,

    "products_silver_vs_dim_product": """
        SELECT CASE
            WHEN
                (SELECT COUNT(*) FROM minio_lake.silver.products)
                =
                (SELECT COUNT(*) FROM minio_lake.gold.dim_product)
            THEN 0 ELSE 1
        END AS n
    """,

    "payments_silver_vs_fact_payments": """
        SELECT CASE
            WHEN
                (SELECT COUNT(*) FROM minio_lake.silver.payments)
                =
                (SELECT COUNT(*) FROM minio_lake.gold.fact_payments)
            THEN 0 ELSE 1
        END AS n
    """,

    "revenue_silver_vs_gold": """
        SELECT CASE
            WHEN ABS(
                (SELECT COALESCE(SUM(amount), 0)
                 FROM minio_lake.silver.orders)
                -
                (SELECT COALESCE(SUM(total_revenue), 0)
                 FROM minio_lake.gold.daily_sales)
            ) <= 0.01
            THEN 0 ELSE 1
        END AS n
    """
}

failed = False

print("")
print("========== RECONCILIATION CHECKS ==========")

for name, sql in checks.items():
    value = spark.sql(sql).first()["n"]

    if value == 0:
        print(f"PASS: {name}")
    else:
        print(f"FAIL: {name}")
        failed = True


print("")
print("========== SILVER METRICS ==========")

spark.sql("""
SELECT
    (SELECT COUNT(*) FROM minio_lake.silver.orders) AS orders,
    (SELECT COUNT(*) FROM minio_lake.silver.order_items) AS order_items,
    (SELECT COUNT(*) FROM minio_lake.silver.customers) AS customers,
    (SELECT COUNT(*) FROM minio_lake.silver.products) AS products,
    (SELECT COUNT(*) FROM minio_lake.silver.payments) AS payments,
    (SELECT SUM(amount) FROM minio_lake.silver.orders) AS revenue
""").show(truncate=False)


print("")
print("========== GOLD METRICS ==========")

spark.sql("""
SELECT
    (SELECT SUM(orders) FROM minio_lake.gold.daily_sales) AS orders,
    (SELECT COUNT(*) FROM minio_lake.gold.fact_orders) AS fact_orders,
    (SELECT COUNT(*) FROM minio_lake.gold.dim_customer) AS customers,
    (SELECT COUNT(*) FROM minio_lake.gold.dim_product) AS products,
    (SELECT COUNT(*) FROM minio_lake.gold.fact_payments) AS payments,
    (SELECT SUM(total_revenue) FROM minio_lake.gold.daily_sales) AS revenue
""").show(truncate=False)


if failed:
    raise RuntimeError("RECONCILIATION FAILED")

print("RECONCILIATION COMPLETE")

spark.stop()
