import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import get_spark
import os
from decimal import Decimal

from pyspark.sql import SparkSession


spark = get_spark("Minio_source_reconciliation")

spark.sparkContext.setLogLevel("WARN")


PG_URL = os.environ["PG_URL"]
PG_USER = os.environ["PG_USER"]
PG_PASSWORD = os.environ["PG_PASSWORD"]


def postgres_query(sql):
    return (
        spark.read
        .format("jdbc")
        .option("url", PG_URL)
        .option("query", sql)
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


tables = [
    "customers",
    "orders",
    "products",
    "order_items",
    "payments",
    "inventory",
]

failed = False

print("")
print("========== POSTGRESQL -> SILVER RECONCILIATION ==========")

for table in tables:

    source_count = (
        postgres_query(
            f"SELECT COUNT(*)::BIGINT AS row_count "
            f"FROM commerce.{table}"
        )
        .first()["row_count"]
    )

    silver_count = spark.sql(
        f"SELECT COUNT(*) AS row_count "
        f"FROM minio_lake.silver.{table}"
    ).first()["row_count"]

    if source_count == silver_count:
        print(
            f"PASS: {table} "
            f"source={source_count} silver={silver_count}"
        )
    else:
        print(
            f"FAIL: {table} "
            f"source={source_count} silver={silver_count}"
        )
        failed = True


source_revenue = postgres_query("""
    SELECT COALESCE(SUM(amount), 0) AS revenue
    FROM commerce.orders
""").first()["revenue"]

silver_revenue = spark.sql("""
    SELECT COALESCE(SUM(amount), 0) AS revenue
    FROM minio_lake.silver.orders
""").first()["revenue"]

source_revenue = Decimal(str(source_revenue))
silver_revenue = Decimal(str(silver_revenue))

if abs(source_revenue - silver_revenue) <= Decimal("0.01"):
    print(
        "PASS: orders_revenue "
        f"source={source_revenue} silver={silver_revenue}"
    )
else:
    print(
        "FAIL: orders_revenue "
        f"source={source_revenue} silver={silver_revenue}"
    )
    failed = True


if failed:
    raise RuntimeError("SOURCE RECONCILIATION FAILED")

print("SOURCE RECONCILIATION COMPLETE")

spark.stop()
