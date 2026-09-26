from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CommerceReconciliation")
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
    "orders_silver_vs_daily_sales": """
        SELECT CASE
            WHEN
                (SELECT COUNT(*) FROM lakehouse.silver.orders)
                =
                (SELECT COALESCE(SUM(orders), 0)
                 FROM lakehouse.gold.daily_sales)
            THEN 0 ELSE 1
        END AS n
    """,

    "order_items_silver_vs_fact_orders": """
        SELECT CASE
            WHEN
                (SELECT COUNT(*) FROM lakehouse.silver.order_items)
                =
                (SELECT COUNT(*) FROM lakehouse.gold.fact_orders)
            THEN 0 ELSE 1
        END AS n
    """,

    "customers_silver_vs_dim_customer": """
        SELECT CASE
            WHEN
                (SELECT COUNT(*) FROM lakehouse.silver.customers)
                =
                (SELECT COUNT(*) FROM lakehouse.gold.dim_customer)
            THEN 0 ELSE 1
        END AS n
    """,

    "products_silver_vs_dim_product": """
        SELECT CASE
            WHEN
                (SELECT COUNT(*) FROM lakehouse.silver.products)
                =
                (SELECT COUNT(*) FROM lakehouse.gold.dim_product)
            THEN 0 ELSE 1
        END AS n
    """,

    "payments_silver_vs_fact_payments": """
        SELECT CASE
            WHEN
                (SELECT COUNT(*) FROM lakehouse.silver.payments)
                =
                (SELECT COUNT(*) FROM lakehouse.gold.fact_payments)
            THEN 0 ELSE 1
        END AS n
    """,

    "revenue_silver_vs_gold": """
        SELECT CASE
            WHEN ABS(
                (SELECT COALESCE(SUM(amount), 0)
                 FROM lakehouse.silver.orders)
                -
                (SELECT COALESCE(SUM(total_revenue), 0)
                 FROM lakehouse.gold.daily_sales)
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
    (SELECT COUNT(*) FROM lakehouse.silver.orders) AS orders,
    (SELECT COUNT(*) FROM lakehouse.silver.order_items) AS order_items,
    (SELECT COUNT(*) FROM lakehouse.silver.customers) AS customers,
    (SELECT COUNT(*) FROM lakehouse.silver.products) AS products,
    (SELECT COUNT(*) FROM lakehouse.silver.payments) AS payments,
    (SELECT SUM(amount) FROM lakehouse.silver.orders) AS revenue
""").show(truncate=False)


print("")
print("========== GOLD METRICS ==========")

spark.sql("""
SELECT
    (SELECT SUM(orders) FROM lakehouse.gold.daily_sales) AS orders,
    (SELECT COUNT(*) FROM lakehouse.gold.fact_orders) AS fact_orders,
    (SELECT COUNT(*) FROM lakehouse.gold.dim_customer) AS customers,
    (SELECT COUNT(*) FROM lakehouse.gold.dim_product) AS products,
    (SELECT COUNT(*) FROM lakehouse.gold.fact_payments) AS payments,
    (SELECT SUM(total_revenue) FROM lakehouse.gold.daily_sales) AS revenue
""").show(truncate=False)


if failed:
    raise RuntimeError("RECONCILIATION FAILED")

print("RECONCILIATION COMPLETE")

spark.stop()
