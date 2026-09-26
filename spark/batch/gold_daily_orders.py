from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CommerceGoldDailyOrders")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.session.timeZone", "UTC")
    .config(
        "spark.sql.catalog.lakehouse",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config("spark.sql.catalog.lakehouse.warehouse", "/data/warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

spark.sql("""
    CREATE OR REPLACE TABLE lakehouse.gold.daily_orders
    USING iceberg
    AS
    SELECT
        CAST(created_at AS DATE) AS order_date,
        status,
        currency,
        COUNT(*) AS order_count,
        SUM(amount) AS total_order_amount,
        CAST(AVG(amount) AS DECIMAL(14,2)) AS average_order_amount
    FROM lakehouse.silver.orders
    GROUP BY CAST(created_at AS DATE), status, currency
""")

print("GOLD DAILY ORDERS COMPLETE", flush=True)

spark.sql("""
    SELECT *
    FROM lakehouse.gold.daily_orders
    ORDER BY order_date, status, currency
""").show(100, truncate=False)

silver = spark.sql("""
    SELECT COUNT(*) AS n, COALESCE(SUM(amount), 0) AS total
    FROM lakehouse.silver.orders
""").first()

gold = spark.sql("""
    SELECT COALESCE(SUM(order_count), 0) AS n,
           COALESCE(SUM(total_order_amount), 0) AS total
    FROM lakehouse.gold.daily_orders
""").first()

if silver["n"] != gold["n"] or silver["total"] != gold["total"]:
    raise RuntimeError("Gold totals do not match Silver")

print("PASS: Gold order count and amount match Silver.", flush=True)
spark.stop()
