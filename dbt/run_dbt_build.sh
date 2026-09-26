#!/usr/bin/env bash
set -e

export HOME=/tmp/dbt-home
mkdir -p "$HOME"
export PATH="$HOME/.local/bin:$PATH"

echo "===== INSTALLING DBT STACK ====="

python3 -m pip install \
  --user \
  --no-cache-dir \
  "isodate==0.6.1" \
  "dbt-core==1.8.6" \
  "dbt-spark[session]==1.8.0" \
  "pyspark==3.5.7"

echo "===== VERSIONS ====="

dbt --version

python3 -c "import pyspark; print('PySpark:', pyspark.__version__)"

export PYSPARK_SUBMIT_ARGS="\
--master local[2] \
--conf spark.jars.ivy=/data/ivy \
--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.2 \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.lakehouse.type=hadoop \
--conf spark.sql.catalog.lakehouse.warehouse=/data/warehouse \
--conf spark.sql.defaultCatalog=lakehouse \
pyspark-shell"

cd /work/dbt

echo "===== DBT BUILD ====="

dbt build --profiles-dir .

echo "===== GOLD COUNTS ====="

dbt show \
  --profiles-dir . \
  --inline "
    SELECT
        (SELECT COUNT(*) FROM gold.dim_customer) AS customers,
        (SELECT COUNT(*) FROM gold.dim_product) AS products,
        (SELECT COUNT(*) FROM gold.fact_orders) AS fact_rows,
        (SELECT COUNT(*) FROM gold.fact_payments) AS payments
  "

echo "===== DAILY SALES ====="

dbt show \
  --profiles-dir . \
  --inline "
    SELECT *
    FROM gold.daily_sales
    ORDER BY order_date
  "

echo "GOLD DBT BUILD COMPLETE"
