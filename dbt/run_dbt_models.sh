#!/usr/bin/env bash
set -e

export HOME=/tmp/dbt-home
mkdir -p "$HOME"
export PATH="$HOME/.local/bin:$PATH"

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

dbt run --profiles-dir .

echo "DBT MODELS COMPLETE"
