#!/usr/bin/env bash
set -euo pipefail
: "${DBT_ENV_SECRET_PG_PASSWORD:?Missing database password}"
: "${AWS_ACCESS_KEY_ID:?Missing MinIO user}"
: "${AWS_SECRET_ACCESS_KEY:?Missing MinIO password}"
# Keep HOME provided by the image; isolate dbt output from the original run.
export PYSPARK_SUBMIT_ARGS="--master local[2] --driver-memory 1g --conf spark.jars.ivy=/data/ivy --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.2,org.apache.iceberg:iceberg-aws-bundle:1.7.2,org.postgresql:postgresql:42.7.4 pyspark-shell"
export DBT_TARGET_PATH=/work/dbt/target-minio
export DBT_LOG_PATH=/work/dbt/logs-minio
cd /work/dbt
case "${1:-}" in
  run|test) dbt "$1" --profiles-dir /work/dbt/minio --no-partial-parse ;;
  show) dbt show --profiles-dir /work/dbt/minio --no-partial-parse --inline 'SELECT SUM(orders) AS orders, SUM(units_sold) AS units, SUM(total_revenue) AS revenue FROM gold.daily_sales' ;;
  *) echo 'Expected run, test, or show' >&2; exit 2 ;;
esac
