import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import get_spark
from pyspark.sql import SparkSession
from urllib.request import Request, urlopen
import re

spark = get_spark("Minio_publish_metrics")

spark.sparkContext.setLogLevel("WARN")


def scalar(sql):
    return spark.sql(sql).first()[0]


# ============================================================
# Lakehouse metrics
# ============================================================

silver_orders = scalar(
    "SELECT COUNT(*) FROM minio_lake.silver.orders"
)

silver_order_items = scalar(
    "SELECT COUNT(*) FROM minio_lake.silver.order_items"
)

silver_customers = scalar(
    "SELECT COUNT(*) FROM minio_lake.silver.customers"
)

silver_products = scalar(
    "SELECT COUNT(*) FROM minio_lake.silver.products"
)

silver_payments = scalar(
    "SELECT COUNT(*) FROM minio_lake.silver.payments"
)

silver_inventory = scalar(
    "SELECT COUNT(*) FROM minio_lake.silver.inventory"
)

bronze_events = scalar(
    "SELECT COUNT(*) FROM minio_lake.bronze.commerce_events"
)

latency = spark.sql("""
    SELECT
        COALESCE(
            AVG(
                unix_timestamp(ingestion_time)
                - unix_timestamp(kafka_timestamp)
            ),
            0
        ) AS avg_latency_seconds,
        COALESCE(
            MAX(
                unix_timestamp(ingestion_time)
                - unix_timestamp(kafka_timestamp)
            ),
            0
        ) AS max_latency_seconds
    FROM minio_lake.bronze.commerce_events
""").first()

avg_processing_latency = latency["avg_latency_seconds"]
max_processing_latency = latency["max_latency_seconds"]

dlq_size = scalar(
    "SELECT COUNT(*) FROM minio_lake.silver.dlq"
)

gold = spark.sql("""
    SELECT
        COALESCE(SUM(orders), 0) AS orders,
        COALESCE(SUM(total_revenue), 0) AS revenue
    FROM minio_lake.gold.daily_sales
""").first()

gold_orders = gold["orders"]
gold_revenue = gold["revenue"]


# ============================================================
# Kafka broker offsets from kafka-exporter
# ============================================================

exporter_url = "http://kafka-exporter:9308/metrics"

response = urlopen(exporter_url, timeout=15)
exporter_text = response.read().decode("utf-8")

kafka_offsets = {}

for line in exporter_text.splitlines():

    if not line.startswith("kafka_topic_partition_current_offset{"):
        continue

    metric_part, value_part = line.rsplit(" ", 1)

    labels_text = metric_part[
        metric_part.find("{") + 1:
        metric_part.rfind("}")
    ]

    labels = dict(
        re.findall(r'([a-zA-Z_][a-zA-Z0-9_]*)="([^"]*)"', labels_text)
    )

    topic = labels.get("topic")
    partition = labels.get("partition")

    if not topic or partition is None:
        continue

    if not topic.startswith("shop.commerce."):
        continue

    kafka_offsets[(topic, int(partition))] = int(float(value_part))


# ============================================================
# Bronze processed offsets
# ============================================================

bronze_rows = spark.sql("""
    SELECT
        topic,
        partition,
        MAX(offset) AS max_offset
    FROM minio_lake.bronze.commerce_events
    WHERE topic LIKE 'shop.commerce.%'
    GROUP BY topic, partition
""").collect()

bronze_offsets = {}

for row in bronze_rows:
    bronze_offsets[
        (row["topic"], int(row["partition"]))
    ] = int(row["max_offset"])


# ============================================================
# Kafka -> Bronze lag
#
# Kafka exporter gives the next/log-end offset.
# Bronze stores the last consumed Kafka offset.
#
# lag = kafka_offset - (bronze_max_offset + 1)
# ============================================================

partition_lags = []

for key, kafka_offset in kafka_offsets.items():

    bronze_max_offset = bronze_offsets.get(key, -1)

    bronze_next_offset = bronze_max_offset + 1

    lag = max(
        kafka_offset - bronze_next_offset,
        0
    )

    partition_lags.append(lag)


kafka_bronze_lag_total = sum(partition_lags)

kafka_bronze_lag_max = (
    max(partition_lags)
    if partition_lags
    else 0
)


# ============================================================
# Prometheus metrics
# ============================================================

metrics = f"""
# TYPE commerce_silver_orders gauge
commerce_silver_orders {silver_orders}

# TYPE commerce_silver_order_items gauge
commerce_silver_order_items {silver_order_items}

# TYPE commerce_silver_customers gauge
commerce_silver_customers {silver_customers}

# TYPE commerce_silver_products gauge
commerce_silver_products {silver_products}

# TYPE commerce_silver_payments gauge
commerce_silver_payments {silver_payments}

# TYPE commerce_silver_inventory gauge
commerce_silver_inventory {silver_inventory}

# TYPE commerce_bronze_events_total gauge
commerce_bronze_events_total {bronze_events}

# TYPE commerce_processing_latency_avg_seconds gauge
commerce_processing_latency_avg_seconds {avg_processing_latency}

# TYPE commerce_processing_latency_max_seconds gauge
commerce_processing_latency_max_seconds {max_processing_latency}

# TYPE commerce_dlq_size gauge
commerce_dlq_size {dlq_size}

# TYPE commerce_gold_orders gauge
commerce_gold_orders {gold_orders}

# TYPE commerce_gold_revenue gauge
commerce_gold_revenue {gold_revenue}

# TYPE commerce_kafka_bronze_lag_total gauge
commerce_kafka_bronze_lag_total {kafka_bronze_lag_total}

# TYPE commerce_kafka_bronze_lag_max gauge
commerce_kafka_bronze_lag_max {kafka_bronze_lag_max}

# TYPE commerce_pipeline_success gauge
commerce_pipeline_success 1
""".strip() + "\n"


# ============================================================
# Push metrics
# ============================================================

pushgateway_url = (
    "http://pushgateway:9091/"
    "metrics/job/commerce_lakehouse"
)

request = Request(
    pushgateway_url,
    data=metrics.encode("utf-8"),
    method="PUT",
    headers={
        "Content-Type": "text/plain; version=0.0.4"
    }
)

with urlopen(request, timeout=15) as response:
    print(
        "PUSHGATEWAY STATUS:",
        response.status
    )


print("========== METRICS PUBLISHED ==========")
print(metrics)

spark.stop()
