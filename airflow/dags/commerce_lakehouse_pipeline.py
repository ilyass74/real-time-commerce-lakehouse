import os
import os
import socket

import docker
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


PROJECT_ROOT = os.environ["PROJECT_ROOT"]
NETWORK = os.environ["COMPOSE_NETWORK"]

SPARK_IMAGE = "apache/spark:3.5.7-python3"

ICEBERG_PACKAGE = (
    "org.apache.iceberg:"
    "iceberg-spark-runtime-3.5_2.12:1.7.2"
)

STREAMING_PACKAGES = (
    "org.apache.spark:"
    "spark-sql-kafka-0-10_2.12:3.5.7,"
    + ICEBERG_PACKAGE
)


def check_sources():
    services = {
        "postgres": ("postgres", 5432),
        "kafka": ("kafka", 9092),
        "debezium": ("connect", 8083),
    }

    for name, (host, port) in services.items():
        with socket.create_connection((host, port), timeout=5):
            print(f"PASS: {name} reachable")

    print("SOURCE CHECK COMPLETE")


def schema_validation():
    client = docker.from_env()

    containers = client.containers.list(
        filters={
            "label": [
                "com.docker.compose.service=postgres"
            ]
        }
    )

    if not containers:
        raise RuntimeError("PostgreSQL container not found")

    postgres = containers[0]

    sql = """
    SELECT table_name, column_name
    FROM information_schema.columns
    WHERE table_schema = 'commerce'
      AND table_name IN (
        'customers',
        'products',
        'orders',
        'order_items',
        'payments',
        'inventory'
      )
    ORDER BY table_name, ordinal_position;
    """

    result = postgres.exec_run(
        [
            "psql",
            "-U",
            "commerce",
            "-d",
            "commerce",
            "-At",
            "-c",
            sql,
        ]
    )

    if result.exit_code != 0:
        raise RuntimeError(result.output.decode())

    rows = result.output.decode().strip().splitlines()

    found = {}

    for row in rows:
        table, column = row.split("|")
        found.setdefault(table, set()).add(column)

    required = {
        "customers": {
            "customer_id",
            "full_name",
            "city",
        },
        "products": {
            "product_id",
            "sku",
            "product_name",
            "category",
            "price",
        },
        "orders": {
            "order_id",
            "customer_id",
            "status",
            "amount",
        },
        "order_items": {
            "order_item_id",
            "order_id",
            "product_id",
            "quantity",
            "unit_price",
        },
        "payments": {
            "payment_id",
            "order_id",
            "amount",
        },
        "inventory": {
            "product_id",
            "quantity",
        },
    }

    for table, required_columns in required.items():

        actual = found.get(table, set())
        missing = required_columns - actual

        if missing:
            raise RuntimeError(
                f"{table}: missing columns {sorted(missing)}"
            )

        print(f"PASS schema: {table}")

    print("SCHEMA VALIDATION COMPLETE")


def notify_success():
    print("COMMERCE LAKEHOUSE PIPELINE COMPLETE")
    print("Airflow orchestration succeeded.")


spark_mounts = [
    Mount(
        source=f"{PROJECT_ROOT}/spark",
        target="/opt/spark/apps",
        type="bind",
        read_only=True,
    ),
    Mount(
        source=f"{PROJECT_ROOT}/lakehouse",
        target="/data",
        type="bind",
    ),
]

dbt_mounts = [
    Mount(
        source=f"{PROJECT_ROOT}/dbt",
        target="/work/dbt",
        type="bind",
    ),
    Mount(
        source=f"{PROJECT_ROOT}/lakehouse",
        target="/data",
        type="bind",
    ),
]


def spark_task(task_id, script, kafka=False):

    packages = (
        STREAMING_PACKAGES
        if kafka
        else ICEBERG_PACKAGE
    )

    return DockerOperator(
        task_id=task_id,
        image=SPARK_IMAGE,
        entrypoint="/opt/spark/bin/spark-submit",

        command=[
            "--master",
            "local[2]",

            "--driver-memory",
            "1g",

            "--conf",
            "spark.jars.ivy=/data/ivy",

            "--packages",
            packages,

            script,
        ],

        environment={
            "SPARK_LOCAL_IP": "127.0.0.1",
        },

        mounts=spark_mounts,
        network_mode=NETWORK,
        mount_tmp_dir=False,
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
    )


def dbt_task(task_id, script):

    return DockerOperator(
        task_id=task_id,

        image="commerce-dbt-spark:1.0",

        entrypoint="/bin/bash",

        command=[
            "-lc",
            f"bash {script};",
        ],

        mounts=dbt_mounts,
        network_mode=NETWORK,
        mount_tmp_dir=False,
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
    )


with DAG(
    dag_id="commerce_lakehouse_pipeline",

    description=(
        "End-to-end Commerce Lakehouse "
        "Airflow orchestration"
    ),

    start_date=pendulum.datetime(
        2026,
        9,
        25,
        tz="UTC"
    ),

    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,

    tags=[
        "commerce",
        "lakehouse",
        "spark",
        "iceberg",
        "dbt",
    ],
) as dag:

    check_sources_task = PythonOperator(
        task_id="check_sources",
        python_callable=check_sources,
    )

    schema_validation_task = PythonOperator(
        task_id="schema_validation",
        python_callable=schema_validation,
    )

    run_ingestion = spark_task(
        "run_ingestion",
        "/opt/spark/apps/streaming/bronze_iceberg.py",
        kafka=True,
    )

    silver_customers = spark_task(
        "silver_customers",
        "/opt/spark/apps/streaming/silver_customers.py",
    )

    silver_orders = spark_task(
        "silver_orders",
        "/opt/spark/apps/streaming/silver_orders_dlq.py",
    )

    silver_remaining = spark_task(
        "silver_remaining",
        "/opt/spark/apps/streaming/silver_remaining.py",
    )

    data_quality = spark_task(
        "data_quality",
        "/opt/spark/apps/batch/data_quality.py",
    )

    source_reconciliation = DockerOperator(
        task_id="source_reconciliation",

        image=SPARK_IMAGE,
        entrypoint="/opt/spark/bin/spark-submit",

        command=[
            "--master",
            "local[2]",

            "--driver-memory",
            "1g",

            "--conf",
            "spark.jars.ivy=/data/ivy",

            "--packages",
            ICEBERG_PACKAGE + ",org.postgresql:postgresql:42.7.4",

            "/opt/spark/apps/batch/source_reconciliation.py",
        ],

        environment={
            "SPARK_LOCAL_IP": "127.0.0.1",
            "PG_URL": "jdbc:postgresql://postgres:5432/commerce",
            "PG_USER": "commerce",
            "PG_PASSWORD": os.environ["COMMERCE_DB_PASSWORD"],
        },

        mounts=spark_mounts,
        network_mode=NETWORK,
        mount_tmp_dir=False,
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
    )

    dbt_build = dbt_task(
        "dbt_build",
        "/work/dbt/run_dbt_models.sh",
    )

    dbt_test = dbt_task(
        "dbt_test",
        "/work/dbt/run_dbt_tests.sh",
    )

    refresh_gold = DockerOperator(
        task_id="refresh_gold",

        image="commerce-dbt-spark:1.0",

        entrypoint="/bin/bash",

        command=[
            "-c",
            """
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

            dbt show \
              --profiles-dir . \
              --inline "
                SELECT
                    SUM(orders) AS orders,
                    SUM(units_sold) AS units,
                    SUM(total_revenue) AS revenue
                FROM gold.daily_sales
              "

            echo "GOLD REFRESH VALIDATION COMPLETE"
            """
        ],

        mounts=dbt_mounts,
        network_mode=NETWORK,
        mount_tmp_dir=False,
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
    )

    reconciliation = spark_task(
        "reconciliation",
        "/opt/spark/apps/batch/reconciliation.py",
    )

    publish_metrics = spark_task(
        "publish_metrics",
        "/opt/spark/apps/batch/publish_metrics.py",
    )

    notify = PythonOperator(
        task_id="notify",
        python_callable=notify_success,
    )


    (
        check_sources_task
        >> schema_validation_task
        >> run_ingestion
        >> silver_customers
        >> silver_orders
        >> silver_remaining
        >> data_quality
        >> source_reconciliation
        >> dbt_build
        >> dbt_test
        >> refresh_gold
        >> reconciliation
        >> publish_metrics
        >> notify
    )
