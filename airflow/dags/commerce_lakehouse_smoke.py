import os
import socket

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator


PROJECT_ROOT = os.environ["PROJECT_ROOT"]
NETWORK = os.environ["COMPOSE_NETWORK"]


def check_sources():
    services = {
        "postgres": ("postgres", 5432),
        "kafka": ("kafka", 9092),
        "debezium_connect": ("connect", 8083),
    }

    for name, (host, port) in services.items():
        try:
            with socket.create_connection((host, port), timeout=5):
                print(f"PASS: {name} reachable at {host}:{port}")
        except Exception as exc:
            raise RuntimeError(
                f"FAIL: {name} unreachable at {host}:{port}: {exc}"
            )

    print("SOURCE CHECK COMPLETE")


spark_mounts = [
    {
        "source": f"{PROJECT_ROOT}/spark",
        "target": "/opt/spark/apps",
        "type": "bind",
        "read_only": True,
    },
    {
        "source": f"{PROJECT_ROOT}/lakehouse",
        "target": "/data",
        "type": "bind",
    },
]


with DAG(
    dag_id="commerce_lakehouse_smoke",
    description="Airflow orchestration smoke test for Commerce Lakehouse",
    start_date=pendulum.datetime(2026, 9, 25, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["commerce", "lakehouse", "spark"],
) as dag:

    check_sources_task = PythonOperator(
        task_id="check_sources",
        python_callable=check_sources,
    )

    bronze_ingestion = DockerOperator(
        task_id="bronze_ingestion",

        image="apache/spark:3.5.7-python3",

        entrypoint="/opt/spark/bin/spark-submit",

        command=[
            "--master",
            "local[2]",
            "--driver-memory",
            "1g",

            "--conf",
            "spark.jars.ivy=/data/ivy",

            "--packages",
            (
                "org.apache.spark:"
                "spark-sql-kafka-0-10_2.12:3.5.7,"
                "org.apache.iceberg:"
                "iceberg-spark-runtime-3.5_2.12:1.7.2"
            ),

            "/opt/spark/apps/streaming/bronze_iceberg.py",
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

    check_sources_task >> bronze_ingestion
