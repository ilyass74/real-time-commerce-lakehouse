"""Additive MinIO cutover DAG. Keep the original DAG paused."""
import os
import socket
import docker
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

ROOT = os.environ['PROJECT_ROOT']
NETWORK = os.environ['COMPOSE_NETWORK']
PACKAGES = 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.2,org.apache.iceberg:iceberg-aws-bundle:1.7.2,org.postgresql:postgresql:42.7.4'
KAFKA = ',org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7'
# Missing credentials cause task failure, not disappearance of the DAG at parse time.
def task_env():
    return {
        'SPARK_LOCAL_IP': '127.0.0.1',
        'PG_PASSWORD': os.environ.get('COMMERCE_DB_PASSWORD', ''),
        'DBT_ENV_SECRET_PG_PASSWORD': os.environ.get('COMMERCE_DB_PASSWORD', ''),
        'PG_URL': 'jdbc:postgresql://postgres:5432/commerce',
        'PG_USER': 'commerce',
        'AWS_ACCESS_KEY_ID': os.environ.get('MINIO_ROOT_USER', ''),
        'AWS_SECRET_ACCESS_KEY': os.environ.get('MINIO_ROOT_PASSWORD', ''),
        'AWS_REGION': 'us-east-1', 'AWS_EC2_METADATA_DISABLED': 'true',
    }

def preflight():
    from airflow.models import DagRun, DagModel
    from airflow.utils.session import create_session
    with create_session() as session:
        old = session.query(DagModel).filter(DagModel.dag_id == 'commerce_lakehouse_pipeline').first()
        if old is None or not old.is_paused:
            raise RuntimeError('Pause commerce_lakehouse_pipeline first.')
        if session.query(DagRun).filter(DagRun.dag_id == 'commerce_lakehouse_pipeline', DagRun.state.in_(['running','queued'])).count():
            raise RuntimeError('Original DAG still has active runs. Let them finish.')
    for key in ('COMMERCE_DB_PASSWORD','MINIO_ROOT_USER','MINIO_ROOT_PASSWORD'):
        if not os.environ.get(key):
            raise RuntimeError('Missing configuration: ' + key)
    for host, port in [('postgres',5432),('kafka',9092),('connect',8083),('minio',9000)]:
        with socket.create_connection((host,port),timeout=10):
            print('PASS reachable: ' + host)

def schema_validation():
    client = docker.from_env()

    containers = client.containers.list(
        filters={
            "label": [
                "com.docker.compose.service=postgres",
                "com.docker.compose.project=commerce-lakehouse"
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

def spark_task(task_id, script, kafka=False):
    return DockerOperator(
        task_id=task_id, image='apache/spark:3.5.7-python3',
        entrypoint='/opt/spark/bin/spark-submit',
        command=['--master','local[2]','--driver-memory','1g','--conf','spark.jars.ivy=/data/ivy',
                 '--packages',PACKAGES + (KAFKA if kafka else ''), '/opt/spark/apps/minio/'+script],
        private_environment=task_env(),
        mounts=[Mount(source=ROOT+'/spark',target='/opt/spark/apps',type='bind',read_only=True),
                Mount(source=ROOT+'/lakehouse',target='/data',type='bind')],
        network_mode=NETWORK,mount_tmp_dir=False,auto_remove='success',docker_url='unix://var/run/docker.sock')

def dbt_task(task_id, mode):
    return DockerOperator(
        task_id=task_id,image='commerce-dbt-spark:1.0',entrypoint='/bin/bash',
        command=['-c', 'exec bash /work/dbt/minio/run.sh ' + mode],private_environment=task_env(),
        mounts=[Mount(source=ROOT+'/dbt',target='/work/dbt',type='bind'),
                Mount(source=ROOT+'/lakehouse',target='/data',type='bind')],
        network_mode=NETWORK,mount_tmp_dir=False,auto_remove='success',docker_url='unix://var/run/docker.sock')

with DAG('commerce_lakehouse_minio',description='Validated MinIO/JDBC pipeline',
         start_date=pendulum.datetime(2026,9,25,tz='UTC'),schedule=None,catchup=False,
         is_paused_upon_creation=True,max_active_runs=1,max_active_tasks=1,
         tags=['commerce','minio','iceberg']) as dag:
    tasks = [
        PythonOperator(task_id='preflight',python_callable=preflight),
        PythonOperator(task_id='schema_validation',python_callable=schema_validation),
        spark_task('run_ingestion','streaming/bronze_iceberg.py',kafka=True),
        spark_task('silver_customers','streaming/silver_customers.py'),
        spark_task('silver_orders','streaming/silver_orders_dlq.py'),
        spark_task('silver_remaining','streaming/silver_remaining.py'),
        spark_task('data_quality','batch/data_quality.py'),
        spark_task('source_reconciliation','batch/source_reconciliation.py'),
        dbt_task('dbt_build','run'),dbt_task('dbt_test','test'),dbt_task('refresh_gold','show'),
        spark_task('reconciliation','batch/reconciliation.py'),
        spark_task('validate_storage','validate.py'),
        spark_task('publish_metrics','batch/publish_metrics.py'),
    ]
    for upstream, downstream in zip(tasks,tasks[1:]):
        upstream >> downstream
