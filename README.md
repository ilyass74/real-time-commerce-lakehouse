# Real-Time Commerce Lakehouse

A local data engineering project that captures PostgreSQL changes with Debezium and Kafka, processes them with Spark, and stores Bronze, Silver and Gold Iceberg tables in MinIO. Airflow orchestrates transformations and checks; dbt builds analytical models.

**Validated milestone:** the MinIO cutover completed on 1 October 2026. The project owner's local validation showed all 14 Airflow tasks successful, all 20 migrated tables readable in MinIO, and original local-table snapshots unchanged. The implementation was pushed in commit `564e49f`.

## Architecture

| Component | Role |
|---|---|
| PostgreSQL | Operational commerce data; separate `iceberg_catalog` database for the Iceberg JDBC catalog |
| Debezium / Kafka Connect | Capture source changes and publish Kafka events |
| Kafka | Transport events for customers, products, orders, order items, payments and inventory |
| Spark Structured Streaming | Ingest Kafka events into Bronze with a persistent checkpoint |
| Spark / Iceberg | Apply Silver transformations and execute data checks |
| MinIO | Store Iceberg data and metadata files in `s3://commerce-lakehouse/warehouse` |
| dbt Spark | Build and test Gold analytical models |
| Airflow | Sequence the 14 pipeline tasks |
| Prometheus / Pushgateway / Grafana | Collect and display pipeline metrics |

The active catalog is **`minio_lake`**, using Iceberg's JDBC catalog and S3FileIO. Keep that catalog identity consistent with the migration.

CDC capture and pipeline execution have different lifecycles: the Airflow DAG is **manually triggered** (`schedule=None`). Bronze uses an `availableNow` streaming trigger, processing available input and then terminating. This configuration does not claim continuous, always-running end-to-end transformations.

## Data layers

- **Bronze:** `commerce_events`, including Kafka topic, partition and offset.
- **Silver:** customers, orders, order_items, payments, products, inventory and a dead-letter table (`dlq`).
- **Gold:** customer/product/date/location dimensions, order/payment facts, daily summaries, inventory KPIs, product performance, customer metrics and regional sales.

The migration retained 20 current tables. Test namespaces were not migrated. The legacy `gold.daily_orders` table remains readable but is not refreshed by the active DAG's task/model selection.

## Validation results

These are point-in-time local results, not throughput benchmarks:

| Check | Observed result |
|---|---|
| Migration and preparation | All 20 tables matched their originals using schema checks, row counts and bidirectional data comparison |
| Bronze events at preparation | 304,692 rows |
| Silver orders at preparation | 101,001 rows |
| Silver order items at preparation | 102,477 rows |
| Silver payments at preparation | 101,001 rows |
| Airflow MinIO validation | All 14 tasks successful after correcting the dbt command's template handling |
| Final storage validation | 20 tables readable in MinIO; original snapshots unchanged |

The DAG includes source schema checks, Silver data quality, PostgreSQL-to-Silver reconciliation, dbt tests, Gold reconciliation, storage checks and metrics publishing.

## Run the existing migrated installation

These commands assume the local installation has already been initialized and migrated. A fresh clone alone does not contain the database volumes, MinIO objects, Kafka checkpoints or cutover manifest.

Requirements:

- Docker Desktop running Linux containers and PowerShell.
- Existing configured `.env`, source database and Debezium connector.
- MinIO bucket `commerce-lakehouse` and PostgreSQL database `iceberg_catalog`.
- Local dbt image `commerce-dbt-spark:1.0`, built from [dbt/Dockerfile](dbt/Dockerfile).
- The migrated tables, original local warehouse/checkpoint, and `lakehouse/cutover/minio_start_v1.json`.
- Correct `PROJECT_ROOT` and `COMPOSE_NETWORK` in [docker-compose.airflow.yml](docker-compose.airflow.yml).

From the project root, start the services using all relevant overlays:

```powershell
$ComposeFiles = @(
    '-f', 'docker-compose.yml',
    '-f', 'docker-compose.airflow.yml',
    '-f', 'docker-compose.monitoring.yml',
    '-f', 'docker-compose.storage.yml',
    '-f', 'docker-compose.cutover.yml'
)
docker compose @ComposeFiles up -d postgres kafka connect minio airflow-db airflow-scheduler airflow-webserver prometheus pushgateway kafka-exporter grafana
```

Keep **`commerce_lakehouse_pipeline` paused**. Use **`commerce_lakehouse_minio`**:

```powershell
# Trigger one run only when neither pipeline has a queued or running run.
powershell -ExecutionPolicy Bypass -File .\tools\minio-cutover.ps1 Run

# Inspect run states.
powershell -ExecutionPolicy Bypass -File .\tools\minio-cutover.ps1 Status

# After completion, validate storage and original snapshot preservation.
powershell -ExecutionPolicy Bypass -File .\tools\minio-cutover.ps1 Validate
```

Airflow: http://localhost:8081  
MinIO console: http://localhost:9001  
Grafana: http://localhost:3000  
Prometheus: http://localhost:9090

The `Prepare` stage is for the initial handoff after migration. It pauses the original DAG, compares the 20 tables and records committed Kafka offsets. It is not a fresh-install bootstrap or a required step before every subsequent run.

## Cutover and recovery

The original local warehouse and checkpoint are retained. MinIO ingestion has a separate checkpoint:

| Purpose | Container path |
|---|---|
| Original Bronze checkpoint | `/data/checkpoints/commerce_bronze` |
| MinIO Bronze checkpoint | `/data/checkpoints/commerce_bronze_minio_v1` |
| Handoff manifest | `/data/cutover/minio_start_v1.json` |

Preparation reads the latest committed old Kafka batch and checks it against actual Kafka partition boundaries. It refuses retention gaps, disappearing partitions and offset regression.

MinIO Bronze uses a MERGE keyed by topic, partition and offset so a retried batch does not insert an already present Kafka position. The pipeline verifies that original local snapshots and the old checkpoint have not changed since preparation. Do not run manual writers against the preserved original tables.

If a task fails, inspect its logs and fix the cause. Clear that task and the necessary downstream tasks **in the same run**, preserving successful upstream tasks. Do not mark failed work as successful.

For rollback, pause the MinIO DAG and let active work finish. Verify Kafka still retains offsets needed by the original checkpoint before resuming the original pipeline. Do not reuse the MinIO checkpoint for local-table ingestion or delete volumes/checkpoints to bypass a failure.

The migration copied current data, not historical snapshots or original partition specifications.

## Configuration and current limits

Use environment variables for credentials and keep `.env` outside Git. In addition to the existing [.env.example](.env.example), MinIO requires `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD`.

This is a validated local portfolio implementation, not a production deployment:

- Airflow's Compose configuration contains a machine-specific bind-mount root.
- The Airflow metadata connection currently contains a fixed password while its PostgreSQL service uses `AIRFLOW_DB_PASSWORD`; these must agree. Fresh-install credential configuration needs alignment.
- The MinIO image is built from the source Dockerfile under `storage/minio`.
- A fresh installation needs source/connector initialization, catalog database and bucket creation, initial data preparation and a committed checkpoint before the migration handoff.
- Source reconciliation is easiest to interpret with demo data generators stopped during validation.
- Sustained throughput, end-to-end latency, failure recovery under load, high availability and automated disaster recovery are not established by the successful validation run.

## Key implementation files

- [Airflow MinIO DAG](airflow/dags/commerce_lakehouse_minio.py)
- [PowerShell run helper](tools/minio-cutover.ps1)
- [Catalog configuration](spark/minio/common.py)
- [Migration comparison](spark/batch/migrate_to_minio.py)
- [Handoff preparation](spark/minio/prepare.py)
- [Kafka offset checks](spark/minio/offsets.py)
- [Bronze ingestion](spark/minio/streaming/bronze_iceberg.py)
- [Storage validation](spark/minio/validate.py)
- [dbt MinIO profile](dbt/minio/profiles.yml)
- [dbt runner](dbt/minio/run.sh)

These source files define the implementation. Validation counts above record the project owner's observed local run on 1 October 2026.
