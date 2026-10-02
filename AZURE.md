# Azure deployment

## Validated deployment

The existing Windows Docker deployment was migrated to an Azure
Ubuntu 24.04 x64 VM using cold backups of seven Docker volumes,
project files, lakehouse data, and streaming checkpoints.
Archive transfers were verified using SHA-256 checksums.

Run `azure_validation_20261002_01` of `commerce_lakehouse_minio`
succeeded on 2026-10-02, from 12:12:00 to 12:16:43 UTC.
Validation covered ingestion, Silver processing, data quality,
source reconciliation, dbt, reconciliation, storage, and metrics.

This is a single-VM deployment. The DAG is manually triggered.
Keep `commerce_lakehouse_pipeline` and `commerce_lakehouse_smoke`
paused. Keep the original local commerce stack stopped while Azure
is the active deployment.

## Configuration

- `docker-compose.azure.yml`: Linux paths, Docker socket group,
  locally loaded images, and loopback-only dashboard ports.
- `docker-compose.azure-images.json`: pinned registry image digests.
- `tools/azure-compose.sh`: loads all required Compose files.

The Azure override currently targets:
- Project directory: `/home/azureuser/real-time-commerce-lakehouse`
- Docker socket group ID: `988`

Adjust both for another host. Check the socket group with:
`stat -c %g /var/run/docker.sock`

This configuration expects restored volumes and `.env`, plus the
exported Airflow, dbt, and MinIO images loaded with `docker image load`.
A Git clone alone is not a complete deployment.
Never commit credentials, private keys, or backup archives.

## Operations

Run from the project directory:

```bash
bash tools/azure-compose.sh ps -a
bash tools/azure-compose.sh exec -T airflow-scheduler airflow dags list
bash tools/azure-compose.sh exec -T airflow-scheduler airflow dags trigger -r "azure_manual_$(date -u +%Y%m%dT%H%M%SZ)" commerce_lakehouse_minio
bash tools/azure-compose.sh exec -T airflow-scheduler airflow dags list-runs -d commerce_lakehouse_minio

```

Trigger once, then monitor the existing run.

## Dashboard access

On your computer, replace the key path and VM address:

```bash
ssh -i /path/to/key.pem -N -L 18081:127.0.0.1:8081 -L 13000:127.0.0.1:3000 azureuser@VM_PUBLIC_IP
```

Keep the tunnel open.
Airflow: http://localhost:18081
Grafana: http://localhost:13000

## Costs and shutdown

Check auto-shutdown, its time zone, remaining credit, and costs
in Azure Portal. Let active pipeline runs finish before shutdown.
Manage the preserved OS disk separately when retiring the VM.

## References

- https://docs.docker.com/engine/storage/volumes/
- https://docs.docker.com/reference/cli/docker/image/save/
- https://docs.docker.com/reference/cli/docker/image/load/
