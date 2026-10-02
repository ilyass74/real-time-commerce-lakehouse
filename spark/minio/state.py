"""Cutover guards shared by preparation and ingestion."""
import json
from pathlib import Path
from common import MANIFEST, OLD_CHECKPOINT, snapshot_id
from offsets import committed_offsets
TABLES = ['bronze.commerce_events'] + ['silver.' + t for t in
    ('customers','dlq','inventory','orders','order_items','payments','products')] + ['gold.' + t for t in
    ('customer_metrics','daily_orders','daily_sales','dim_customer','dim_date','dim_location',
     'dim_product','fact_orders','fact_payments','inventory_kpis','product_performance','sales_by_region')]


def read_manifest():
    path = Path(MANIFEST)
    if not path.exists():
        raise RuntimeError('Run tools/minio-cutover.ps1 Prepare first.')
    data = json.loads(path.read_text(encoding='utf-8'))
    if data.get('version') != 1 or set(data.get('tables', {})) != set(TABLES):
        raise RuntimeError('Unexpected cutover manifest; do not edit it manually.')
    return data


def assert_original_unchanged(spark, data):
    batch, offsets = committed_offsets(OLD_CHECKPOINT)
    if batch != data['committed_batch'] or offsets != data['committed_offsets']:
        raise RuntimeError('The original Bronze checkpoint changed after preparation. Stop and review.')
    for name, recorded in data['tables'].items():
        if snapshot_id(spark, 'local_lake.' + name) != recorded['local_snapshot']:
            raise RuntimeError('Original table changed after preparation: ' + name)


def check_duplicates(spark):
    from common import BRONZE
    if spark.sql(f'''SELECT topic, partition, offset FROM {BRONZE}
                    GROUP BY topic, partition, offset HAVING count(*) > 1 LIMIT 1''').count():
        raise RuntimeError('Duplicate Kafka positions in MinIO Bronze.')
