"""Read-only comparison, then write a non-secret immutable handoff manifest."""
import json
from pathlib import Path
from datetime import datetime, timezone
from common import get_spark, snapshot_id, kafka_bounds, TOPICS, OLD_CHECKPOINT, NEW_CHECKPOINT, MANIFEST, BRONZE
from offsets import committed_offsets, resolve_offsets
from state import TABLES, read_manifest, assert_original_unchanged, check_duplicates

spark = get_spark('PrepareMinioCutover', include_local=True)
try:
    if Path(MANIFEST).exists():
        data = read_manifest()
        assert_original_unchanged(spark, data)
        print('PREPARE ALREADY COMPLETE. Manifest retained; ingestion progress was not reset.', flush=True)
    else:
        if Path(NEW_CHECKPOINT).exists():
            raise RuntimeError('New checkpoint already exists without a manifest. Stop and review; do not delete it.')
        batch, committed = committed_offsets(OLD_CHECKPOINT)
        bounds = kafka_bounds(spark)
        offsets = resolve_offsets(committed, bounds, TOPICS)
        data = {'version': 1, 'prepared_at': datetime.now(timezone.utc).isoformat(),
                'committed_batch': batch, 'committed_offsets': committed,
                'starting_offsets': offsets, 'tables': {}}
        for number, name in enumerate(TABLES, 1):
            local, remote = 'local_lake.' + name, 'minio_lake.' + name
            local_id, remote_id = snapshot_id(spark, local), snapshot_id(spark, remote)
            if local_id is None or remote_id is None:
                raise RuntimeError('Missing snapshot for ' + name)
            a = spark.read.option('snapshot-id', str(local_id)).table(local)
            b = spark.read.option('snapshot-id', str(remote_id)).table(remote)
            shape = lambda df: [(f.name, f.dataType.simpleString()) for f in df.schema.fields]
            if shape(a) != shape(b):
                raise RuntimeError('Schema mismatch: ' + name)
            rows = a.count()
            if rows != b.count() or a.exceptAll(b).limit(1).count() or b.exceptAll(a).limit(1).count():
                raise RuntimeError('Data changed since migration: ' + name + '. Stop; do not overwrite the migrated table.')
            if spark.sql(f"SELECT file_path FROM {remote}.files").filter("NOT file_path LIKE 's3://commerce-lakehouse/%'").limit(1).count():
                raise RuntimeError('Unexpected storage path for ' + name)
            data['tables'][name] = {'local_snapshot': local_id, 'minio_snapshot': remote_id, 'rows': rows}
            print(f'PASS [{number}/20] {name}: {rows} rows, exact match', flush=True)
        check_duplicates(spark)
        # Synthetic test records use partition 99; compare only actual Kafka partitions.
        maxima = spark.sql(f'SELECT topic, partition, max(offset) AS last FROM {BRONZE} GROUP BY topic, partition').collect()
        for row in maxima:
            key = (row['topic'], row['partition'])
            if key in bounds and row['last'] >= offsets[key[0]][str(key[1])]:
                raise RuntimeError('Bronze is ahead of the committed checkpoint. Stop and review before resuming.')
        assert_original_unchanged(spark, data)
        for name, recorded in data['tables'].items():
            if snapshot_id(spark, 'minio_lake.' + name) != recorded['minio_snapshot']:
                raise RuntimeError('MinIO table changed during preparation: ' + name)
        # Re-check retention in case the comparison took a long time.
        resolve_offsets(offsets, kafka_bounds(spark), TOPICS)
        path = Path(MANIFEST)
        path.parent.mkdir(parents=True, exist_ok=True)
        # Exclusive creation: never silently replace a previous handoff.
        with path.open('x', encoding='utf-8') as handle:
            json.dump(data, handle, indent=2)
            handle.write('\n')
        print('PASS PREPARE: all 20 tables match; Kafka handoff recorded. No pipeline triggered.', flush=True)
finally:
    spark.stop()
