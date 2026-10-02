"""Resume into MinIO with a separate checkpoint and replay-safe inserts."""
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql.functions import current_timestamp
from common import get_spark, kafka_bounds, TOPICS, BRONZE, NEW_CHECKPOINT, snapshot_id
from offsets import resolve_offsets
from state import read_manifest, assert_original_unchanged, check_duplicates

spark = get_spark('MinioBronze', include_local=True)
try:
    data = read_manifest()
    assert_original_unchanged(spark, data)
    if not spark.catalog.tableExists(BRONZE):
        raise RuntimeError('Migrated Bronze table is missing.')
    check_duplicates(spark)
    if not Path(NEW_CHECKPOINT).exists():
        if snapshot_id(spark, BRONZE) != data['tables']['bronze.commerce_events']['minio_snapshot']:
            raise RuntimeError('Bronze changed before its first checkpoint. Stop and review.')
        resolve_offsets(data['starting_offsets'], kafka_bounds(spark), TOPICS)
    raw = (spark.readStream.format('kafka')
           .option('kafka.bootstrap.servers', 'kafka:9092')
           .option('subscribe', ','.join(TOPICS))
           .option('startingOffsets', json.dumps(data['starting_offsets']))
           .option('failOnDataLoss', 'true')
           .option('maxOffsetsPerTrigger', '2000').load())
    events = raw.selectExpr('topic','partition','offset','timestamp AS kafka_timestamp',
                            'CAST(key AS STRING) AS event_key','CAST(value AS STRING) AS payload')
    events = events.withColumn('ingestion_time', current_timestamp())

    def merge_batch(batch, batch_id):
        # foreachBatch can retry after a successful table commit. Kafka positions are the key.
        batch.dropDuplicates(['topic','partition','offset']).createOrReplaceTempView('minio_incoming')
        batch.sparkSession.sql(f'''MERGE INTO {BRONZE} t USING minio_incoming s
                      ON t.topic = s.topic AND t.partition = s.partition AND t.offset = s.offset
                      WHEN NOT MATCHED THEN INSERT *''')
        print(f'BRONZE committed batch {batch_id}', flush=True)

    query = (events.writeStream.foreachBatch(merge_batch).outputMode('append')
             .option('checkpointLocation', NEW_CHECKPOINT).trigger(availableNow=True).start())
    query.awaitTermination()
    check_duplicates(spark)
    print('PASS MINIO BRONZE: ingestion finished; no duplicate Kafka positions.', flush=True)
finally:
    spark.stop()
