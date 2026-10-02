"""Shared catalog configuration. minio_lake is also the JDBC catalog identity."""
import os

CATALOG = 'minio_lake'
BRONZE = CATALOG + '.bronze.commerce_events'
OLD_CHECKPOINT = '/data/checkpoints/commerce_bronze'
NEW_CHECKPOINT = '/data/checkpoints/commerce_bronze_minio_v1'
MANIFEST = '/data/cutover/minio_start_v1.json'
TOPICS = ['shop.commerce.' + t for t in (
    'customers', 'products', 'orders', 'order_items', 'payments', 'inventory'
)]


def get_spark(app, include_local=False):
    from pyspark.sql import SparkSession
    password = os.environ.get('PG_PASSWORD')
    if not password:
        raise RuntimeError('PG_PASSWORD is not configured')
    for key in ('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY'):
        if not os.environ.get(key):
            raise RuntimeError(key + ' is not configured')
    b = (SparkSession.builder.appName(app)
         .config('spark.sql.shuffle.partitions', '2')
         .config('spark.sql.session.timeZone', 'UTC')
         .config('spark.sql.extensions',
                 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
         .config('spark.sql.defaultCatalog', CATALOG))
    settings = {
        '': 'org.apache.iceberg.spark.SparkCatalog',
        '.catalog-impl': 'org.apache.iceberg.jdbc.JdbcCatalog',
        '.uri': 'jdbc:postgresql://postgres:5432/iceberg_catalog',
        '.jdbc.user': 'commerce', '.jdbc.password': password,
        '.warehouse': 's3://commerce-lakehouse/warehouse',
        '.io-impl': 'org.apache.iceberg.aws.s3.S3FileIO',
        '.s3.endpoint': 'http://minio:9000',
        '.s3.path-style-access': 'true', '.client.region': 'us-east-1',
    }
    for suffix, value in settings.items():
        b = b.config('spark.sql.catalog.' + CATALOG + suffix, value)
    if include_local:
        b = (b.config('spark.sql.catalog.local_lake', 'org.apache.iceberg.spark.SparkCatalog')
             .config('spark.sql.catalog.local_lake.type', 'hadoop')
             .config('spark.sql.catalog.local_lake.warehouse', '/data/warehouse'))
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    return spark


def snapshot_id(spark, table):
    spark.catalog.refreshTable(table)
    rows = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'").collect()
    return rows[0]['snapshot_id'] if rows else None


def kafka_bounds(spark):
    """Read actual partition start/end offsets without joining a consumer group."""
    jvm = spark.sparkContext._jvm
    props = jvm.java.util.Properties()
    for k, v in {
        'bootstrap.servers': 'kafka:9092',
        'key.deserializer': 'org.apache.kafka.common.serialization.ByteArrayDeserializer',
        'value.deserializer': 'org.apache.kafka.common.serialization.ByteArrayDeserializer',
        'enable.auto.commit': 'false', 'request.timeout.ms': '15000',
        'default.api.timeout.ms': '30000', 'allow.auto.create.topics': 'false',
    }.items():
        props.setProperty(k, v)
    consumer = jvm.org.apache.kafka.clients.consumer.KafkaConsumer(props)
    try:
        parts = jvm.java.util.ArrayList()
        for topic in TOPICS:
            infos = consumer.partitionsFor(topic)
            if infos is None or infos.isEmpty():
                raise RuntimeError('Missing Kafka topic: ' + topic)
            for info in infos:
                parts.add(jvm.org.apache.kafka.common.TopicPartition(topic, info.partition()))
        starts, ends = consumer.beginningOffsets(parts), consumer.endOffsets(parts)
        return {(p.topic(), p.partition()): (int(starts.get(p)), int(ends.get(p))) for p in parts}
    finally:
        consumer.close()
