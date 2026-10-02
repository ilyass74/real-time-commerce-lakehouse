"""Validate catalog, file placement, and original-pipeline isolation."""
from common import get_spark
from state import TABLES, read_manifest, assert_original_unchanged, check_duplicates
spark = get_spark('ValidateMinioStorage', include_local=True)
try:
    assert_original_unchanged(spark, read_manifest())
    for name in TABLES:
        table = 'minio_lake.' + name
        rows = spark.table(table).count()
        if spark.sql(f'SELECT file_path FROM {table}.files').filter("NOT file_path LIKE 's3://commerce-lakehouse/%'").limit(1).count():
            raise RuntimeError('Unexpected file location: ' + table)
        print(f'PASS MINIO STORAGE: {name}: {rows} rows', flush=True)
    check_duplicates(spark)
    print('PASS STORAGE: 20 tables readable in MinIO; original snapshots unchanged.', flush=True)
finally:
    spark.stop()
