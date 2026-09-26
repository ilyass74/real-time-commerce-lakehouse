FROM apache/spark:3.5.7-python3

USER root

ENV PYTHONPATH="/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip"

RUN python3 -m pip install --no-cache-dir \
    --timeout 120 \
    --retries 10 \
    isodate==0.6.1 \
    dbt-core==1.8.6 \
    dbt-spark==1.8.0

USER 185
