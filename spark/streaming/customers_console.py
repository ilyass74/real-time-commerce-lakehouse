from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object

spark = (
    SparkSession.builder
    .appName("CommerceCustomerEvents")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "shop.commerce.customers")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "500")
    .load()
)

events = raw.selectExpr(
    "CAST(key AS STRING) AS event_key",
    "CAST(value AS STRING) AS payload",
    "partition",
    "offset",
)

readable = events.select(
    "event_key",
    get_json_object("payload", "$.op").alias("operation"),
    get_json_object("payload", "$.after.full_name").alias("customer"),
    get_json_object("payload", "$.after.city").alias("city"),
    "partition",
    "offset",
)

query = (
    readable.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", "10")
    .option("checkpointLocation", "/tmp/customer-console-checkpoint")
    .trigger(processingTime="5 seconds")
    .start()
)

query.awaitTermination()
