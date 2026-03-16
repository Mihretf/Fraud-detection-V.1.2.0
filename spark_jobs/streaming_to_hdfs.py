from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Full_Bank_Bronze_Ingestor") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------
# READ FROM ALL TOPICS
# ---------------------------------------------------------
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka_old:9092") \
    .option("subscribePattern", "bank_.*_transactions") \
    .option("startingOffsets", "earliest") \
    .load()

# Transformation remains the same for Bronze (Raw storage)
bronze_df = raw_df.selectExpr(
    "CAST(key AS STRING)", 
    "CAST(value AS STRING) as raw_payload", 
    "topic", 
    "partition", 
    "offset", 
    "timestamp as ingest_time"
)

# ---------------------------------------------------------
# WRITE TO HDFS (The Medallion Foundation)
# ---------------------------------------------------------
hdfs_query = bronze_df.writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:8020/projects/synthetic/bronze") \
    .option("checkpointLocation", "hdfs://namenode:8020/projects/synthetic/checkpoint_v_NEW_TEST2") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

# Optional: Keep console output to verify the multi-topic flow
console_query = bronze_df.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="10 seconds") \
    .start()

spark.streams.awaitAnyTermination()