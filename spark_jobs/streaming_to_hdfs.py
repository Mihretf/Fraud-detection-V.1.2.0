from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaToHDFS_Bronze_Ingestor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Kafka Configuration
kafka_bootstrap_servers = "kafka:9092"
# Subscribing to all bank topics using a pattern
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribePattern", "bank_.*_transactions") \
    .option("startingOffsets", "latest") \
    .load()

# ---------------------------------------------------------
# THE FIX: Keep it RAW. 
# We save the 'value' as a string so Silver can parse it later.
# ---------------------------------------------------------
bronze_df = raw_df.selectExpr(
    "CAST(key AS STRING)", 
    "CAST(value AS STRING) as raw_payload", 
    "topic", 
    "partition", 
    "offset", 
    "timestamp as ingest_time"
)

# Write to HDFS Bronze
query = bronze_df.writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:8020/fraud_detection/bronze/transactions") \
    .option("checkpointLocation", "hdfs://namenode:8020/fraud_detection/checkpoints/bronze") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()