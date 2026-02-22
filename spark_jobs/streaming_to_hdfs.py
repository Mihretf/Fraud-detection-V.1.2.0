from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# -----------------------------
# Define Kafka message schema
# -----------------------------
transaction_schema = StructType() \
    .add("transaction_id", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("bank_id", StringType()) \
    .add("channel", StringType()) \
    .add("amount", DoubleType()) \
    .add("status", StringType())

# -----------------------------
# Initialize Spark Session
# -----------------------------
spark = SparkSession.builder \
    .appName("KafkaToHDFSStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.streaming.checkpointLocation", "/fraud_detection/checkpoints/bronze") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Read stream from Kafka
# -----------------------------
kafka_bootstrap_servers = "kafka:9092"  # docker-compose service name
kafka_topics = "bank_a_atm_transactions,bank_a_pos_transactions,bank_a_web_transactions,bank_a_mobile_transactions," \
               "bank_b_atm_transactions,bank_b_pos_transactions,bank_b_web_transactions,bank_b_mobile_transactions," \
               "bank_c_atm_transactions,bank_c_pos_transactions,bank_c_web_transactions,bank_c_mobile_transactions"

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topics) \
    .option("startingOffsets", "latest") \
    .load()

# -----------------------------
# Convert value from Kafka (bytes) to string and parse JSON
# -----------------------------
transactions_df = raw_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), transaction_schema).alias("data")) \
    .select("data.*")  # Flatten the struct

# -----------------------------
# Write to HDFS Bronze layer
# -----------------------------

hdfs_base = "hdfs://namenode:8020"
hdfs_path = f"{hdfs_base}/fraud_detection/bronze/transactions"
checkpoint_path = f"{hdfs_base}/fraud_detection/checkpoints/bronze"
query = transactions_df.writeStream \
    .format("parquet") \
    .option("path", hdfs_path) \
    .option("checkpointLocation", checkpoint_path) \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()