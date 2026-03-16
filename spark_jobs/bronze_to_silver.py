from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, sha2, to_timestamp, coalesce, current_timestamp, struct, to_json
from schemas import bronze_transaction_schema, bronze_read_schema

spark = SparkSession.builder \
    .appName("Silver_Layer_Batch_Harmonizer") \
    .getOrCreate()

# 1. READ RAW BRONZE (Batch Mode)
# Added recursiveFileLookup in case Spark partitioned the Bronze data
raw_bronze_df = spark.read \
    .format("parquet") \
    .option("recursiveFileLookup", "true") \
    .load("hdfs://namenode:8020/projects/synthetic/bronze")

# 2. PARSE THE JSON
parsed_df = raw_bronze_df.withColumn("data", from_json(col("raw_payload"), bronze_transaction_schema))

# 3. HARMONIZE & FLATTEN
flattened_df = parsed_df.select(
    col("data.source_event_id").alias("transaction_id"),
    col("data.bank_id"),
    col("data.channel"),
    col("data.raw_payload.trxAmt").cast("double").alias("amount"),
    col("data.raw_payload.trxTime").alias("raw_time"),
    col("data.raw_payload.cardNumber").alias("card_number"),
    col("data.extra_metadata.device_status").alias("status"),
    col("data.extra_metadata.branch_id").alias("location"),
    col("ingest_time")
)

# 4. TRIAGE & SCORING (Quality Control)
final_silver = flattened_df.withColumn("quality_score",
    when(col("transaction_id").isNull() | col("amount").isNull() | col("bank_id").isNull(), "HARD_REJECT")
    .when(col("status").isNull() | col("location").isNull(), "WARNING_MISSING_CONTEXT")
    .otherwise("CLEAN")
)

# 5. MASKING & REFINING (PII Protection)
final_silver = final_silver.withColumn("masked_card", sha2(col("card_number"), 256)) \
    .withColumn("event_timestamp", 
        coalesce(
            to_timestamp(col("raw_time"), "dd/MM/yy HH:mm"),
            to_timestamp(col("raw_time"), "yyyy-MM-dd HH:mm:ss"),
            current_timestamp()
        )
    ) \
    .select("transaction_id", "event_timestamp", "bank_id", "channel", 
            "amount", "masked_card", "status", "quality_score",
            current_timestamp().alias("processed_at"))

# 6. DUAL ROUTING
# Clean Data to Silver
final_silver.filter(col("quality_score") == "CLEAN") \
    .write.mode("append") \
    .partitionBy("bank_id", "channel") \
    .parquet("hdfs://namenode:8020/projects/synthetic/silver")

# Rejects to Garbage
final_silver.filter(col("quality_score") != "CLEAN") \
    .write.mode("append") \
    .json("hdfs://namenode:8020/projects/synthetic/garbage/rejected")

# 7. KAFKA EGRESS (Real-time downstream)
# Ensure the bootstrap server matches your docker name!
try:
    kafka_output_df = final_silver.filter(col("quality_score") == "CLEAN") \
        .selectExpr("CAST(transaction_id AS STRING) AS key", "to_json(struct(*)) AS value")
    
    kafka_output_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka_old:9092") \
        .option("topic", "cleaned_transactions_for_ml") \
        .save()
except Exception as e:
    print(f"Kafka Egress failed but continuing: {e}")

# 8. EXPORT FOR BILISE (CSV for local analysis)
# Coalesce(1) is good for Bilise but careful with huge data!
final_silver.filter(col("quality_score") == "CLEAN") \
    .coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("hdfs://namenode:8020/projects/synthetic/export/bilise_latest")

print("🚀 Silver Harmonizer Finished.")
spark.stop()