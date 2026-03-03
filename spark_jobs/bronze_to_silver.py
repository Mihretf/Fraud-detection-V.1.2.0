from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, sha2, to_timestamp, coalesce, current_timestamp
from schemas import bronze_transaction_schema, bronze_read_schema

spark = SparkSession.builder \
    .appName("Silver_Layer_Batch_Harmonizer") \
    .getOrCreate()

# 1. READ RAW BRONZE (Batch Mode)
raw_bronze_df = spark.read \
    .format("parquet") \
    .schema(bronze_read_schema) \
    .load("hdfs://namenode:8020/fraud_detection/bronze/transactions")

# 2. PARSE THE JSON
# Note: We use 'bronze_transaction_schema' to decode the 'raw_payload' column
parsed_df = raw_bronze_df.withColumn("data", from_json(col("raw_payload"), bronze_transaction_schema))

# 3. HARMONIZE (Corrected paths based on your schemas.py)
# Your schema nests trxAmt inside raw_payload, which is inside 'data'
flattened_df = parsed_df.select(
    col("data.source_event_id").alias("transaction_id"),
    col("data.bank_id"),
    col("data.channel"),
    # trxAmt is inside raw_payload, which is inside our 'data' struct
    col("data.raw_payload.trxAmt").cast("double").alias("amount"),
    col("data.raw_payload.trxTime").alias("raw_time"),
    col("data.raw_payload.cardNumber").alias("card_number"),
    # extra_metadata is a separate struct inside our 'data' struct
    col("data.extra_metadata.device_status").alias("status"),
    col("data.extra_metadata.branch_id").alias("location"),
    col("ingest_time")
)

# 4. TRIAGE & SCORING
final_silver = flattened_df.withColumn("quality_score",
    when(
        col("transaction_id").isNull() | col("amount").isNull() | col("bank_id").isNull(),                
        "HARD_REJECT"
    )
    .when(
        col("status").isNull() | col("location").isNull(),
        "WARNING_MISSING_CONTEXT"
    )
    .otherwise("CLEAN")
)

# 5. MASKING & REFINING
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

# 6. DUAL ROUTING (Batch Style)
print("Processing clean transactions...")
final_silver.filter(col("quality_score") == "CLEAN") \
    .write \
    .mode("append") \
    .partitionBy("bank_id", "channel") \
    .parquet("hdfs://namenode:8020/fraud_detection/silver/transactions")

print("Processing rejected transactions...")
final_silver.filter(col("quality_score") != "CLEAN") \
    .write \
    .mode("append") \
    .json("hdfs://namenode:8020/fraud_detection/garbage/rejected")

print("Pipeline Finished Successfully.")
spark.stop()