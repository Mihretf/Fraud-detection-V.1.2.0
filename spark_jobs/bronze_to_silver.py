from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, sha2, lit, to_timestamp, coalesce, current_timestamp
from schemas import bronze_transaction_schema  # Ensure this schema matches your Producer's JSON!
from schemas import bronze_read_schema, bronze_transaction_schema

spark = SparkSession.builder \
    .appName("Silver_Layer_Harmonizer") \
    .getOrCreate()

# 1. READ RAW BRONZE
raw_bronze_df = spark.readStream \
    .format("parquet") \
    .schema(bronze_read_schema) \
    .load("hdfs://namenode:8020/fraud_detection/bronze/transactions")

# 2. PARSE THE JSON
# We take that 'raw_payload' string and turn it into real columns
parsed_df = raw_bronze_df.withColumn("data", from_json(col("raw_payload"), bronze_transaction_schema)) \
    .select("data.*", "ingest_time")

# 3. HARMONIZE (Mapping your producer's keys to our standard columns)
# Adjust these names (e.g., trxAmt vs amount) based on your schemas.py
flattened_df = parsed_df.select(
    col("source_event_id").alias("transaction_id"),
    col("bank_id"),
    col("channel"),
    col("raw_payload.trxAmt").cast("double").alias("amount"),
    col("raw_payload.trxTime").alias("raw_time"),
    col("raw_payload.cardNumber").alias("card_number"),
    col("extra_metadata.device_status").alias("status")
)

# 4. QUALITY SCORING
scored_df = flattened_df.withColumn("quality_score", 
    when(col("amount").isNull(), "MISSING_AMOUNT")
    .when(col("amount") < 0, "INVALID_VALUE")
    .when(col("status") == "WARN", "DEVICE_WARNING")
    .otherwise("CLEAN")
)

# 5. MASKING & REFINING
final_silver = scored_df.withColumn("masked_card", sha2(col("card_number"), 256)) \
    .withColumn("event_timestamp", 
        coalesce(
            to_timestamp(col("raw_time"), "dd/MM/yy HH:mm"),
            to_timestamp(col("raw_time"), "yyyy-MM-dd HH:mm:ss")
        )
    ) \
    .select("transaction_id", "event_timestamp", "bank_id", "channel", 
            "amount", "masked_card", "status", "quality_score",
            current_timestamp().alias("processed_at"))

# 6. DUAL ROUTING
# Stream A: Clean to Parquet
silver_query = final_silver.filter(col("quality_score") == "CLEAN") \
    .writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:8020/fraud_detection/silver/transactions") \
    .option("checkpointLocation", "hdfs://namenode:8020/fraud_detection/checkpoints/silver") \
    .partitionBy("bank_id", "channel") \
    .start()

# Stream B: Bad to JSON
garbage_query = final_silver.filter(col("quality_score") != "CLEAN") \
    .writeStream \
    .format("json") \
    .option("path", "hdfs://namenode:8020/fraud_detection/garbage/rejected") \
    .option("checkpointLocation", "hdfs://namenode:8020/fraud_detection/checkpoints/garbage") \
    .start()

spark.streams.awaitAnyTermination()