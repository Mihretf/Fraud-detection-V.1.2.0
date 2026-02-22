from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
from pyspark.sql.types import StructType, StringType, TimestampType
# 1. Internal Payload Structure (The mess inside raw_payload)
payload_schema = StructType() \
    .add("atmCode", StringType()) \
    .add("trxAmt", StringType()) \
    .add("trxTime", StringType()) \
    .add("cardNumber", StringType()) \
    .add("extra_field", IntegerType())

# 2. Internal Metadata Structure
metadata_schema = StructType() \
    .add("branch_id", StringType()) \
    .add("device_status", StringType())

# 3. The Main Bronze Schema (What we read from HDFS)
bronze_transaction_schema = StructType() \
    .add("bank_id", StringType()) \
    .add("channel", StringType()) \
    .add("raw_payload", payload_schema) \
    .add("ingest_time", StringType()) \
    .add("source_event_id", StringType()) \
    .add("extra_metadata", metadata_schema)



# This is what the RAW Bronze Parquet looks like
bronze_read_schema = StructType() \
    .add("key", StringType()) \
    .add("raw_payload", StringType()) \
    .add("topic", StringType()) \
    .add("partition", StringType()) \
    .add("offset", StringType()) \
    .add("ingest_time", TimestampType())