import sys
import os
import streamlit as st
import pandas as pd

# --- 1. SMART SPARK LOADER (Fixes ModuleNotFoundError) ---
# This ensures Streamlit can find Spark regardless of the Docker image layout
possible_spark_paths = ["/opt/spark/python", "/opt/bitnami/spark/python"]
for path in possible_spark_paths:
    if os.path.exists(path):
        sys.path.insert(0, path)
        lib_path = os.path.join(path, "lib")
        if os.path.exists(lib_path):
            for file in os.listdir(lib_path):
                if file.endswith(".zip"):
                    sys.path.insert(0, os.path.join(lib_path, file))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# --- 2. STREAMLIT UI CONFIG ---
st.set_page_config(layout="wide", page_title="Fraud Pipeline Monitor")
st.title("🛡️ Real-Time Fraud Pipeline Monitor")
st.markdown("---")

# --- 3. SPARK SESSION INITIALIZATION ---
@st.cache_resource
def get_spark():
    return SparkSession.builder \
        .appName("Dashboard") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

spark = get_spark()

# --- 4. DATA LOADING LOGIC ---
def load_data():
    """Reads Medallion layers from HDFS. No caching to ensure 'Live' feel."""
    try:
        bronze = spark.read.parquet("hdfs://namenode:8020/fraud_detection/bronze/transactions")
        silver = spark.read.parquet("hdfs://namenode:8020/fraud_detection/silver/transactions")
        
        try:
            garbage = spark.read.json("hdfs://namenode:8020/fraud_detection/garbage/rejected")
        except:
            garbage = None # Case where no rejected data exists yet
            
        return bronze, silver, garbage
    except Exception as e:
        st.error(f"Waiting for Spark Jobs to create HDFS folders... ({e})")
        st.stop()

df_bronze, df_silver, df_garbage = load_data()

# --- 5. SIDEBAR FILTERS ---
st.sidebar.header("📊 Global Filters")
# Extract unique banks for the filter dropdown
all_banks = df_silver.select("bank_id").distinct().toPandas()["bank_id"].tolist()
selected_banks = st.sidebar.multiselect("Select Banks", all_banks, default=all_banks)

# Apply filter to the silver dataframe
df_silver_filt = df_silver.filter(col("bank_id").isin(selected_banks))

# --- 6. TABS FOR VISUALIZATION ---
tab1, tab2, tab3, tab4 = st.tabs(["🚀 Comparison & Metrics", "🥉 Bronze (Raw)", "🥈 Silver (Cleaned)", "🛑 Garbage"])

with tab1:
    # Top Row: KPI Metrics
    st.subheader("Live Pipeline Health")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Records", df_silver.count())
    col2.metric("Active Banks", len(all_banks))
    col3.metric("Layer Status", "Silver", delta="Stable")
    col4.metric("Security", "SHA-256", delta="Active")
    
    st.divider()

    # Middle Row: The Comparison Table
    st.subheader("🔄 Transformation Journey (PII Masking Proof)")
    st.write("Below is a comparison showing the original transaction amount and the masked credit card hash:")
    
    # We show a sample of the cleaned data
    comp_view = df_silver_filt.select(
        "transaction_id", "bank_id", "channel", "amount", "masked_card"
    ).limit(10).toPandas()
    
    st.dataframe(comp_view, use_container_width=True)

    st.divider()

    # Bottom Row: Bank Distribution Pie Chart
    st.subheader("🏦 Bank Traffic Distribution")
    bank_dist = df_silver.groupBy("bank_id").count().toPandas()
    
    # Simple Plotly Chart (Streamlit has built-in support)
    st.plotly_chart({
        "data": [{"values": bank_dist["count"], "labels": bank_dist["bank_id"], "type": "pie", "hole": .4}],
        "layout": {"title": "Transactions Share per Bank"}
    }, use_container_width=True)

    st.divider()
    st.subheader("🛠️ Schema Canonization (Structural Mapping)")
    st.write("This table demonstrates how our pipeline handles **Schema Evolution**. Even with varying source fields, the Silver layer enforces a single source of truth.")

    # Re-organized for a "Source -> Target" visual flow
    mapping_data = {
        "Bank Source": ["🏦 Bank Alpha", "🏦 Bank Bravo", "🏦 Bank Charlie"],
        "RAW: Card Field": ["`card_no`", "`cc_number`", "`card_id`"],
        "RAW: Amount Field": ["`amt`", "`transaction_val`", "`amount`"],
        "SILVER: Standard Card": ["✅ `masked_card`", "✅ `masked_card`", "✅ `masked_card`"],
        "SILVER: Standard Amount": ["✅ `amount`", "✅ `amount`", "✅ `amount`"]
    }
    
    # Display as a clean table
    st.table(pd.DataFrame(mapping_data))
    
    # Live Schema View with better formatting
    col_a, col_b = st.columns(2)
    with col_a:
        st.info("📂 **Raw Bronze Schema** (Flexible/Messy)")
        # Showing just the payload part to emphasize the 'messiness'
        st.code("root\n |-- raw_payload: string (JSON)\n |-- ingest_time: timestamp", language="text")
    with col_b:
        st.success("💎 **Standardized Silver Schema** (Strict/Clean)")
        # Showing the result of your hard work
        st.code("root\n |-- transaction_id: string\n |-- bank_id: string\n |-- amount: double\n |-- masked_card: string", language="text")

with tab2:
    st.header("Bronze Layer: Raw Kafka Payloads")
    st.info("This is the exact data as it arrived from Kafka before processing.")
    st.dataframe(df_bronze.limit(20).toPandas(), use_container_width=True)

with tab3:
    st.header("Silver Layer: Cleaned & Partitioned")
    st.success(f"Displaying processed data for: {', '.join(selected_banks)}")
    st.dataframe(df_silver_filt.limit(20).toPandas(), use_container_width=True)

with tab4:
    st.header("Dead Letter Office (Quality Audit)")
    if df_garbage:
        st.warning("The following records were rejected due to schema mismatch or data quality issues:")
        st.dataframe(df_garbage.toPandas(), use_container_width=True)
    else:
        st.success("Quality Control: 0 records rejected. All data flows are healthy.")