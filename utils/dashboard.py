import sys
import os
import streamlit as st
import pandas as pd
from datetime import datetime

# --- 1. SMART SPARK LOADER ---
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
from pyspark.sql.functions import col, max as spark_max

# --- 2. STREAMLIT UI CONFIG ---
st.set_page_config(layout="wide", page_title="AAU Fraud Pipeline Monitor")
st.title("🛡️ Real-Time Fraud Pipeline Monitor")
st.markdown(f"**Status:** `Active` | **Node:** `-Data-Cluster` | **Engineer:** Mihret")
st.markdown("---")

# --- 3. SPARK SESSION INITIALIZATION ---
@st.cache_resource
def get_spark():
    return SparkSession.builder \
        .appName("Synthetic_Dashboard") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

spark = get_spark()

# --- 4. DATA LOADING LOGIC ---
def load_data():
    try:
        bronze = spark.read.parquet("hdfs://namenode:8020/projects/synthetic/bronze")
        silver = spark.read.parquet("hdfs://namenode:8020/projects/synthetic/silver/")
        
        try:
            garbage = spark.read.json("hdfs://namenode:8020/projects/synthetic/garbage/rejected")
        except:
            garbage = None 
            
        return bronze, silver, garbage
    except Exception as e:
        st.error(f"Waiting for Spark Jobs to populate HDFS... (Make sure Airflow has run successfully)")
        st.stop()

df_bronze, df_silver, df_garbage = load_data()

# --- 5. SIDEBAR FILTERS ---
st.sidebar.header("📊 Global Filters")
all_banks = df_silver.select("bank_id").distinct().toPandas()["bank_id"].tolist()
selected_banks = st.sidebar.multiselect("Select Banks", all_banks, default=all_banks)
df_silver_filt = df_silver.filter(col("bank_id").isin(selected_banks))

# --- 6. TABS FOR VISUALIZATION ---
tab1, tab2, tab3, tab4 = st.tabs(["🚀 Comparison & Metrics", "🥉 Bronze (Raw)", "🥈 Silver (Cleaned)", "🛑 Garbage"])

with tab1:
    # KPI Metrics Row
    st.subheader("Live Pipeline Health")
    total_bronze = df_bronze.count()
    total_silver = df_silver.count()
    health_rate = (total_silver / total_bronze * 100) if total_bronze > 0 else 100
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Records", total_silver)
    col2.metric("Data Health", f"{health_rate:.1f}%", delta="Stable")
    col3.metric("Security", "SHA-256", delta="Masking On")
    
    # Calculate Freshness
    last_processed = df_silver.select(spark_max("processed_at")).collect()[0][0]
    col4.metric("Last Pulse", last_processed.strftime("%H:%M:%S") if last_processed else "N/A")
    
    st.divider()

    # Transformation Proof
    st.subheader("🔄 Transformation Journey (PII Masking Proof)")
    comp_view = df_silver_filt.select(
        "transaction_id", "bank_id", "amount", "masked_card", "quality_score"
    ).limit(10).toPandas()
    st.dataframe(comp_view, use_container_width=True)

    # Bank Distribution Chart
    st.divider()
    st.subheader("🏦 Bank Traffic Distribution")
    bank_dist = df_silver.groupBy("bank_id").count().toPandas()
    st.plotly_chart({
        "data": [{"values": bank_dist["count"], "labels": bank_dist["bank_id"], "type": "pie", "hole": .4}],
        "layout": {"title": "Transaction Volume Share"}
    }, use_container_width=True)

    # Schema Mapping Table (For Documentation)
    st.divider()
    st.subheader("🛠️ Schema Mapping Logic")
    mapping_data = {
        "Source Field": ["cardNumber", "trxAmt", "trxTime"],
        "Transformation": ["SHA-256 Hashing", "Double Casting", "Timestamp Normalization"],
        "Silver Field": ["✅ masked_card", "✅ amount", "✅ event_timestamp"]
    }
    st.table(pd.DataFrame(mapping_data))

with tab2:
    st.header("Bronze Layer: Raw Ingestion")
    st.info("Direct Parquet read from `hdfs:///projects/synthetic/bronze`")
    st.dataframe(df_bronze.limit(20).toPandas(), use_container_width=True)

with tab3:
    st.header("Silver Layer: Delivery & Export")
    
    # DELIVERY SECTION FOR BILISE
    st.warning("🤝 **ML Handover Ready**")
    csv_export = df_silver_filt.toPandas().to_csv(index=False).encode('utf-8')
    
    st.download_button(
        label="📥 Download Cleaned CSV for Bilise",
        data=csv_export,
        file_name=f'cleaned_batch_{datetime.now().strftime("%Y%m%d_%H%M")}.csv',
        mime='text/csv',
        help="Click to generate a clean CSV file for model training."
    )
    
    st.divider()
    st.success(f"Viewing processed data for: {', '.join(selected_banks)}")
    st.dataframe(df_silver_filt.limit(30).toPandas(), use_container_width=True)

with tab4:
    st.header("🛑 Garbage / Rejected Records")
    if df_garbage:
        st.error(f"Identified {df_garbage.count()} records failing validation.")
        st.dataframe(df_garbage.toPandas(), use_container_width=True)
    else:
        st.success("No rejected records found in current batch.")