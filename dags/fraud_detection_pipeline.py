from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Barney',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'fraud_medallion_pipeline',
    default_args=default_args,
    description='Automated ETL from Bronze to Silver with Triaging',
    schedule_interval=timedelta(minutes=5),
    catchup=False
)

# FIX: Added dag=dag to the operator
process_bronze_to_silver = SparkSubmitOperator(
    task_id='process_bronze_to_silver',
    dag=dag,  # <--- CRITICAL FIX: This connects the task to the DAG
    conn_id='spark_default',
    application='/opt/airflow/jobs/bronze_to_silver.py',
    py_files='/opt/airflow/jobs/schemas.py',
    total_executor_cores=1,
    executor_memory='1G',
    verbose=True, # Added this so you can see Spark logs in Airflow
    conf={
        'spark.master': 'spark://spark:7077'
    }
)

# This is fine, but the 'dag=dag' above is what does the real work
process_bronze_to_silver