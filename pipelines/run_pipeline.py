import os
import sys
from producers.bank_a.atm_producer import ATMProducer
from pipelines.bronze_to_silver import run_etl

def run_full_pipeline(batch_size=10):
    print("=== Starting full pipeline ===")

    # Step 1: Produce Bronze events
    print("Producing Bronze events...")
    producer = ATMProducer(batch_size=batch_size)
    producer.produce_batch()

    # Step 2: Run ETL (Bronze -> Silver)
    print("Running Bronze -> Silver ETL...")
    run_etl()

    print("=== Pipeline complete ===")

if __name__ == "__main__":
    run_full_pipeline(batch_size=10)
