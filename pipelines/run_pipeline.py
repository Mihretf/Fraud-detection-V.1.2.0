import sys
from producers.bank_a.atm_producer import ATMProducer as ATMProducerA
from producers.bank_a.pos_producer import POSProducerA
from producers.bank_a.web_producer import WebProducerA
from producers.bank_a.mobile_producer import MobileProducer as MobileProducerA

from producers.bank_b.atm_producer import ATMProducerB
from producers.bank_b.pos_producer import POSProducerB
from producers.bank_b.web_producer import WebProducerB
from producers.bank_b.mobile_producer import  MobileProducerB

from producers.bank_c.atm_producer import ATMProducerC
from producers.bank_c.pos_producer import POSProducerC
from producers.bank_c.web_producer import WebProducerC
from producers.bank_c.mobile_producer import MobileProducerC

from pipelines.bronze_to_silver import run_etl

def run_full_pipeline(batch_size=10):
    print("=== Starting full pipeline ===")

    print("Producing Bronze events for all banks/channels...")

    # Bank A
    ATMProducerA(batch_size=batch_size).produce_batch()
    POSProducerA(batch_size=batch_size).produce_batch()
    WebProducerA(batch_size=batch_size).produce_batch()
    MobileProducerA(batch_size=batch_size).produce_batch()

    # Bank B
    ATMProducerB(batch_size=batch_size).produce_batch()
    POSProducerB(batch_size=batch_size).produce_batch()
    WebProducerB(batch_size=batch_size).produce_batch()
    MobileProducerB(batch_size=batch_size).produce_batch()

    # Bank C
    ATMProducerC(batch_size=batch_size).produce_batch()
    POSProducerC(batch_size=batch_size).produce_batch()
    WebProducerC(batch_size=batch_size).produce_batch()
    MobileProducerC(batch_size=batch_size).produce_batch()

    # Run ETL for all Bronze events
    print("Running Bronze -> Silver ETL...")
    run_etl()

    print("=== Pipeline complete ===")

if __name__ == "__main__":
    run_full_pipeline(batch_size=10)
