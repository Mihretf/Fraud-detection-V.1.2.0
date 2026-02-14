import os
from pathlib import Path
import json
from datetime import datetime
from generators.transaction_generator import TransactionGenerator

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# POS
BRONZE_DIR = PROJECT_ROOT / "storage" / "bronze" / "bank_a" / "pos"
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

class POSProducer:
    def __init__(self, bank_id="BANK_A", channel="POS", batch_size=10):
        self.bank_id = bank_id
        self.channel = channel
        self.batch_size = batch_size
        self.generator = TransactionGenerator(bank_id=bank_id, channel=channel)

    def produce_batch(self):
        events = self.generator.generate_batch(n=self.batch_size, dirty_ratio=0.3)
        for event in events:
            ts = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
            filename = f"{BRONZE_DIR}/{self.bank_id}_{self.channel}_{ts}.json"
            with open(filename, "w") as f:
                json.dump(event, f, indent=2)
        print(f"{len(events)} events produced to {BRONZE_DIR}")


if __name__ == "__main__":
    producer = POSProducer(batch_size=5)
    producer.produce_batch()
