import os
from pathlib import Path
import json
from confluent_kafka import Producer
from datetime import datetime
from generators.transaction_generator import TransactionGenerator

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "bank_a_atm_transactions"

# PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# ATM
# BRONZE_DIR = PROJECT_ROOT / "storage" / "bronze" / "bank_a" / "atm"
# BRONZE_DIR.mkdir(parents=True, exist_ok=True)

class ATMProducer:
    def __init__(self, bank_id="BANK_A", channel="ATM", batch_size=10):
        self.bank_id = bank_id
        self.channel = channel
        self.batch_size = batch_size
        self.generator = TransactionGenerator(bank_id=bank_id, channel=channel)

        # Initialize Kafka producer
        self.producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})
    def delivery_report(self, err, msg):
        """Callback for Kafka delivery status"""
        if err:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def produce_batch(self):
        """Generate a batch of events and send them to Kafka"""
        events = self.generator.generate_batch(n=self.batch_size)
        for event in events:
            # Include timestamp in event if needed
            event["event_timestamp"] = datetime.utcnow().isoformat()
            # Send JSON to Kafka
            self.producer.produce(
                TOPIC,
                json.dumps(event).encode("utf-8"),
                callback=self.delivery_report
            )

        # Ensure all messages are sent
        self.producer.flush()
        print(f"{len(events)} events sent to Kafka topic {TOPIC}")


# Example usage
if __name__ == "__main__":
    producer = ATMProducer(batch_size=5)
    producer.produce_batch()
