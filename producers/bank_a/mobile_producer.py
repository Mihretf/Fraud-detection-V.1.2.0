import os
from pathlib import Path
import json
from confluent_kafka import Producer
from datetime import datetime
from generators.transaction_generator import TransactionGenerator
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# MOBILE
# BRONZE_DIR = PROJECT_ROOT / "storage" / "bronze" / "bank_a" / "mobile"
# BRONZE_DIR.mkdir(parents=True, exist_ok=True)

# Kafka config
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "bank_a_mobile_transactions"

class MobileProducer:
    def __init__(self, bank_id="BANK_A", channel="MOBILE", batch_size=10):
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
        events = self.generator.generate_batch(n=self.batch_size, dirty_ratio=0.3)
        for event in events:
            # Add timestamp
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

if __name__ == "__main__":
    producer = MobileProducer(batch_size=5)
    producer.produce_batch()
