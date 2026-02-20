from confluent_kafka import Producer
from generators.transaction_generator import TransactionGenerator
from datetime import datetime
import json

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "bank_a_pos_transactions"

class POSProducerA:
    def __init__(self, bank_id="BANK_A", channel="POS", batch_size=10):
        self.bank_id = bank_id
        self.channel = channel
        self.batch_size = batch_size
        self.generator = TransactionGenerator(bank_id=bank_id, channel=channel)
        self.producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})

    def delivery_report(self, err, msg):
        if err:
            print(f"Delivery failed: {err}")
        else:
            print(f"Delivered to {msg.topic()} [{msg.partition()}]")

    def produce_batch(self):
        events = self.generator.generate_batch(n=self.batch_size, dirty_ratio=0.3)
        for event in events:
            event["event_timestamp"] = datetime.utcnow().isoformat()
            self.producer.produce(TOPIC, json.dumps(event).encode("utf-8"), callback=self.delivery_report)
        self.producer.flush()
        print(f"{len(events)} events sent to Kafka topic {TOPIC}")


if __name__ == "__main__":
    producer = POSProducerA(batch_size=5)
    producer.produce_batch()