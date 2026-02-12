import random
import uuid
from datetime import datetime, timedelta

class TransactionGenerator:
    def __init__(self, bank_id="BANK_A", channel="ATM", currency="ETB"):
        self.bank_id = bank_id
        self.channel = channel
        self.currency = currency

    def generate_valid_event(self):
        """Generate a valid Bronze transaction event"""
        amount = round(random.uniform(10, 5000), 2)
        trx_time = datetime.utcnow() - timedelta(minutes=random.randint(0, 1440))
        raw_payload = {
            "atmCode": f"A{random.randint(1, 50)}",
            "trxAmt": str(amount),
            "trxTime": trx_time.strftime("%d/%m/%y %H:%M"),
            "cardNumber": f"****{random.randint(1000, 9999)}"
        }
        event = {
            "bank_id": self.bank_id,
            "channel": self.channel,
            "raw_payload": raw_payload,
            "ingest_time": datetime.utcnow().isoformat(),
            "source_event_id": f"{self.bank_id}_{self.channel}_{uuid.uuid4().hex[:8]}",
            "extra_metadata": {
                "branch_id": f"B{random.randint(1,20)}",
                "device_status": random.choice(["OK", "WARN"])
            }
        }
        return event

    def generate_dirty_event(self):
        """Generate a Bronze event with missing/wrong fields"""
        event = self.generate_valid_event()
        raw_payload = event["raw_payload"]

        # Randomly remove a field
        if random.random() < 0.3:
            key_to_remove = random.choice(list(raw_payload.keys()))
            del raw_payload[key_to_remove]

        # Randomly corrupt a field
        if random.random() < 0.2:
            raw_payload["trxAmt"] = random.choice(["N/A", None, "???"])

        # Randomly add an extra field
        if random.random() < 0.2:
            raw_payload["extra_field"] = random.randint(1, 100)

        return event

    def generate_batch(self, n=10, dirty_ratio=0.3):
        """Generate a batch of events, mixing clean and dirty"""
        events = []
        for _ in range(n):
            if random.random() < dirty_ratio:
                events.append(self.generate_dirty_event())
            else:
                events.append(self.generate_valid_event())
        return events


# Example usage
if __name__ == "__main__":
    gen = TransactionGenerator()
    batch = gen.generate_batch(n=5)
    for e in batch:
        print(e)
