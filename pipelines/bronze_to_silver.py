import os
import json
from datetime import datetime
from pathlib import Path

# Paths
BRONZE_DIR = Path("../../storage/bronze/bank_a/atm")
SILVER_DIR = Path("../../storage/silver/bank_a/atm")
GARBAGE_DIR= Path("../../storage/garbage/bank_a/atm")
SILVER_DIR.mkdir(parents=True, exist_ok=True)

# Canonical Silver schema required fields
REQUIRED_FIELDS = ["bank_id", "channel", "transaction_id", "amount", "currency", "transaction_time", "status", "raw_event_ref", "created_at"]

def map_bronze_to_silver(bronze_event):
    """Map a single Bronze event dict to the canonical Silver schema"""
    raw = bronze_event.get("raw_payload", {})
    silver = {
        "bank_id": bronze_event.get("bank_id"),
        "channel": bronze_event.get("channel"),
        "transaction_id": bronze_event.get("source_event_id") or f"{bronze_event.get('bank_id')}_{uuid.uuid4().hex[:8]}",
        "amount": None,
        "currency": "ETB",  # default if missing
        "transaction_time": None,
        "status": "valid",
        "account_id": raw.get("accountNumber") or None,
        "merchant_id": raw.get("merchantId") or None,
        "raw_event_ref": bronze_event.get("source_event_id"),
        "created_at": datetime.utcnow().isoformat(),
        "extra_attributes": {}
    }

    # Map amount (convert if possible)
    amt = raw.get("trxAmt") or raw.get("amount")
    try:
        silver["amount"] = float(amt)
    except (TypeError, ValueError):
        silver["amount"] = None
        silver["status"] = "invalid"

    # Map transaction_time
    trx_time = raw.get("trxTime") or raw.get("timestamp")
    if trx_time:
        try:
            # Try parsing dd/mm/yy HH:MM
            silver["transaction_time"] = datetime.strptime(trx_time, "%d/%m/%y %H:%M").isoformat()
        except:
            silver["transaction_time"] = None
            silver["status"] = "invalid"
    else:
        silver["transaction_time"] = None
        silver["status"] = "invalid"

    # Store extra fields
    for k, v in raw.items():
        if k not in ["trxAmt", "trxTime", "accountNumber", "merchantId"]:
            silver["extra_attributes"][k] = v

    return silver

def clean_silver_event(event, garbage_dir=None):
    """
    Clean a Silver event:
    - Validate amounts and timestamps
    - Flag missing required fields
    - Save invalid events to garbage_dir
    """
    issues = []

    # Amount
    if event["amount"] in ["N/A", "???", None]:
        issues.append("missing_amount")
        event["amount"] = None
        event["status"] = "invalid"

    # Transaction time
    try:
        if event["transaction_time"]:
            datetime.fromisoformat(event["transaction_time"])
        else:
            issues.append("missing_transaction_time")
            event["status"] = "invalid"
    except Exception:
        issues.append("invalid_timestamp")
        event["transaction_time"] = None
        event["status"] = "invalid"

    # Required fields
    for field in ["bank_id", "channel", "transaction_id", "amount", "transaction_time"]:
        if event.get(field) is None:
            issues.append(f"missing_{field}")
            event["status"] = "invalid"

    # Store invalid events in garbage
    if garbage_dir and issues:
        for issue in set(issues):
            issue_folder = Path(garbage_dir) / issue
            issue_folder.mkdir(parents=True, exist_ok=True)
            filename = issue_folder / f"{event['transaction_id']}.json"
            with open(filename, "w") as f:
                json.dump(event, f, indent=2)

    return event, issues

def run_etl():
    bronze_files = list(BRONZE_DIR.glob("*.json"))
    print(f"Found {len(bronze_files)} bronze events to process.")

    for file_path in bronze_files:
        with open(file_path, "r") as f:
            bronze_event = json.load(f)

        # Map Bronze → Silver
        silver_event = map_bronze_to_silver(bronze_event)

        # Clean and handle garbage
        silver_event, issues = clean_silver_event(silver_event, garbage_dir=GARBAGE_DIR)

        # Only write valid events to Silver
        if silver_event["status"] == "valid":
            silver_filename = SILVER_DIR / f"{silver_event['transaction_id']}.json"
            with open(silver_filename, "w") as f:
                json.dump(silver_event, f, indent=2)

    print(f"ETL complete. Silver events written to {SILVER_DIR}")
    print(f"Invalid events stored in {GARBAGE_DIR}")

if __name__ == "__main__":
    run_etl()