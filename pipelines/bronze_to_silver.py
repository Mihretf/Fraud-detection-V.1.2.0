import json
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BRONZE_DIR = PROJECT_ROOT / "storage" / "bronze"
SILVER_DIR = PROJECT_ROOT / "storage" / "silver"
GARBAGE_DIR = PROJECT_ROOT / "storage" / "garbage"

# Define required fields for canonical Silver schema
REQUIRED_FIELDS = ["transaction_id", "amount", "timestamp", "bank_id", "channel"]

def valid_timestamp(ts):
    """Check if timestamp is valid ISO format"""
    try:
        datetime.fromisoformat(ts)
        return True
    except Exception:
        return False

def clean_event(event):
    """Return list of issues found in the event"""
    issues = []
    for field in REQUIRED_FIELDS:
        if field not in event or event[field] is None:
            issues.append(f"missing_{field}")

    if "amount" in event and (not isinstance(event["amount"], (int, float)) or event["amount"] < 0):
        issues.append("invalid_amount")

    if "timestamp" in event and not valid_timestamp(event["timestamp"]):
        issues.append("invalid_timestamp")

    return issues

def map_to_canonical(event):
    """
    Map bank-specific fields to canonical Silver schema.
    For now, assume your generator already matches canonical field names where possible.
    """
    canonical = {
        "transaction_id": event.get("transaction_id"),
        "amount": event.get("amount"),
        "timestamp": event.get("timestamp"),
        "bank_id": event.get("bank_id"),
        "channel": event.get("channel")
    }
    return canonical

def run_etl():
    print("Running Bronze -> Silver ETL...")

    for bank_folder in BRONZE_DIR.iterdir():
        if not bank_folder.is_dir():
            continue
        for channel_folder in bank_folder.iterdir():
            if not channel_folder.is_dir():
                continue

            silver_path = SILVER_DIR / bank_folder.name / channel_folder.name
            garbage_path = GARBAGE_DIR / bank_folder.name / channel_folder.name
            silver_path.mkdir(parents=True, exist_ok=True)
            garbage_path.mkdir(parents=True, exist_ok=True)

            bronze_files = list(channel_folder.glob("*.json"))
            print(f"Found {len(bronze_files)} bronze events to process in {bank_folder.name}/{channel_folder.name}")

            for file_path in bronze_files:
                with open(file_path, "r") as f:
                    event = json.load(f)

                issues = clean_event(event)
                if issues:
                    for issue in issues:
                        issue_folder = garbage_path / issue
                        issue_folder.mkdir(exist_ok=True)
                        dest_file = issue_folder / file_path.name
                        with open(dest_file, "w") as gf:
                            json.dump(event, gf, indent=2)
                else:
                    silver_event = map_to_canonical(event)
                    dest_file = silver_path / file_path.name
                    with open(dest_file, "w") as sf:
                        json.dump(silver_event, sf, indent=2)

    print("ETL complete. Silver events written to:", SILVER_DIR)
    print("Invalid events stored in:", GARBAGE_DIR)


if __name__ == "__main__":
    run_etl()
