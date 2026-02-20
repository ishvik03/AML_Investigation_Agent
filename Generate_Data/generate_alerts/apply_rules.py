print("the package is ", __package__)

import json
from datetime import datetime
from pathlib import Path

from .config.utils.loader import load_json, load_jsonl
from .config.utils.writer import write_jsonl
from .config.utils.services.indexing import index_by_customer
from .rules.single_tx_rules import apply_single_transaction_rules
from .rules.aggregation_rules import apply_aggregation_rules


def main():
    BASE_DIR = Path(__file__).resolve().parent.parent
    print("BASE_DIR", BASE_DIR)

    # ----------------------------
    # LOAD DATA
    # ----------------------------
    tx_path = BASE_DIR / "generate_transactions" / "transactions.jsonl"
    transactions = load_jsonl(tx_path)

    customer_path = BASE_DIR / "customer_profiles.json"
    customers = load_json(customer_path)

    rules_path = BASE_DIR / "generate_alerts" / "config" / "rules_config.json"
    rules_doc = load_json(rules_path)

    customers_by_id = {c["customer_id"]: c for c in customers}
    rules = rules_doc["rules"]

    single_rules = [r for r in rules if r["type"] == "single_transaction"]
    agg_rules = [r for r in rules if r["type"] in ("aggregation", "pattern")]

    tx_by_customer = index_by_customer(transactions)

    # ----------------------------
    # APPLY RULES
    # ----------------------------
    alerts = []
    alerts += apply_single_transaction_rules(transactions, single_rules, customers_by_id)
    alerts += apply_aggregation_rules(tx_by_customer, agg_rules, customers_by_id)

    # ----------------------------
    # WRITE ALERTS
    # ----------------------------
    alert_path = BASE_DIR / "generate_alerts" / "alerts.jsonl"
    write_jsonl(alert_path, alerts)

    print(f"\nTransactions processed: {len(transactions)}")
    print(f"Alerts generated: {len(alerts)}")

    # ----------------------------
    # WRITE VERIFICATION
    # ----------------------------
    written_alerts = load_jsonl(alert_path)

    memory_count = len(alerts)
    file_count = len(written_alerts)

    print("\n--- Alert Write Verification ---")
    print(f"In-memory alerts: {memory_count}")
    print(f"Alerts in file: {file_count}")

    if file_count == memory_count:
        print("✅ Verification PASSED")
        verification_status = "PASSED"
    else:
        print("❌ Verification FAILED")
        verification_status = "FAILED"

    # ----------------------------
    # SAVE RUN METADATA
    # ----------------------------
    metadata = {
        "timestamp": datetime.utcnow().isoformat(),
        "transactions_processed": len(transactions),
        "alerts_in_memory": memory_count,
        "alerts_in_file": file_count,
        "verification_status": verification_status
    }

    metadata_path = BASE_DIR / "generate_alerts" / "alert_run_metadata.json"

    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"\n📝 Saved run metadata to {metadata_path}")


if __name__ == "__main__":
    main()