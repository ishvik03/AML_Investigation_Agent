import uuid
from datetime import datetime

def create_alert(customer, rule, triggered_tx_ids, event_time ,window_start=None, window_end=None):
    return {
        "alert_id": str(uuid.uuid4()),
        "generated_at": datetime.utcnow().isoformat(),
        "alert_event_time": event_time,
        "customer_id": customer["customer_id"],
        "customer_risk_rating": customer["risk_rating"],
        "customer_type": customer["customer_type"],
        "account_status": customer["account_status"],

        "rule_id": rule["rule_id"],
        "rule_name": rule["name"],
        "rule_type": rule["type"],
        "severity": rule["severity"],
        "base_score": rule["base_score"],

        "triggered_transaction_ids": triggered_tx_ids,
        "window_start": window_start,
        "window_end": window_end
    }
