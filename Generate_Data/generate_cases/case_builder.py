import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
import json



BASE_DIR = Path(__file__).parent.parent.resolve()
ALERT_PATH = BASE_DIR / "generate_alerts" / "alerts.jsonl"
CASE_OUTPUT_PATH = BASE_DIR / "generate_cases" / "cases.jsonl"

WINDOW_DAYS = 14


def load_jsonl(path: str):
    with open(path, "r") as f:
        return [json.loads(line) for line in f if line.strip()]

def group_alerts_by_customer(alerts):
    grouped = defaultdict(list)
    for alert in alerts:
        grouped[alert["customer_id"]].append(alert)
    return grouped


def cluster_alerts_by_time(alerts):
    alerts_sorted = sorted(alerts, key=lambda a: a["alert_event_time"])
    clusters = []
    current_cluster = []
    cluster_start_time = None

    for alert in alerts_sorted:
        alert_time = datetime.fromisoformat(alert["alert_event_time"])

        if not current_cluster:
            current_cluster = [alert]
            cluster_start_time = alert_time
            continue

        # FIX: compare to FIRST alert in cluster (fixed window)
        if alert_time - cluster_start_time <= timedelta(days=WINDOW_DAYS):
            current_cluster.append(alert)
        else:
            clusters.append(current_cluster)
            current_cluster = [alert]
            cluster_start_time = alert_time

    if current_cluster:
        clusters.append(current_cluster)

    return clusters

def determine_case_priority(alert_cluster):
    aggregated_score = sum(a.get("base_score", 0) for a in alert_cluster)

    has_high = any(a["severity"] == "high" for a in alert_cluster)
    has_pattern = any(a["rule_id"].startswith("PATTERN") for a in alert_cluster)

    if has_high or has_pattern:
        return "high", aggregated_score
    elif aggregated_score > 100:
        return "medium", aggregated_score
    else:
        return "low", aggregated_score


def build_case(customer_id, alert_cluster):
    case_id = str(uuid.uuid4())
    priority, aggregated_score = determine_case_priority(alert_cluster)

    return {
        "case_id": case_id,
        "customer_id": customer_id,
        "customer_risk_rating": alert_cluster[0]["customer_risk_rating"],
        "created_at": datetime.utcnow().isoformat(),
        "status": "open",
        "alerts": [a["alert_id"] for a in alert_cluster],
        "total_alerts": len(alert_cluster),
        "aggregated_score": aggregated_score,
        "case_priority": priority,
        "first_alert_at": alert_cluster[0]["alert_event_time"],
        "last_alert_at": alert_cluster[-1]["alert_event_time"]
    }


def main():
    alerts = load_jsonl(ALERT_PATH)
    alerts_by_customer = group_alerts_by_customer(alerts)

    cases = []

    for customer_id, customer_alerts in alerts_by_customer.items():
        clusters = cluster_alerts_by_time(customer_alerts)

        for cluster in clusters:
            case = build_case(customer_id, cluster)
            cases.append(case)

    with open(CASE_OUTPUT_PATH, "w") as f:
        for case in cases:
            f.write(json.dumps(case) + "\n")

    print(f"Generated {len(cases)} cases.")


if __name__ == "__main__":
    main()