import json
from collections import Counter, defaultdict
from statistics import mean
from datetime import datetime
from pathlib import Path

from Generate_Data import generate_alerts


BASE_DIR = Path(__file__).parent.parent.resolve()
print("BASE_DIR", BASE_DIR)
ALERT_PATH = BASE_DIR/"generate_alerts"/"alerts.jsonl"
customer_path = BASE_DIR/"customer_profiles.json"
customers = load_json(customer_path)


# ----------------------------
# IO
# ----------------------------
def load_jsonl(path):
    rows = []
    with open(path, "r") as f:
        for line in f:
            if line.strip():
                rows.append(json.loads(line))
    return rows


# ----------------------------
# VALIDATION ENGINE
# ----------------------------
def validate_alerts(alerts):

    print("\n================ ALERT VALIDATION REPORT ================\n")

    if not alerts:
        print("❌ No alerts found.")
        return

    failures = []
    warnings = []

    # --------------------------------
    # Basic distributions
    # --------------------------------
    alert_by_rule = Counter(a["rule_id"] for a in alerts)
    alert_by_risk = Counter(a["customer_risk_rating"] for a in alerts)
    alert_by_customer = Counter(a["customer_id"] for a in alerts)
    alert_by_severity = Counter(a["severity"] for a in alerts)
    alert_by_customer_type = Counter(a["customer_type"] for a in alerts)

    total_alerts = len(alerts)

    print(f"Total Alerts: {total_alerts}")
    print("\nAlerts by Rule:")
    for r, c in alert_by_rule.most_common():
        print(f"  {r}: {c} ({c/total_alerts:.2%})")

    print("\nAlerts by Risk Rating:")
    for r, c in alert_by_risk.items():
        print(f"  {r}: {c} ({c/total_alerts:.2%})")

    print("\nAlerts by Severity:")
    for s, c in alert_by_severity.items():
        print(f"  {s}: {c}")

    print("\nAlerts by Customer Type:")
    for t, c in alert_by_customer_type.items():
        print(f"  {t}: {c}")

    # --------------------------------
    # Concentration checks
    # --------------------------------
    customers_gt5 = sum(1 for v in alert_by_customer.values() if v > 5)
    customers_gt10 = sum(1 for v in alert_by_customer.values() if v > 10)

    print(f"\nCustomers with >5 alerts: {customers_gt5}")
    print(f"Customers with >10 alerts: {customers_gt10}")

    if customers_gt10 > 0:
        warnings.append("Some customers have >10 alerts (possible rule explosion).")

    # --------------------------------
    # Rule dominance check
    # --------------------------------
    for rule, count in alert_by_rule.items():
        if count / total_alerts > 0.70:
            warnings.append(f"Rule {rule} dominates >70% of alerts.")

    # --------------------------------
    # Risk realism check
    # --------------------------------
    high = alert_by_risk.get("High", 0)
    medium = alert_by_risk.get("Medium", 0)
    low = alert_by_risk.get("Low", 0)

    if low > high:
        warnings.append("Low-risk customers generating more alerts than High-risk.")

    if high == 0:
        failures.append("No alerts generated for High-risk customers.")

    # --------------------------------
    # Alert score sanity
    # --------------------------------
    scores = [a.get("base_score", 0) for a in alerts]
    avg_score = mean(scores)
    max_score = max(scores)

    print(f"\nAverage Alert Score: {avg_score:.2f}")
    print(f"Max Alert Score: {max_score}")

    if max_score > 1000:
        warnings.append("Unusually high alert score detected.")

    # --------------------------------
    # Timestamp sanity
    # --------------------------------
    malformed_ts = 0
    for a in alerts:
        try:
            datetime.fromisoformat(a["generated_at"])
        except:
            malformed_ts += 1

    if malformed_ts > 0:
        failures.append(f"{malformed_ts} alerts have invalid timestamps.")

    # --------------------------------
    # Empty triggered tx check
    # --------------------------------
    empty_trigger = sum(
        1 for a in alerts if not a.get("triggered_transaction_ids")
    )

    if empty_trigger > 0:
        failures.append(f"{empty_trigger} alerts have empty triggered_transaction_ids.")

    # Count total customers by risk
    total_customers_by_risk = Counter(c["risk_rating"] for c in customers)

    avg_alerts_per_customer = {}

    for risk in ["Low", "Medium", "High"]:
        total_alerts_risk = alert_by_risk.get(risk, 0)
        total_customers_risk = total_customers_by_risk.get(risk, 0)

        if total_customers_risk > 0:
            if risk == "High" :
                print(f"the calculation is {total_alerts_risk}/{total_customers_risk}")
                avg_alerts_per_customer[risk] = total_alerts_risk / total_customers_risk
        else:
            avg_alerts_per_customer[risk] = 0

    print("\nCorrect Average Alerts Per Customer:")
    for k, v in avg_alerts_per_customer.items():
        print(f"{k}: {v:.2f}")
    # --------------------------------
    # Print Final Report
    # --------------------------------
    print("\n================ SUMMARY ================\n")

    if failures:
        print("❌ FAILURES:")
        for f in failures:
            print(" -", f)
    else:
        print("✅ No hard failures.")

    if warnings:
        print("\n⚠️ WARNINGS:")
        for w in warnings:
            print(" -", w)
    else:
        print("\n✅ No warnings detected.")

        # --------------------------------
        # CROSS-RUN VERIFICATION (ALWAYS RUN)
        # --------------------------------
    run_metadata_path = BASE_DIR / "generate_alerts" / "alert_run_metadata.json"

    if run_metadata_path.exists():
        run_metadata = load_json(run_metadata_path)

        expected_alerts = run_metadata.get("alerts_in_file")
        print("\n--- Cross-Run Verification ---")
        print(f"Apply-Rules Alerts: {expected_alerts}")
        print(f"Validation Alerts: {total_alerts}")

        if expected_alerts != total_alerts:
            failures.append(
                f"Alert count mismatch: apply_rules={expected_alerts}, validation={total_alerts}"
            )
            print("❌ ALERT COUNT MISMATCH")
        else:
            print("✅ Alert count matches apply_rules metadata.")
    else:
        warnings.append("No alert_run_metadata.json found for cross-run verification.")

    print("\n=========================================================\n")



# ----------------------------
# MAIN
# ----------------------------
if __name__ == "__main__":
    alerts = load_jsonl(ALERT_PATH)
    validate_alerts(alerts)