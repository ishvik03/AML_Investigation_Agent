import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean

BASE_DIR = Path(__file__).parent.parent.resolve()
CASE_PATH = BASE_DIR / "generate_cases" / "cases.jsonl"
ALERT_PATH = BASE_DIR / "generate_alerts" / "alerts.jsonl"
CUSTOMER_PATH = BASE_DIR / "customer_profiles.json"


def load_jsonl(path: str):
    with open(path, "r") as f:
        return [json.loads(line) for line in f if line.strip()]

def load_json(path: str):
    with open(path, "r") as f:
        return json.load(f)

def validate_cases():

    print("\n================ CASE VALIDATION REPORT ================\n")

    failures = []
    warnings = []

    cases = load_jsonl(CASE_PATH)
    alerts = load_jsonl(ALERT_PATH)
    customers = load_json(CUSTOMER_PATH)

    if not cases:
        print("❌ No cases found.")
        return

    print(f"Total Cases: {len(cases)}")

    alerts_by_id = {a["alert_id"]: a for a in alerts}
    customer_risk_map = {c["customer_id"]: c["risk_rating"] for c in customers}

    case_ids = set()
    all_alerts_in_cases = []
    case_spans = {}
    case_priority_dist = Counter()
    alerts_per_case = {}

    # --------------------------------------------------
    # Case-level checks
    # --------------------------------------------------
    for case in cases:

        case_id = case["case_id"]

        # Duplicate case_id
        if case_id in case_ids:
            failures.append(f"Duplicate case_id found: {case_id}")
        case_ids.add(case_id)

        alerts_in_case = case.get("alerts", [])

        if not alerts_in_case:
            failures.append(f"Case {case_id} has no alerts.")

        alerts_per_case[case_id] = len(alerts_in_case)

        # Validate alerts exist
        for alert_id in alerts_in_case:
            if alert_id not in alerts_by_id:
                failures.append(f"Case {case_id} references missing alert {alert_id}")
            else:
                all_alerts_in_cases.append(alert_id)

        # Timestamp sanity
        try:
            first_ts = datetime.fromisoformat(case["first_alert_at"])
            last_ts = datetime.fromisoformat(case["last_alert_at"])

            if first_ts > last_ts:
                print(f"first alert at {first_ts} > last alert at {last_ts}")
                failures.append(f"Case {case_id} has invalid time ordering.")

            span_days = (last_ts - first_ts).days
            case_spans[case_id] = span_days

            # AML business sanity: case span too large
            if span_days > 45:
                warnings.append(f"Case {case_id} spans {span_days} days (possible clustering issue).")

        except Exception:
            failures.append(f"Case {case_id} has invalid timestamp format.")

        # Recalculate aggregated score
        recalculated_score = sum(
            alerts_by_id[a]["base_score"] for a in alerts_in_case
            if a in alerts_by_id
        )

        if abs(recalculated_score - case["aggregated_score"]) > 0.01:
            failures.append(f"Case {case_id} aggregated_score mismatch.")

        # Priority logic validation
        has_high_alert = any(
            alerts_by_id[a]["severity"] == "high"
            for a in alerts_in_case if a in alerts_by_id
        )

        has_pattern = any(
            alerts_by_id[a]["rule_id"].startswith("PATTERN")
            for a in alerts_in_case if a in alerts_by_id
        )


        if (has_high_alert or has_pattern) and case["case_priority"] != "high":
            failures.append(f"Case {case_id} should be HIGH priority but is not.")

        if case["case_priority"] == "high" and not (has_high_alert or has_pattern):
            warnings.append(f"Case {case_id} marked HIGH without high-severity or pattern rule.")

        if case["customer_risk_rating"] != customer_risk_map.get(case["customer_id"]):
            failures.append(f"Case {case_id} risk rating mismatch with customer profile.")

        case_priority_dist[case["case_priority"]] += 1

    # --------------------------------------------------
    # Cross-case validation
    # --------------------------------------------------

    # Check alert duplication across cases
    alert_usage_count = Counter(all_alerts_in_cases)
    duplicated_alerts = [a for a, c in alert_usage_count.items() if c > 1]

    if duplicated_alerts:
        failures.append(f"{len(duplicated_alerts)} alerts appear in multiple cases.")

    # High-risk customers should not have zero cases
    high_risk_customers = [
        c["customer_id"] for c in customers if c["risk_rating"] == "High"
    ]

    cases_by_customer = Counter(c["customer_id"] for c in cases)

    high_risk_without_case = [
        cid for cid in high_risk_customers if cases_by_customer.get(cid, 0) == 0
    ]

    if high_risk_without_case:
        warnings.append(
            f"{len(high_risk_without_case)} high-risk customers have no cases."
        )

    # --------------------------------------------------
    # Distribution Metrics
    # --------------------------------------------------

    print("\nCase Priority Distribution:")
    for p, c in case_priority_dist.items():
        print(f"  {p}: {c}")

    avg_alerts = mean(alerts_per_case.values())
    print(f"\nAverage Alerts Per Case: {avg_alerts:.2f}")

    max_case = max(alerts_per_case, key=alerts_per_case.get)
    print(f"Case with most alerts: {max_case} → {alerts_per_case[max_case]}")

    print(f"Max Alerts In Single Case: {max(alerts_per_case.values())}")

    avg_spans = mean(case_spans.values())
    print(f"Average Case Span (days): {avg_spans:.2f}")

    # --------------------------------------------------
    # Final Report
    # --------------------------------------------------

    print("\n================ SUMMARY ================\n")

    if failures:
        print("❌ FAILURES:")
        for f in failures:
            print(" -", f)
    else:
        print("✅ No structural failures detected.")

    if warnings:
        print("\n⚠️ WARNINGS:")
        for w in warnings:
            print(" -", w)
    else:
        print("\n✅ No AML business warnings detected.")

    for case_id, span in case_spans.items():
        if span == 0:
            print(f"Zero span case: {case_id}")

    # --------------------------------------------------
    # ZERO-SPAN CASE ANALYSIS
    # --------------------------------------------------

    zero_span_cases = []
    zero_span_by_customer = defaultdict(int)

    for case_id, span in case_spans.items():
        if span == 0:
            zero_span_cases.append(case_id)

            # find customer for that case
            for case in cases:
                if case["case_id"] == case_id:
                    zero_span_by_customer[case["customer_id"]] += 1
                    break

    total_zero_span = len(zero_span_cases)
    customers_with_zero_span = len(zero_span_by_customer)

    print("\n------------ ZERO SPAN CASE ANALYSIS ------------")
    print(f"Total Zero-Span Cases: {total_zero_span}")
    print(f"Customers Involved: {customers_with_zero_span}")

    if zero_span_by_customer:
        zero_counts = list(zero_span_by_customer.values())

        avg_zero_per_customer = mean(zero_counts)
        min_zero = min(zero_counts)
        max_zero = max(zero_counts)

        customer_min_zero = min(zero_span_by_customer, key=zero_span_by_customer.get)
        customer_max_zero = max(zero_span_by_customer, key=zero_span_by_customer.get)

        print(f"Average Zero-Span Cases Per Customer: {avg_zero_per_customer:.2f}")
        print(f"Minimum Zero-Span Cases For A Customer: {min_zero} (Customer: {customer_min_zero})")
        print(f"Maximum Zero-Span Cases For A Customer: {max_zero} (Customer: {customer_max_zero})")

        # Unique customer list with zero-span cases
        unique_zero_span_customers = list(zero_span_by_customer.keys())

        print("\nUnique Customers With Zero-Span Cases:")
        for cid in unique_zero_span_customers:
            print(f"  {cid}")

        print(f"\nTotal Unique Customers With Zero-Span Cases: {len(unique_zero_span_customers)}")

    print("-------------------------------------------------\n")

    print("\n=========================================================\n")


if __name__ == "__main__":
    validate_cases()