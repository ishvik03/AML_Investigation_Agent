import json
from pathlib import Path
from statistics import mean

BASE_DIR = Path(__file__).parent.parent.resolve()

# INPUT: your enriched cases (the file you just generated)
ENRICHED_CASES_PATH = BASE_DIR / "enriched_cases" / "enriched_cases.jsonl"

# OUTPUT: ground truth labels for evaluation
GROUND_TRUTH_PATH = BASE_DIR / "case_results" / "case_ground_truth.jsonl"

POLICY_VERSION = "GT_V1"


def load_jsonl(path: Path):
    with open(path, "r") as f:
        return [json.loads(line) for line in f if line.strip()]


def write_jsonl(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


def safe_get(d, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def extract_signals(case: dict) -> dict:
    risk_rating = safe_get(case, "customer_snapshot", "risk_rating", default="unknown")
    priority = safe_get(case, "case_metadata", "priority", default="unknown")
    aggregated_score = float(safe_get(case, "case_metadata", "aggregated_score", default=0) or 0)
    total_alerts = int(safe_get(case, "case_metadata", "total_alerts", default=0) or 0)
    pattern_present = bool(safe_get(case, "case_metadata", "pattern_present", default=False))

    alerts_in_case = case.get("alerts_in_case", []) or []
    flagged_txs = case.get("flagged_transactions", []) or []
    behavior = case.get("behavior_snapshot", {}) or {}

    has_high_sev_alert = any((a.get("severity") == "high") for a in alerts_in_case)
    any_pattern_rule = any((str(a.get("rule_id", "")).startswith("PATTERN")) for a in alerts_in_case)

    crypto_percentage = float(behavior.get("crypto_percentage") or 0)
    max_tx_amount = float(behavior.get("max_tx_amount") or 0)

    any_threshold_exceeded = any(
        (tx.get("rule_trigger_reason", {}) or {}).get("threshold_exceeded") is True
        for tx in flagged_txs
    )
    any_pattern_detected = any(
        (tx.get("rule_trigger_reason", {}) or {}).get("pattern_detected") is True
        for tx in flagged_txs
    )

    # Useful for rules / debugging
    return {
        "risk_rating": risk_rating,
        "priority": priority,
        "aggregated_score": aggregated_score,
        "total_alerts": total_alerts,
        "pattern_present": pattern_present,
        "has_high_sev_alert": has_high_sev_alert,
        "any_pattern_rule": any_pattern_rule,
        "crypto_percentage": crypto_percentage,
        "max_tx_amount": max_tx_amount,
        "any_threshold_exceeded": any_threshold_exceeded,
        "any_pattern_detected": any_pattern_detected,
    }


def decide_ground_truth(signals: dict):
    """
    Returns: (decision, confidence, reasons, required_next_actions)
    Deterministic, consistent, eval-friendly.
    """
    rr = signals["risk_rating"]
    score = signals["aggregated_score"]
    total_alerts = signals["total_alerts"]
    pattern = signals["pattern_present"] or signals["any_pattern_rule"] or signals["any_pattern_detected"]
    high_sev = signals["has_high_sev_alert"]
    crypto_pct = signals["crypto_percentage"]
    max_amt = signals["max_tx_amount"]
    threshold_exceeded = signals["any_threshold_exceeded"]

    reasons = []
    actions = []

    # --- Decision Rule 1: SAR_REVIEW_L2 (strongest)
    if (pattern and high_sev and score >= 300) or \
       (high_sev and score >= 400) or \
       (pattern and score >= 450) or \
       (rr == "High" and (pattern or score >= 350)):

        if pattern: reasons.append("pattern_present")
        if high_sev: reasons.append("high_severity_alert")
        if score >= 300: reasons.append("aggregated_score>=300")
        if rr == "High": reasons.append("customer_risk=High")
        if crypto_pct >= 30: reasons.append("crypto_pct>=30")
        if max_amt >= 25000: reasons.append("max_tx_amount>=25000")

        actions = [
            "escalate_to_l2",
            "request_source_of_funds",
            "request_exchange_kyc",
            "review_transaction_chain",
            "consider_sar_filing",
        ]

        # Confidence: deterministic but bounded
        confidence = min(0.95, 0.80 + (0.05 if high_sev else 0) + (0.05 if pattern else 0) + (0.05 if rr == "High" else 0))
        return "SAR_REVIEW_L2", confidence, reasons, actions

    # --- Decision Rule 2: ESCALATE_L2
    if (pattern and score >= 250) or \
       (high_sev and score >= 250) or \
       (total_alerts >= 8 and score >= 250) or \
       (crypto_pct >= 30 and pattern) or \
       (max_amt >= 25000 and (pattern or high_sev)):

        if pattern: reasons.append("pattern_present")
        if high_sev: reasons.append("high_severity_alert")
        if score >= 250: reasons.append("aggregated_score>=250")
        if total_alerts >= 8: reasons.append("total_alerts>=8")
        if crypto_pct >= 30: reasons.append("crypto_pct>=30")
        if max_amt >= 25000: reasons.append("max_tx_amount>=25000")

        actions = [
            "escalate_to_l2",
            "request_supporting_docs",
            "review_counterparty_countries",
            "review_behavior_snapshot",
        ]

        confidence = min(0.90, 0.70 + (0.08 if pattern else 0) + (0.07 if high_sev else 0) + (0.05 if score >= 325 else 0))
        return "ESCALATE_L2", confidence, reasons, actions

    # --- Decision Rule 3: L1_REVIEW
    if (score >= 120) or \
       (total_alerts >= 3) or \
       (rr == "Medium" and score >= 80) or \
       threshold_exceeded:

        if score >= 120: reasons.append("aggregated_score>=120")
        if total_alerts >= 3: reasons.append("total_alerts>=3")
        if rr == "Medium" and score >= 80: reasons.append("customer_risk=Medium_and_score>=80")
        if threshold_exceeded: reasons.append("threshold_exceeded")

        actions = [
            "l1_review",
            "request_basic_context",
            "confirm_customer_activity_purpose",
        ]

        confidence = min(0.85, 0.55 + (0.10 if score >= 200 else 0) + (0.05 if total_alerts >= 5 else 0) + (0.05 if rr != "Low" else 0))
        return "L1_REVIEW", confidence, reasons, actions

    # --- Decision Rule 4: CLOSE_NO_ACTION
    reasons = ["insufficient_risk_signal"]
    actions = ["close_case"]
    confidence = 0.60
    return "CLOSE_NO_ACTION", confidence, reasons, actions


def main():
    cases = load_jsonl(ENRICHED_CASES_PATH)
    if not cases:
        print(f"❌ No enriched cases found at: {ENRICHED_CASES_PATH}")
        return

    out = []
    decision_counts = {}

    for case in cases:
        signals = extract_signals(case)
        decision, confidence, reasons, actions = decide_ground_truth(signals)

        decision_counts[decision] = decision_counts.get(decision, 0) + 1

        out.append({
            "case_id": case.get("case_id"),
            "customer_id": case.get("customer_id"),
            "policy_version": POLICY_VERSION,
            "decision": decision,
            "confidence": round(float(confidence), 3),
            "reasons": reasons,
            "required_next_actions": actions,
            "debug_signals": signals
        })

    write_jsonl(GROUND_TRUTH_PATH, out)

    print("\n================ GROUND TRUTH GENERATION ================\n")
    print(f"Input cases: {len(cases)}")
    print(f"Output labels: {len(out)}")
    print(f"Wrote: {GROUND_TRUTH_PATH}\n")

    print("Decision distribution:")
    for k, v in sorted(decision_counts.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v} ({v/len(out):.2%})")

    print("\n========================================================\n")


if __name__ == "__main__":
    main()