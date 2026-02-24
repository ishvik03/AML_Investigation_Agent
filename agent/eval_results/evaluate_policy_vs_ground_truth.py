from __future__ import annotations

import json
from pathlib import Path
from collections import defaultdict
from typing import Dict, List

from agent.policy_engine.policy_engine import PolicyEngine



# ----------------------------------------------------------
# Utility
# ----------------------------------------------------------

def load_jsonl(path: Path) -> List[Dict]:
    rows = []
    with path.open("r") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


# ----------------------------------------------------------
# Evaluation
# ----------------------------------------------------------

def main():

    repo_root = Path(__file__).resolve().parent.parent.parent
    print(repo_root)

    enriched_path = repo_root / "Generate_Data" / "enriched_cases" / "enriched_cases.jsonl"
    ground_truth_path = repo_root / "Generate_Data" / "case_results" / "case_ground_truth.jsonl"

    policy_path = repo_root / "policies" / "policy_v1.json"
    audit_path = repo_root / "policies" / "audits" / "eval_audit_tmp.jsonl"
    decision_output_path = repo_root / "agent" / "eval_results" / "eval_tmp_decisions.jsonl"

    # Initialize engine
    engine = PolicyEngine(
        policy_path=policy_path,
        audit_log_path=audit_path,
        decision_output_path=decision_output_path,
        append_outputs=False
    )

    print("Loading data...")
    enriched_cases = load_jsonl(enriched_path)
    ground_truth_cases = load_jsonl(ground_truth_path)

    print(f"Enriched cases: {len(enriched_cases)}")
    print(f"Ground truth cases: {len(ground_truth_cases)}")

    # Build GT lookup
    gt_map = {c["case_id"]: c for c in ground_truth_cases}

    predictions = {}
    mismatches = []

    for case in enriched_cases:
        case_id = case["case_id"]

        if case_id not in gt_map:
            print(f"⚠ No ground truth for case {case_id}")
            continue

        decision = engine.evaluate_enriched_case(case)
        predictions[case_id] = decision.decision

        true_decision = gt_map[case_id]["decision"]

        if decision.decision != true_decision:
            mismatches.append({
                "case_id": case_id,
                "predicted": decision.decision,
                "true": true_decision
            })

    # ------------------------------------------------------
    # Metrics
    # ------------------------------------------------------

    total = len(predictions)
    correct = sum(
        1 for cid in predictions
        if predictions[cid] == gt_map[cid]["decision"]
    )

    accuracy = correct / total if total > 0 else 0.0

    print("\n==============================")
    print(f"Total evaluated: {total}")
    print(f"Accuracy: {accuracy:.4f}")
    print("==============================\n")

    # ------------------------------------------------------
    # Confusion Matrix
    # ------------------------------------------------------

    labels = [
        "CLOSE_NO_ACTION",
        "L1_REVIEW",
        "ESCALATE_L2",
        "SAR_REVIEW_L2"
    ]

    matrix = defaultdict(lambda: defaultdict(int))

    for cid, pred in predictions.items():
        true = gt_map[cid]["decision"]
        matrix[true][pred] += 1

    print("Confusion Matrix:")
    header = "True \\ Pred".ljust(20) + "".join(l.ljust(18) for l in labels)
    print(header)

    for true_label in labels:
        row = true_label.ljust(20)
        for pred_label in labels:
            row += str(matrix[true_label][pred_label]).ljust(18)
        print(row)

    # ------------------------------------------------------
    # Escalation Metrics
    # ------------------------------------------------------

    escalation_labels = {"ESCALATE_L2", "SAR_REVIEW_L2"}

    tp = fp = fn = tn = 0

    for cid, pred in predictions.items():
        true = gt_map[cid]["decision"]

        pred_escalated = pred in escalation_labels
        true_escalated = true in escalation_labels

        if pred_escalated and true_escalated:
            tp += 1
        elif pred_escalated and not true_escalated:
            fp += 1
        elif not pred_escalated and true_escalated:
            fn += 1
        else:
            tn += 1

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0

    print("\nEscalation Metrics:")
    print(f"TP: {tp}")
    print(f"FP (Over-escalation): {fp}")
    print(f"FN (Missed escalation): {fn}")
    print(f"TN: {tn}")
    print(f"Escalation Precision: {precision:.4f}")
    print(f"Escalation Recall: {recall:.4f}")

    # ------------------------------------------------------
    # Mismatches
    # ------------------------------------------------------

    print(f"\nTotal mismatches: {len(mismatches)}")

    if mismatches:
        mismatch_path = repo_root / "Generate_Data" / "Case_results" / "policy_mismatches.jsonl"
        with mismatch_path.open("w") as f:
            for m in mismatches:
                f.write(json.dumps(m) + "\n")
        print(f"Mismatches saved to: {mismatch_path}")


if __name__ == "__main__":
    main()