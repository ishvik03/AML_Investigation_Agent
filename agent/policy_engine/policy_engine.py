from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple


from agent.tools.risk_signals import calculate_risk_signals_safe
from agent.tools.behavior_signals import extract_behavior_signals_safe
from agent.policy_engine.rule_eval import evaluate_rule, RuleEvalError


class PolicyEngineError(ValueError):
    """Raised when the policy engine cannot evaluate a case safely."""



@dataclass(frozen=True)
class PolicyDecision:
    case_id: str
    customer_id: str
    policy_version: str
    decision: str
    confidence: float
    reasons: List[str]
    required_next_actions: List[str]
    debug_signals: Dict[str, Any]


class PolicyEngine:
    """
    Deterministic AML Policy Evaluator.

    - Loads policy_spec_v1.json
    - Calls Tool1 + Tool2
    - Evaluates rule blocks
    - Applies highest severity conflict resolution
    - Writes audit + decision files separately
    """

    # ---------------------------------------------------------
    # INIT
    # ---------------------------------------------------------

    def __init__(
        self,
        policy_path: Path,
        audit_log_path: Path,
        decision_output_path: Path,
        append_outputs: bool = True,
    ):
        self.policy_path = policy_path
        self.audit_log_path = audit_log_path
        self.decision_output_path = decision_output_path
        self.append_outputs = append_outputs

        self.policy = self._load_policy()

        self.audit_log_path.parent.mkdir(parents=True, exist_ok=True)
        self.decision_output_path.parent.mkdir(parents=True, exist_ok=True)

        # Deterministic action mapping
        self.action_map = {
            "CLOSE_NO_ACTION": ["close_case"],
            "L1_REVIEW": [
                "l1_review",
                "request_basic_context",
                "confirm_customer_activity_purpose",
            ],
            "ESCALATE_L2": [
                "escalate_to_l2",
                "request_source_of_funds",
                "review_transaction_chain",
            ],
            "SAR_REVIEW_L2": [
                "escalate_to_l2",
                "consider_sar_filing",
                "request_source_of_funds",
                "review_transaction_chain",
            ],
        }

    # ---------------------------------------------------------
    # LOAD POLICY
    # ---------------------------------------------------------

    def _load_policy(self) -> Dict[str, Any]:
        if not self.policy_path.exists():
            raise PolicyEngineError(f"Policy file not found: {self.policy_path}")

        with self.policy_path.open("r") as f:
            policy = json.load(f)

        required_keys = [
            "policy_version",
            "decision_hierarchy",
            "hard_escalation_rules",
            "strong_escalation_rules",
            "l1_review_rules",
            "close_rules",
        ]

        for key in required_keys:
            if key not in policy:
                raise PolicyEngineError(f"Policy missing required key: {key}")

        return policy

    # ---------------------------------------------------------
    # BUILD RULE VARIABLES
    # ---------------------------------------------------------

    def _build_rule_variables(
        self,
        risk: Dict[str, Any],
        behavior: Dict[str, Any],
    ) -> Dict[str, Any]:

        return {
            # Tool1
            "aggregated_score": float(risk["aggregated_score"]),
            "total_alerts": int(risk["total_alerts"]),
            "pattern_present": bool(risk["pattern_present"]),
            "high_sev": bool(risk["has_high_sev_alert"]),
            "customer_risk": risk["customer_risk"],
            "priority": risk["priority"],

            # Tool2
            "crypto_percentage": float(behavior["crypto_percentage"]),
            "max_tx_amount": float(behavior["max_tx_amount"]),
            "total_tx_in_window": int(behavior["total_tx_in_window"]),
            "total_volume_in_window": float(behavior["total_volume_in_window"]),
            "any_threshold_exceeded": bool(behavior["any_threshold_exceeded"]),
            "any_pattern_detected": bool(behavior["any_pattern_detected"]),
        }

    # ---------------------------------------------------------
    # RULE BLOCK EVALUATION
    # ---------------------------------------------------------

    def _evaluate_rule_block(
        self,
        decision_label: str,
        rules: List[str],
        variables: Dict[str, Any],
        audit_rows: List[Dict[str, Any]],
    ) -> Tuple[bool, List[str]]:

        matched_rules: List[str] = []

        for rule in rules:
            try:
                matched, normalized = evaluate_rule(rule, variables)

                audit_rows.append({
                    "decision_block": decision_label,
                    "rule": rule,
                    "normalized_rule": normalized,
                    "matched": matched,
                })

                if matched:
                    matched_rules.append(rule)

            except RuleEvalError as e:
                audit_rows.append({
                    "decision_block": decision_label,
                    "rule": rule,
                    "normalized_rule": None,
                    "matched": False,
                    "error": f"{type(e).__name__}: {e}",
                })

                raise PolicyEngineError(
                    f"Invalid rule in policy under {decision_label}: {rule}"
                ) from e

        return len(matched_rules) > 0, matched_rules

    # ---------------------------------------------------------
    # CONFLICT RESOLUTION
    # ---------------------------------------------------------

    def _choose_decision(
        self,
        matches: Dict[str, List[str]]
    ) -> Tuple[str, List[str]]:

        # Highest severity rule wins
        hierarchy = self.policy["decision_hierarchy"]
        severity_order = list(reversed(hierarchy))

        for decision in severity_order:
            if decision in matches and matches[decision]:
                return decision, matches[decision]

        return "CLOSE_NO_ACTION", []

    # ---------------------------------------------------------
    # CONFIDENCE HEURISTIC
    # ---------------------------------------------------------

    def _confidence(
        self,
        decision: str,
        variables: Dict[str, Any],
        matched_rules: List[str]
    ) -> float:

        base = {
            "CLOSE_NO_ACTION": 0.55,
            "L1_REVIEW": 0.65,
            "ESCALATE_L2": 0.75,
            "SAR_REVIEW_L2": 0.85,
        }.get(decision, 0.60)

        bump = min(0.10, 0.02 * len(matched_rules))

        if variables.get("pattern_present") and variables.get("high_sev"):
            bump += 0.05

        return min(0.95, base + bump)

    # ---------------------------------------------------------
    # MAIN ENTRY
    # ---------------------------------------------------------

    def evaluate_enriched_case(self, enriched_case: Dict[str, Any]) -> PolicyDecision:

        # Tool1
        r1 = calculate_risk_signals_safe(enriched_case)
        if not r1["ok"]:
            raise PolicyEngineError(f"Tool1 failed: {r1['error']}")

        # Tool2
        r2 = extract_behavior_signals_safe(enriched_case)
        if not r2["ok"]:
            raise PolicyEngineError(f"Tool2 failed: {r2['error']}")

        risk = r1["signals"]
        behavior = r2["signals"]

        variables = self._build_rule_variables(risk, behavior)

        audit_rows: List[Dict[str, Any]] = []
        matches: Dict[str, List[str]] = {}

        # Evaluate blocks
        blocks = [
            ("SAR_REVIEW_L2", self.policy["hard_escalation_rules"].get("SAR_REVIEW_L2", [])),
            ("ESCALATE_L2", self.policy["strong_escalation_rules"].get("ESCALATE_L2", [])),
            ("L1_REVIEW", self.policy["l1_review_rules"].get("L1_REVIEW", [])),
            ("CLOSE_NO_ACTION", self.policy["close_rules"].get("CLOSE_NO_ACTION", [])),
        ]

        for label, rules in blocks:
            hit, matched = self._evaluate_rule_block(label, rules, variables, audit_rows)
            if hit:
                matches[label] = matched

        decision, matched_rules = self._choose_decision(matches)

        confidence = self._confidence(decision, variables, matched_rules)

        case_id = enriched_case.get("case_id", "unknown")
        customer_id = enriched_case.get("customer_id", "unknown")

        output = PolicyDecision(
            case_id=case_id,
            customer_id=customer_id,
            policy_version=self.policy["policy_version"],
            decision=decision,
            confidence=confidence,
            reasons=matched_rules if matched_rules else ["default_close"],
            required_next_actions=self.action_map.get(decision, []),
            debug_signals=variables,
        )

        self._write_audit(output, audit_rows)
        self._write_decision(output)

        return output

    # ---------------------------------------------------------
    # WRITE AUDIT
    # ---------------------------------------------------------

    def _write_audit(self, decision: PolicyDecision, audit_rows: List[Dict[str, Any]]):

        record = {
            "timestamp": datetime.utcnow().isoformat(),
            "case_id": decision.case_id,
            "customer_id": decision.customer_id,
            "policy_version": decision.policy_version,
            "decision": decision.decision,
            "confidence": decision.confidence,
            "reasons": decision.reasons,
            "signals": decision.debug_signals,
            "rule_evaluations": audit_rows,
        }

        mode = "a" if self.append_outputs else "w"
        with self.audit_log_path.open(mode) as f:
            f.write(json.dumps(record) + "\n")

    # ---------------------------------------------------------
    # WRITE DECISION
    # ---------------------------------------------------------

    def _write_decision(self, decision: PolicyDecision):

        record = {
            "case_id": decision.case_id,
            "customer_id": decision.customer_id,
            "policy_version": decision.policy_version,
            "decision": decision.decision,
            "confidence": decision.confidence,
            "reasons": decision.reasons,
            "required_next_actions": decision.required_next_actions,
            "debug_signals": decision.debug_signals,
        }

        mode = "a" if self.append_outputs else "w"
        with self.decision_output_path.open(mode) as f:
            f.write(json.dumps(record) + "\n")