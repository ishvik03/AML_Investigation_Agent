from __future__ import annotations
from typing import Any, Dict, List
from agent.tools.risk_signals import calculate_risk_signals_safe
from agent.tools.behavior_signals import extract_behavior_signals_safe
from agent.policy_engine.policy_engine import PolicyEngine, PolicyEngineError #these two methods are not valid

def node_calculate_risk_signals(state: Dict[str, Any]) -> Dict[str, Any]:
    enriched_case = state["enriched_case"]
    out = calculate_risk_signals_safe(enriched_case)

    if not out["ok"]:
        return {
            "validation_ok": False,
            "validation_errors": [f"Tool1 failed: {out['error']}"],
        }

    return {"risk_signals": out["signals"]}


# -------------------------
# Node 2: Tool2 (behavior)
# -------------------------
def node_extract_behavior_signals(state: Dict[str, Any]) -> Dict[str, Any]:
    enriched_case = state["enriched_case"]
    out = extract_behavior_signals_safe(enriched_case)

    if not out["ok"]:
        return {
            "validation_ok": False,
            "validation_errors": [f"Tool2 failed: {out['error']}"],
        }

    return {"behavior_signals": out["signals"]}



# -------------------------
# Node 3: Policy Engine
# -------------------------
def make_node_policy_engine(policy_engine: PolicyEngine):
    def _node(state: Dict[str, Any]) -> Dict[str, Any]:
        enriched_case = state["enriched_case"]

        try:
            decision = policy_engine.evaluate_enriched_case(enriched_case)
        except Exception as e:
            return {
                "validation_ok": False,
                "validation_errors": [f"PolicyEngine failed: {type(e).__name__}: {e}"],
            }

        # Convert dataclass -> dict (JSON-friendly)
        decision_dict = {
            "case_id": decision.case_id,
            "customer_id": decision.customer_id,
            "policy_version": decision.policy_version,
            "decision": decision.decision,
            "confidence": decision.confidence,
            "reasons": decision.reasons,
            "required_next_actions": decision.required_next_actions,
            "debug_signals": decision.debug_signals,
        }

        return {"policy_decision": decision_dict}

    return _node


# -------------------------
# Node 4: LLM Justification (stub for now)
# Replace tomorrow with real LLM call\
# I ALREADY HAVE THE RULES VIOLATED INSIDE THE 'REASONS' KEY !!!
# PENDING : The customer profile summary( SEP TASK : WE CAN JUST GO OVER ALL CUSTOMERS ONCE AND THEN GET THEIR PROFILE SUMMARY ADDED TO THEIR PROFILE JSON)
# -------------------------
def node_llm_justification(state: Dict[str, Any]) -> Dict[str, Any]:
    # For now: deterministic placeholder text
    pd = state.get("policy_decision", {})
    decision = pd.get("decision", "UNKNOWN")
    reasons = pd.get("reasons", [])

    justification = f"Decision={decision}. Key reasons: {', '.join(reasons) if reasons else 'none'}."
    return {"llm_justification": justification}


# -------------------------
# Node 5: Validation (no hallucinations)
# Ensure LLM did not contradict policy decision
# -------------------------
def node_validate_output(state: Dict[str, Any]) -> Dict[str, Any]:
    errors: List[str] = []

    #To Check if we even have reached the level or has got that state which will be utilised in this node
    if "policy_decision" not in state:
        errors.append("Missing policy_decision in state.")

    # LLM justification is optional; if present, ensure it’s not empty
    j = state.get("llm_justification")
    if j is not None:
        if isinstance(j, dict):
            if not j or not any(v and str(v).strip() for v in j.values()):
                errors.append("llm_justification is present but empty/invalid.")
        elif isinstance(j, str):
            if not j.strip():
                errors.append("llm_justification is present but empty/invalid.")
        else:
            errors.append("llm_justification is present but empty/invalid.")

    ok = len(errors) == 0
    return {"validation_ok": ok, "validation_errors": errors}


