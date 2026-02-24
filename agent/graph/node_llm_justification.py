from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List
from config.prompts import  AML_SYSTEM_PROMPT
from openai import OpenAI 

# from openai import OpenAI  # adjust if using different client


# -------------------------
# CONFIG
# -------------------------

MODEL_NAME = "llama-3.2-3b-instruct"  # Match your LM Studio loaded model name
LM_STUDIO_BASE_URL = "http://127.0.0.1:1234"  # LM Studio OpenAI-compatible API
LM_STUDIO_API_KEY = "sk-lm-Wp2H8t22:wWEOMFLZ1V4C5DXjRtQN"  # LM Studio accepts any non-empty key
MAX_RETRIES = 2
TEMPERATURE = 0.2



# -------------------------
# Output Schema
# -------------------------

JUSTIFICATION_KEYS = [
    "case_summary",
    "risk_assessment_summary",
    "policy_alignment_explanation",
    "recommended_action_rationale",
]


class JustificationError(ValueError):
    pass


# -------------------------
# JSON Validation
# -------------------------

def validate_justification(obj: Dict[str, Any]) -> Dict[str, str]:

    if not isinstance(obj, dict):
        raise JustificationError("LLM output is not a JSON object")

    cleaned: Dict[str, str] = {}

    for key in JUSTIFICATION_KEYS:
        if key not in obj:
            raise JustificationError(f"Missing key: {key}")
        if not isinstance(obj[key], str) or not obj[key].strip():
            raise JustificationError(f"Invalid value for key: {key}")
        cleaned[key] = obj[key].strip()

    # Reject unexpected keys
    extra = set(obj.keys()) - set(JUSTIFICATION_KEYS)
    if extra:
        raise JustificationError(f"Unexpected keys returned: {extra}")

    return cleaned


# -------------------------
# Guardrail: Hallucination Check
# -------------------------

def validate_against_debug_signals(
    justification: Dict[str, str],
    debug_signals: Dict[str, Any],
    reasons: List[str],
) -> None:

    full_text = " ".join(justification.values()).lower()

    # Ensure no unknown signal names are introduced
    allowed_terms = list(debug_signals.keys()) + ["aggregated_score", "total_alerts",
                                                  "pattern_present", "high_sev",
                                                  "customer_risk", "crypto_percentage",
                                                  "max_tx_amount", "total_tx_in_window",
                                                  "total_volume_in_window"]

    # Simple safeguard — reject if model invents "new threshold"
    if "threshold of" in full_text and not any(str(debug_signals.get("aggregated_score")) in full_text for _ in [0]):
        raise JustificationError("Potential hallucinated threshold detected")

    # Ensure it references at least one matched reason
    if reasons:
        reason_hit = any(r.lower() in full_text for r in reasons)
        if not reason_hit:
            raise JustificationError("LLM did not reference matched policy reasons")


# -------------------------
# Prompt Builder
# -------------------------

# Only these keys from the policy decision are sent to the LLM.
POLICY_KEYS_FOR_LLM = ("decision", "confidence", "reasons", "required_next_actions", "debug_signals")


def _policy_slice_for_llm(policy_output: Dict[str, Any]) -> Dict[str, Any]:
    """Keep only decision, confidence, reasons, required_next_actions, debug_signals (and all keys inside debug_signals)."""
    out: Dict[str, Any] = {}
    for key in POLICY_KEYS_FOR_LLM:
        if key not in policy_output:
            continue
        val = policy_output[key]
        if key == "debug_signals" and isinstance(val, dict):
            out[key] = dict(val)  # all keys inside debug_signals
        else:
            out[key] = val
    return out


def build_messages(policy_output: Dict[str, Any], system_prompt: str) -> List[Dict[str, str]]:
    policy_slice = _policy_slice_for_llm(policy_output)

    user_content = (
        "The policy decision on this case is as follows:\n\n"
        + json.dumps(policy_slice, indent=2)
        + "\n\nUsing only the above (decision, confidence, reasons, required_next_actions, debug_signals), "
        "generate your structured justification. Return valid JSON with these keys only: "
        + json.dumps(JUSTIFICATION_KEYS)
        + "."
    )

    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_content},
    ]


# -------------------------
# LangGraph Node
# -------------------------

def node_llm_justification(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Real LLM-backed justification node.

    Expects:
        state["policy_decision"]  (written by the policy engine node)

    Writes:
        state["llm_justification"]
        state["llm_justification_meta"]
    """

    meta = {
        "ok": False,
        "error": None,
        "generated_at": datetime.utcnow().isoformat(),
        "model": MODEL_NAME,
        "retries": 0,
    }

    try:
        policy_decision = state.get("policy_decision")
        if not policy_decision or not isinstance(policy_decision, dict):
            meta["error"] = "Missing or invalid policy_decision in state (policy engine may have failed)."
            return {"llm_justification": None, "llm_justification_meta": meta}
        
        if "debug_signals" not in policy_decision:
            meta["error"] = "policy_decision missing 'debug_signals'."
            return {"llm_justification": None, "llm_justification_meta": meta}

        system_prompt = AML_SYSTEM_PROMPT

        client = OpenAI(base_url=LM_STUDIO_BASE_URL, api_key=LM_STUDIO_API_KEY)

        for attempt in range(MAX_RETRIES + 1):

            meta["retries"] = attempt

            messages = build_messages(policy_decision, system_prompt)

            response = client.chat.completions.create(
                model=MODEL_NAME,
                messages=messages,
                temperature=TEMPERATURE,
                max_tokens=1024,
            )

            choice = response.choices[0] if response.choices else None
            if not choice or not choice.message:
                raise JustificationError("LLM response had no choices or message")
            msg = choice.message
            content = getattr(msg, "content", None) or getattr(msg, "text", None) or ""
            if not content or not str(content).strip():
                raise JustificationError(
                    "LLM returned empty content. Try increasing max_tokens or a different model in LM Studio."
                )
            content = str(content).strip()

            try:
                parsed = json.loads(content)
            except Exception as parse_err:
                raise JustificationError(f"LLM did not return valid JSON: {parse_err}")

            validated = validate_justification(parsed)

            validate_against_debug_signals(
                validated,
                policy_decision["debug_signals"],
                policy_decision.get("reasons", []),
            )

            # If all validations pass — return only updates so LangGraph merges correctly
            meta["ok"] = True
            return {"llm_justification": validated, "llm_justification_meta": meta}

        raise JustificationError("Max retries exceeded")

    except Exception as e:
        meta["error"] = f"{type(e).__name__}: {str(e)}"
        return {"llm_justification": None, "llm_justification_meta": meta}