from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List
from config.prompts import  AML_SYSTEM_PROMPT

# from openai import OpenAI  # adjust if using different client


# -------------------------
# CONFIG
# -------------------------

# MODEL_NAME = "gpt-4o-mini"  # or your preferred model
# YOU CAN CHANGE THE TEMPERATURE BASED ON THE OUTPUT QUALITIES
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

def build_messages(policy_output: Dict[str, Any], system_prompt: str) -> List[Dict[str, str]]:

    user_payload = {
        "policy_engine_output": policy_output,
        "required_output_schema": {k: "string" for k in JUSTIFICATION_KEYS}
    }

    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": json.dumps(user_payload, indent=2)}
    ]


# -------------------------
# LangGraph Node
# -------------------------

def node_llm_justification(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Real LLM-backed justification node.

    Expects:
        state["policy_output"]

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
        policy_output = state["policy_output"]

        system_prompt = AML_SYSTEM_PROMPT

        client = OpenAI()

        for attempt in range(MAX_RETRIES + 1):

            meta["retries"] = attempt

            messages = build_messages(policy_output, system_prompt)

            response = client.chat.completions.create(
                model=MODEL_NAME,
                messages=messages,
                temperature=TEMPERATURE,
                response_format={"type": "json_object"}  # Enforces JSON
            )

            content = response.choices[0].message.content

            try:
                parsed = json.loads(content)
            except Exception:
                raise JustificationError("LLM did not return valid JSON")

            validated = validate_justification(parsed)

            validate_against_debug_signals(
                validated,
                policy_output["debug_signals"],
                policy_output.get("reasons", []),
            )

            # If all validations pass
            state["llm_justification"] = validated
            meta["ok"] = True
            state["llm_justification_meta"] = meta
            return state

        raise JustificationError("Max retries exceeded")

    except Exception as e:
        meta["error"] = f"{type(e).__name__}: {str(e)}"
        state["llm_justification"] = None
        state["llm_justification_meta"] = meta
        return state