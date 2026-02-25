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
LM_STUDIO_BASE_URL = "http://127.0.0.1:1234/v1"  # LM Studio expects /v1/chat/completions
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
# Extract JSON from model output (handles markdown code fences)
# -------------------------

def _extract_json_from_content(content: str) -> str:
    """Strip markdown code fences (```json ... ```) and extract the raw JSON string."""
    s = content.strip()
    # Remove leading ```json or ```
    for prefix in ("```json", "```"):
        if s.lower().startswith(prefix):
            s = s[len(prefix):].lstrip()
            break
    # Remove trailing ```
    if s.rstrip().endswith("```"):
        s = s.rstrip()[:-3].rstrip()
    # Find first { and last } to get the JSON object (handles extra text before/after)
    start = s.find("{")
    if start == -1:
        return s
    depth = 0
    end = -1
    for i in range(start, len(s)):
        if s[i] == "{":
            depth += 1
        elif s[i] == "}":
            depth -= 1
            if depth == 0:
                end = i
                break
    if end != -1:
        return s[start : end + 1]
    return s[start:]


def _normalize_json_for_parsing(s: str) -> str:
    """Replace curly/smart quotes with straight quotes so json.loads can parse LLM output."""
    replacements = (
        ("\u201c", '"'),  # left double quote "
        ("\u201d", '"'),  # right double quote "
        ("\u2018", "'"),  # left single quote '
        ("\u2019", "'"),  # right single quote '
        ("\u201b", "'"),  # single high-reversed-9 quote
    )
    for old, new in replacements:
        s = s.replace(old, new)
    return s


# -------------------------
# JSON Validation
# -------------------------

def _value_to_str(val: Any) -> str:
    """Normalize value to string (LLM may return nested objects instead of plain strings)."""
    if val is None:
        return ""
    if isinstance(val, str):
        return val.strip()
    if isinstance(val, (dict, list)):
        return json.dumps(val, indent=2)
    return str(val)


def validate_justification(obj: Any) -> Dict[str, str]:
    """Validate parsed LLM output (may be dict, list, or other at runtime)."""
    if not isinstance(obj, dict):
        raise JustificationError("LLM output is not a JSON object")

    cleaned: Dict[str, str] = {}

    for key in JUSTIFICATION_KEYS:
        if key not in obj:
            raise JustificationError(f"Missing key: {key}")
        raw = obj[key]
        s = _value_to_str(raw)
        if not s:
            raise JustificationError(f"Invalid or empty value for key: {key}")
        cleaned[key] = s

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
        "## Policy Engine Output (Use Only This Data)\n\n"
        + json.dumps(policy_slice, indent=2)
        + "\n\n## Task\n"
        "Generate the structured justification. Return valid JSON only with keys: "
        "case_summary, risk_assessment_summary, policy_alignment_explanation, recommended_action_rationale. "
        "Do not add markdown fences or commentary."
    )

    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_content},
    ]


# -------------------------
# Debug: capture raw LLM response for troubleshooting
# -------------------------

def _raw_response_to_dict(obj: Any, _depth: int = 0) -> Any:
    """Turn the raw response object into a JSON-serializable dict (for logging)."""
    if _depth > 10:
        return "<max depth>"
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, (list, tuple)):
        return [_raw_response_to_dict(x, _depth + 1) for x in obj[:5]]  # cap list length
    if isinstance(obj, dict):
        return {k: _raw_response_to_dict(v, _depth + 1) for k, v in list(obj.items())[:20]}
    # Pydantic / OpenAI client objects
    if hasattr(obj, "model_dump"):
        return _raw_response_to_dict(getattr(obj, "model_dump")(), _depth + 1)
    if hasattr(obj, "dict"):
        return _raw_response_to_dict(getattr(obj, "dict")(), _depth + 1)
    if hasattr(obj, "__dict__"):
        return _raw_response_to_dict(vars(obj), _depth + 1)
    return str(type(obj).__name__)


def _snapshot_response(response: Any) -> Dict[str, Any]:
    """Build a JSON-serializable snapshot of the API response so we can show it in the UI."""
    out: Dict[str, Any] = {}
    try:
        choices_raw = getattr(response, "choices", None)
        out["choices_type"] = type(choices_raw).__name__
        try:
            choices_list = list(choices_raw) if choices_raw is not None else []
        except Exception as e:
            choices_list = []
            out["choices_list_error"] = str(e)
        out["has_choices"] = len(choices_list) > 0
        out["choices_count"] = len(choices_list)
        out["model"] = getattr(response, "model", None)
        out["id"] = getattr(response, "id", None)
        if getattr(response, "usage", None):
            out["usage"] = str(response.usage)
        choices_preview = []
        for i, c in enumerate(choices_list[:3]):
            msg = getattr(c, "message", None)
            choices_preview.append({
                "index": i,
                "has_message": msg is not None,
                "content_preview": (getattr(msg, "content", None) or getattr(msg, "text", None) or "")[:200] if msg else None,
                "finish_reason": getattr(c, "finish_reason", None),
            })
        out["choices_preview"] = choices_preview
    except Exception as e:
        out["snapshot_error"] = str(e)
        out["raw_type"] = type(response).__name__
    return out


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

            # Full raw response for debugging (where LM Studio puts the text)
            raw_dump = _raw_response_to_dict(response)

            meta["debug_raw_response"] = raw_dump
            meta["debug_response"] = _snapshot_response(response)
            # So you can see it in the backend terminal too
            print("\n[LLM raw response]", json.dumps(raw_dump, indent=2, default=str)[:3000], "\n")

            choice = response.choices[0] if response.choices else None

            if not choice or not choice.message:
                meta["error"] = "LLM response had no choices or message"
                return {"llm_justification": None, "llm_justification_meta": meta}
            
            msg = choice.message
            
            content = getattr(msg, "content", None) or getattr(msg, "text", None) or ""
            if not content or not str(content).strip():
                meta["error"] = "LLM returned empty content. Try increasing max_tokens or a different model in LM Studio."
                return {"llm_justification": None, "llm_justification_meta": meta}
            
            content = str(content).strip()
            json_str = _extract_json_from_content(content)
            json_str = _normalize_json_for_parsing(json_str)

            try:
                parsed = json.loads(json_str)
            except json.JSONDecodeError as parse_err:
                try:
                    from json_repair import repair_json
                    parsed = repair_json(json_str)
                except ImportError:
                    raise JustificationError(f"LLM did not return valid JSON: {parse_err}")
                except Exception as repair_err:
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
        if "debug_response" not in meta:
            meta["debug_response"] = {"exception": str(e), "exception_type": type(e).__name__}
        return {"llm_justification": None, "llm_justification_meta": meta}