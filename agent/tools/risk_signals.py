from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Literal


RiskRating = Literal["Low", "Medium", "High"]
CasePriority = Literal["low", "medium", "high"]






# =========================
# Errors
# =========================
class RiskSignalsValidationError(ValueError):
    """Raised when enriched_case is missing required fields or has invalid values."""


# =========================
# Small validators (strict)
# =========================
def _require(d: Dict[str, Any], key: str, path: str) -> Any:
    if not isinstance(d, dict):
        raise RiskSignalsValidationError(f"Expected dict at {path}, got {type(d).__name__}")
    if key not in d:
        raise RiskSignalsValidationError(f"Missing required key: {path}.{key}")
    return d[key]


def _as_float(x: Any, path: str) -> float:
    # Note: reject bool (bool is subclass of int)
    if isinstance(x, bool):
        raise RiskSignalsValidationError(f"Invalid numeric (bool) at {path}: {x}")
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        try:
            return float(x)
        except Exception:
            pass
    raise RiskSignalsValidationError(f"Expected numeric at {path}, got {repr(x)}")


def _as_int(x: Any, path: str) -> int:
    if isinstance(x, bool):
        raise RiskSignalsValidationError(f"Invalid int (bool) at {path}: {x}")
    if isinstance(x, int):
        return x
    if isinstance(x, float) and x.is_integer():
        return int(x)
    if isinstance(x, str):
        try:
            # accept "12" or "12.0"
            v = float(x)
            if not v.is_integer():
                raise RiskSignalsValidationError(f"Expected int-like numeric at {path}, got {repr(x)}")
            return int(v)
        except RiskSignalsValidationError:
            raise
        except Exception:
            pass
    raise RiskSignalsValidationError(f"Expected int at {path}, got {repr(x)}")


def _as_bool(x: Any, path: str) -> bool:
    if isinstance(x, bool):
        return x
    raise RiskSignalsValidationError(f"Expected bool at {path}, got {repr(x)}")


def _as_str(x: Any, path: str) -> str:
    if isinstance(x, str) and x.strip() != "":
        return x
    raise RiskSignalsValidationError(f"Expected non-empty string at {path}, got {repr(x)}")


def _parse_iso(ts: Any, path: str) -> datetime:
    s = _as_str(ts, path)
    try:
        return datetime.fromisoformat(s)
    except Exception as e:
        raise RiskSignalsValidationError(f"Invalid ISO datetime at {path}: {s}") from e


# =========================
# TOOL 1 (strict)
# =========================
def calculate_risk_signals(enriched_case: Dict[str, Any]) -> Dict[str, Any]:
    """
    TOOL 1 — calculate_risk_signals (STRICT)

    Purpose:
      Deterministically extract "case structural" risk signals for Policy v1.
      This tool does NOT decide, escalate, or summarize with an LLM.

    Input:
      enriched_case must match your enriched-case schema.

    Output:
      {
        "aggregated_score": float,
        "total_alerts": int,
        "pattern_present": bool,
        "has_high_sev_alert": bool,
        "customer_risk": "Low|Medium|High",
        "priority": "low|medium|high"
      }
    """
    if not isinstance(enriched_case, dict):
        raise RiskSignalsValidationError(f"enriched_case must be dict, got {type(enriched_case).__name__}")

    # Top-level IDs (required to keep traceability in logs)
    _ = _require(enriched_case, "case_id", "enriched_case")
    _ = _require(enriched_case, "customer_id", "enriched_case")

    customer_snapshot = _require(enriched_case, "customer_snapshot", "enriched_case")
    case_metadata = _require(enriched_case, "case_metadata", "enriched_case")
    alerts_in_case = _require(enriched_case, "alerts_in_case", "enriched_case")

    # Customer risk rating (enum)
    customer_risk = _as_str(
        _require(customer_snapshot, "risk_rating", "enriched_case.customer_snapshot"),
        "enriched_case.customer_snapshot.risk_rating",
    )
    if customer_risk not in ("Low", "Medium", "High"):
        raise RiskSignalsValidationError(
            f"Invalid risk_rating at enriched_case.customer_snapshot.risk_rating: {customer_risk}"
        )

    # Case metadata signals
    aggregated_score = _as_float(
        _require(case_metadata, "aggregated_score", "enriched_case.case_metadata"),
        "enriched_case.case_metadata.aggregated_score",
    )
    if aggregated_score < 0:
        raise RiskSignalsValidationError(
            f"aggregated_score must be >= 0 at enriched_case.case_metadata.aggregated_score, got {aggregated_score}"
        )

    total_alerts = _as_int(
        _require(case_metadata, "total_alerts", "enriched_case.case_metadata"),
        "enriched_case.case_metadata.total_alerts",
    )
    if total_alerts < 0:
        raise RiskSignalsValidationError(
            f"total_alerts must be >= 0 at enriched_case.case_metadata.total_alerts, got {total_alerts}"
        )

    pattern_present = _as_bool(
        _require(case_metadata, "pattern_present", "enriched_case.case_metadata"),
        "enriched_case.case_metadata.pattern_present",
    )

    priority = _as_str(
        _require(case_metadata, "priority", "enriched_case.case_metadata"),
        "enriched_case.case_metadata.priority",
    ).lower()
    if priority not in ("low", "medium", "high"):
        raise RiskSignalsValidationError(
            f"Invalid priority at enriched_case.case_metadata.priority: {priority}"
        )

    # alerts_in_case consistency + high-sev presence
    if not isinstance(alerts_in_case, list):
        raise RiskSignalsValidationError(
            f"Expected list at enriched_case.alerts_in_case, got {type(alerts_in_case).__name__}"
        )

    # IMPORTANT: if this mismatches, policy comparisons are meaningless → fail fast.
    if len(alerts_in_case) != total_alerts:
        raise RiskSignalsValidationError(
            f"Mismatch for case_id={enriched_case.get('case_id')}: "
            f"case_metadata.total_alerts={total_alerts} but len(alerts_in_case)={len(alerts_in_case)}"
        )

    has_high_sev_alert = False
    for i, alert in enumerate(alerts_in_case):
        if not isinstance(alert, dict):
            raise RiskSignalsValidationError(
                f"Expected dict at enriched_case.alerts_in_case[{i}], got {type(alert).__name__}"
            )
        sev = _as_str(
            _require(alert, "severity", f"enriched_case.alerts_in_case[{i}]"),
            f"enriched_case.alerts_in_case[{i}].severity",
        ).lower()
        if sev not in ("low", "medium", "high"):
            raise RiskSignalsValidationError(
                f"Invalid severity at enriched_case.alerts_in_case[{i}].severity: {sev}"
            )
        if sev == "high":
            has_high_sev_alert = True

    # Optional but valuable sanity check (prevents broken windows ruining clustering/policy)
    tw = case_metadata.get("time_window")
    if isinstance(tw, dict):
        start = tw.get("start")
        end = tw.get("end")
        if start is not None and end is not None:
            start_dt = _parse_iso(start, "enriched_case.case_metadata.time_window.start")
            end_dt = _parse_iso(end, "enriched_case.case_metadata.time_window.end")
            if start_dt > end_dt:
                raise RiskSignalsValidationError(
                    f"time_window.start > time_window.end for case_id={enriched_case.get('case_id')}"
                )

    return {
        "aggregated_score": float(aggregated_score),
        "total_alerts": int(total_alerts),
        "pattern_present": bool(pattern_present),
        "has_high_sev_alert": bool(has_high_sev_alert),
        "customer_risk": customer_risk,  # keep original casing: Low/Medium/High
        "priority": priority,            # normalized: low/medium/high
    }


# =========================
# TOOL 1 (safe wrapper)
# =========================
def calculate_risk_signals_safe(enriched_case: Dict[str, Any]) -> Dict[str, Any]:
    """
    Safe wrapper for agent graphs.

    Never throws.
    Returns:
      {
        "ok": bool,
        "signals": {...} | None,
        "error": str | None
      }

    Why:
      In a LangGraph/LangChain node, you often want to route to an "error handler"
      node instead of crashing the whole run.
    """
    try:
        signals = calculate_risk_signals(enriched_case)
        return {"ok": True, "signals": signals, "error": None}
    except Exception as e:
        return {"ok": False, "signals": None, "error": f"{type(e).__name__}: {e}"}


# =========================
# Optional: quick CLI smoke test
# =========================
def _load_jsonl(path: Path) -> list[dict]:
    rows: list[dict] = []
    with path.open("r") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


if __name__ == "__main__":
    # Example usage:
    # python Generate_Data/agent/tools/risk_signals.py
    #
    # By default, try to load a local enriched_cases.jsonl if present (edit as needed).
    base_dir = Path(__file__).resolve().parents[2]  # .../Generate_Data
    sample_path = base_dir /"Generate_Data" /"enriched_cases" / "enriched_cases.jsonl"

    if not sample_path.exists():
        print(f"[smoke-test] No sample file found at: {sample_path}")
        print("[smoke-test] Exiting.")
        raise SystemExit(0)

    cases = _load_jsonl(sample_path)
    print(f"[smoke-test] Loaded {len(cases)} enriched cases")

    # Run on first 3 cases
    for i, c in enumerate(cases[:3]):
        out = calculate_risk_signals_safe(c)
        cid = c.get("case_id")
        print(f"\nCase {i} case_id={cid}")
        print(json.dumps(out, indent=2))