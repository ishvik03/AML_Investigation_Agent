from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict


# =========================
# Directory placement (recommended)
# =========================



# =========================
# Errors
# =========================
class BehaviorSignalsValidationError(ValueError):
    """Raised when enriched_case is missing required fields or has invalid values."""


# =========================
# Small validators (strict)
# =========================
def _require(d: Dict[str, Any], key: str, path: str) -> Any:
    if not isinstance(d, dict):
        raise BehaviorSignalsValidationError(f"Expected dict at {path}, got {type(d).__name__}")
    if key not in d:
        raise BehaviorSignalsValidationError(f"Missing required key: {path}.{key}")
    return d[key]


def _as_float(x: Any, path: str) -> float:
    # reject bool (bool is subclass of int)
    if isinstance(x, bool):
        raise BehaviorSignalsValidationError(f"Invalid numeric (bool) at {path}: {x}")
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        try:
            return float(x)
        except Exception:
            pass
    raise BehaviorSignalsValidationError(f"Expected numeric at {path}, got {repr(x)}")


def _as_int(x: Any, path: str) -> int:
    if isinstance(x, bool):
        raise BehaviorSignalsValidationError(f"Invalid int (bool) at {path}: {x}")
    if isinstance(x, int):
        return x
    if isinstance(x, float) and x.is_integer():
        return int(x)
    if isinstance(x, str):
        try:
            v = float(x)
            if not v.is_integer():
                raise BehaviorSignalsValidationError(f"Expected int-like numeric at {path}, got {repr(x)}")
            return int(v)
        except BehaviorSignalsValidationError:
            raise
        except Exception:
            pass
    raise BehaviorSignalsValidationError(f"Expected int at {path}, got {repr(x)}")


def _as_bool(x: Any, path: str) -> bool:
    if isinstance(x, bool):
        return x
    raise BehaviorSignalsValidationError(f"Expected bool at {path}, got {repr(x)}")


def _as_str(x: Any, path: str) -> str:
    if isinstance(x, str) and x.strip() != "":
        return x
    raise BehaviorSignalsValidationError(f"Expected non-empty string at {path}, got {repr(x)}")


def _parse_iso(ts: Any, path: str) -> datetime:
    s = _as_str(ts, path)
    try:
        return datetime.fromisoformat(s)
    except Exception as e:
        raise BehaviorSignalsValidationError(f"Invalid ISO datetime at {path}: {s}") from e


# =========================
# TOOL 2 (strict)
# =========================
def extract_behavior_signals(enriched_case: Dict[str, Any]) -> Dict[str, Any]:
    """
    TOOL 2 — extract_behavior_signals (STRICT)

    Purpose:
      Deterministically extract "behavioral escalation triggers" from enriched_case.
      This tool does NOT decide, escalate, or summarize with an LLM.

    Output Contract:
      {
        "crypto_percentage": float,
        "max_tx_amount": float,
        "total_tx_in_window": int,
        "total_volume_in_window": float,
        "any_threshold_exceeded": bool,
        "any_pattern_detected": bool
      }
    """
    if not isinstance(enriched_case, dict):
        raise BehaviorSignalsValidationError(f"enriched_case must be dict, got {type(enriched_case).__name__}")

    # Keep traceability
    _ = _require(enriched_case, "case_id", "enriched_case")
    _ = _require(enriched_case, "customer_id", "enriched_case")

    behavior_snapshot = _require(enriched_case, "behavior_snapshot", "enriched_case")
    flagged_transactions = _require(enriched_case, "flagged_transactions", "enriched_case")

    # ---- behavior_snapshot numbers ----
    crypto_percentage = _as_float(
        _require(behavior_snapshot, "crypto_percentage", "enriched_case.behavior_snapshot"),
        "enriched_case.behavior_snapshot.crypto_percentage",
    )
    if not (0.0 <= crypto_percentage <= 100.0):
        raise BehaviorSignalsValidationError(
            f"crypto_percentage must be in [0,100] at enriched_case.behavior_snapshot.crypto_percentage, got {crypto_percentage}"
        )

    max_tx_amount = _as_float(
        _require(behavior_snapshot, "max_tx_amount", "enriched_case.behavior_snapshot"),
        "enriched_case.behavior_snapshot.max_tx_amount",
    )
    if max_tx_amount < 0:
        raise BehaviorSignalsValidationError(
            f"max_tx_amount must be >= 0 at enriched_case.behavior_snapshot.max_tx_amount, got {max_tx_amount}"
        )

    total_tx_in_window = _as_int(
        _require(behavior_snapshot, "total_tx_in_window", "enriched_case.behavior_snapshot"),
        "enriched_case.behavior_snapshot.total_tx_in_window",
    )
    if total_tx_in_window < 0:
        raise BehaviorSignalsValidationError(
            f"total_tx_in_window must be >= 0 at enriched_case.behavior_snapshot.total_tx_in_window, got {total_tx_in_window}"
        )

    total_volume_in_window = _as_float(
        _require(behavior_snapshot, "total_volume_in_window", "enriched_case.behavior_snapshot"),
        "enriched_case.behavior_snapshot.total_volume_in_window",
    )
    if total_volume_in_window < 0:
        raise BehaviorSignalsValidationError(
            f"total_volume_in_window must be >= 0 at enriched_case.behavior_snapshot.total_volume_in_window, got {total_volume_in_window}"
        )

    # ---- flagged_transactions rollups ----
    if not isinstance(flagged_transactions, list):
        raise BehaviorSignalsValidationError(
            f"Expected list at enriched_case.flagged_transactions, got {type(flagged_transactions).__name__}"
        )

    # NOTE: allow empty flagged_transactions (robust agent pipeline).
    any_threshold_exceeded = False
    any_pattern_detected = False

    for i, tx in enumerate(flagged_transactions):
        if not isinstance(tx, dict):
            raise BehaviorSignalsValidationError(
                f"Expected dict at enriched_case.flagged_transactions[{i}], got {type(tx).__name__}"
            )

        # (Optional sanity checks: keep schema stable)
        _ = _require(tx, "transaction_id", f"enriched_case.flagged_transactions[{i}]")
        _ = _require(tx, "timestamp", f"enriched_case.flagged_transactions[{i}]")
        _parse_iso(tx["timestamp"], f"enriched_case.flagged_transactions[{i}].timestamp")

        reason = _require(tx, "rule_trigger_reason", f"enriched_case.flagged_transactions[{i}]")
        thr = _as_bool(
            _require(reason, "threshold_exceeded", f"enriched_case.flagged_transactions[{i}].rule_trigger_reason"),
            f"enriched_case.flagged_transactions[{i}].rule_trigger_reason.threshold_exceeded",
        )
        pat = _as_bool(
            _require(reason, "pattern_detected", f"enriched_case.flagged_transactions[{i}].rule_trigger_reason"),
            f"enriched_case.flagged_transactions[{i}].rule_trigger_reason.pattern_detected",
        )

        if thr:
            any_threshold_exceeded = True
        if pat:
            any_pattern_detected = True

        # small early-exit optimization
        if any_threshold_exceeded and any_pattern_detected:
            break

    return {
        "crypto_percentage": float(crypto_percentage),
        "max_tx_amount": float(max_tx_amount),
        "total_tx_in_window": int(total_tx_in_window),
        "total_volume_in_window": float(total_volume_in_window),
        "any_threshold_exceeded": bool(any_threshold_exceeded),
        "any_pattern_detected": bool(any_pattern_detected),
    }


# =========================
# TOOL 2 (safe wrapper)
# =========================
def extract_behavior_signals_safe(enriched_case: Dict[str, Any]) -> Dict[str, Any]:
    """
    Safe wrapper for agent graphs.

    Never throws.
    Returns:
      {
        "ok": bool,
        "signals": {...} | None,
        "error": str | None
      }
    """
    try:
        signals = extract_behavior_signals(enriched_case)
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
    # python Generate_Data/agent/tools/behavior_signals.py
    base_dir = Path(__file__).resolve().parents[2]  # .../Generate_Data
    sample_path = base_dir / "Generate_Data" / "enriched_cases" / "enriched_cases.jsonl"

    if not sample_path.exists():
        print(f"[smoke-test] No sample file found at: {sample_path}")
        print("[smoke-test] Exiting.")
        raise SystemExit(0)

    cases = _load_jsonl(sample_path)
    print(f"[smoke-test] Loaded {len(cases)} enriched cases")

    for i, c in enumerate(cases[:3]):
        out = extract_behavior_signals_safe(c)
        cid = c.get("case_id")
        print(f"\nCase {i} case_id={cid}")
        print(json.dumps(out, indent=2))