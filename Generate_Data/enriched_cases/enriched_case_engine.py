import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean

# ============================================================
# PATHS (adjust only if your folders differ)
# ============================================================
BASE_DIR = Path(__file__).parent.parent.resolve()

CUSTOMER_PATH = BASE_DIR / "customer_profiles.jsonl"
TX_PATH = BASE_DIR / "generate_transactions" / "transactions.jsonl"
ALERT_PATH = BASE_DIR / "generate_alerts" / "alerts.jsonl"
CASE_PATH = BASE_DIR / "generate_cases" / "cases.jsonl"

OUT_PATH = BASE_DIR / "enriched_cases" / "enriched_cases.jsonl"
RUNLOG_PATH = BASE_DIR / "enriched_cases" / "enriched_cases_runlog.json"


# ============================================================
# IO
# ============================================================
def load_json(path: Path):
    with open(path, "r") as f:
        return json.load(f)

def load_jsonl(path: Path):
    rows = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows

def write_jsonl(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


# ============================================================
# TIME HELPERS
# ============================================================
def parse_iso(ts: str) -> datetime:
    # Works with "YYYY-MM-DDTHH:MM:SS" and fractional seconds
    return datetime.fromisoformat(ts)

def safe_parse(ts: str):
    if not ts:
        return None
    try:
        return parse_iso(ts)
    except Exception:
        return None


# ============================================================
# RULE -> REASON FLAGS (deterministic, no LLM)
# ============================================================
def reason_flags_for_rule_ids(rule_ids: set) -> dict:
    return {
        "threshold_exceeded": ("TXN_LARGE_AMOUNT" in rule_ids),
        "velocity_violation": any(r.startswith("AGG_VELOCITY") for r in rule_ids),
        "pattern_detected": any(r.startswith("PATTERN") for r in rule_ids),
    }


# ============================================================
# ENRICHMENT
# ============================================================
def enrich_cases():
    # ----------------------------
    # Load
    # ----------------------------
    customers = load_jsonl(CUSTOMER_PATH)
    transactions = load_jsonl(TX_PATH)
    alerts = load_jsonl(ALERT_PATH)
    cases = load_jsonl(CASE_PATH)

    # ----------------------------
    # Build indexes (source of truth joins)
    # ----------------------------
    customers_by_id = {c.get("customer_id"): c for c in customers if c.get("customer_id")}

    transactions_by_id = {}
    tx_by_customer = defaultdict(list)
    for t in transactions:
        tid = t.get("transaction_id")
        cid = t.get("customer_id")
        ts = t.get("timestamp")
        if not tid or not cid or not ts:
            continue
        transactions_by_id[tid] = t
        tx_by_customer[cid].append(t)

    # Sort transactions per customer once (helps window filtering)
    for cid in tx_by_customer:
        tx_by_customer[cid].sort(key=lambda x: x["timestamp"])

    alerts_by_id = {a.get("alert_id"): a for a in alerts if a.get("alert_id")}

    alerts_by_customer = defaultdict(list)
    alert_ids_by_tx_id = defaultdict(set)  # tx_id -> {alert_id,...}
    for a in alerts:
        cid = a.get("customer_id")
        if cid:
            alerts_by_customer[cid].append(a)

        for tx_id in (a.get("triggered_transaction_ids") or []):
            if a.get("alert_id"):
                alert_ids_by_tx_id[tx_id].add(a["alert_id"])

    # ----------------------------
    # Process each case
    # ----------------------------
    enriched_rows = []
    failures = []
    warnings = []

    for case in cases:
        case_id = case.get("case_id")
        customer_id = case.get("customer_id")
        case_alert_ids = case.get("alerts") or []

        if not case_id or not customer_id:
            failures.append(f"Case missing case_id/customer_id: {case}")
            continue

        if not case_alert_ids:
            failures.append(f"Case {case_id} has empty alerts list.")
            continue

        # ---- Resolve alerts for this case
        case_alert_objs = []
        missing_alert_count = 0
        for aid in case_alert_ids:
            a = alerts_by_id.get(aid)
            if not a:
                missing_alert_count += 1
                continue
            case_alert_objs.append(a)

        if not case_alert_objs:
            failures.append(f"Case {case_id}: none of its alert_ids exist in alerts.jsonl.")
            continue

        if missing_alert_count > 0:
            warnings.append(f"Case {case_id}: {missing_alert_count} missing alert references.")

        # ---- Case time window (use case fields if present, else derive from alerts)
        # Your case schema has first_alert_at / last_alert_at, so use those first.
        case_start = safe_parse(case.get("first_alert_at"))
        case_end = safe_parse(case.get("last_alert_at"))

        # If missing, derive from alerts using window_start/window_end (or alert_event_time fallback)
        if not case_start or not case_end:
            start_candidates = []
            end_candidates = []
            for a in case_alert_objs:
                ws = a.get("window_start") or a.get("alert_event_time")
                we = a.get("window_end") or a.get("alert_event_time")
                wsd = safe_parse(ws)
                wed = safe_parse(we)
                if wsd:
                    start_candidates.append(wsd)
                if wed:
                    end_candidates.append(wed)

            if not case_start and start_candidates:
                case_start = min(start_candidates)
            if not case_end and end_candidates:
                case_end = max(end_candidates)

        if not case_start or not case_end:
            failures.append(f"Case {case_id}: could not compute case time window.")
            continue

        if case_start > case_end:
            failures.append(f"Case {case_id}: case_start > case_end (bad timestamps).")
            continue

        # ---- Customer snapshot (from customer_profiles.json)
        cust = customers_by_id.get(customer_id)
        if not cust:
            warnings.append(f"Case {case_id}: missing customer profile for {customer_id}")
            customer_snapshot = {
                "risk_rating": "unknown",
                "customer_type": "unknown",
                "account_status": "unknown",
                "onboarding_date": "unknown",
                "historical_alert_count": 0,
            }
        else:
            # historical alerts = alerts BEFORE case_start, using alert_event_time
            hist_count = 0
            for a in alerts_by_customer.get(customer_id, []):
                aet = safe_parse(a.get("alert_event_time"))
                if aet and aet < case_start:
                    hist_count += 1

            customer_snapshot = {
                "risk_rating": cust.get("risk_rating"),
                "customer_type": cust.get("customer_type"),
                "account_status": cust.get("account_status"),
                "onboarding_date": cust.get("onboarding_date"),
                "historical_alert_count": hist_count,
            }

        # ---- alerts_in_case + rule bookkeeping
        alerts_in_case = []
        case_rule_ids = set()
        pattern_present = False
        rule_ids_by_tx_in_case = defaultdict(set)  # tx_id -> {rule_id,...}

        for a in case_alert_objs:
            rid = a.get("rule_id")
            if rid:
                case_rule_ids.add(rid)
                if rid.startswith("PATTERN"):
                    pattern_present = True

            trig_ids = a.get("triggered_transaction_ids") or []
            for tx_id in trig_ids:
                if rid:
                    rule_ids_by_tx_in_case[tx_id].add(rid)

            alerts_in_case.append({
                "alert_id": a.get("alert_id"),
                "rule_id": a.get("rule_id"),
                "rule_name": a.get("rule_name"),
                "severity": a.get("severity"),
                "base_score": a.get("base_score", 0),
                "triggered_transaction_ids": trig_ids,
            })

        # ---- flagged_transactions (union of all triggered tx ids in this case)
        flagged_tx_ids = set()
        for a in case_alert_objs:
            for tx_id in (a.get("triggered_transaction_ids") or []):
                flagged_tx_ids.add(tx_id)

        flagged_transactions = []
        missing_tx_count = 0

        # For each flagged tx, link to only alerts that belong to THIS case
        case_alert_id_set = set(case_alert_ids)

        for tx_id in sorted(flagged_tx_ids):
            linked_alert_ids = sorted(list(alert_ids_by_tx_id.get(tx_id, set()) & case_alert_id_set))
            linked_rule_ids = rule_ids_by_tx_in_case.get(tx_id, set())
            trigger_reason = reason_flags_for_rule_ids(linked_rule_ids)

            t = transactions_by_id.get(tx_id)
            if not t:
                missing_tx_count += 1
                flagged_transactions.append({
                    "transaction_id": tx_id,
                    "linked_alert_ids": linked_alert_ids,
                    "timestamp": "unknown",
                    "amount": None,
                    "currency": "unknown",
                    "counterparty_country": "unknown",
                    "is_crypto": False,
                    "rule_trigger_reason": trigger_reason
                })
                continue

            flagged_transactions.append({
                "transaction_id": tx_id,
                "linked_alert_ids": linked_alert_ids,
                "timestamp": t.get("timestamp"),
                "amount": t.get("amount_usd"),
                "currency": t.get("currency"),
                "counterparty_country": t.get("counterparty_country"),
                "is_crypto": (t.get("channel") == "crypto"),
                "rule_trigger_reason": trigger_reason
            })

        if missing_tx_count > 0:
            warnings.append(f"Case {case_id}: {missing_tx_count} flagged tx_ids missing from transactions.jsonl")

        # ---- behavior_snapshot (customer transactions within case window)
        cust_txs = tx_by_customer.get(customer_id, [])
        in_window = []
        for t in cust_txs:
            t_time = safe_parse(t.get("timestamp"))
            if t_time and case_start <= t_time <= case_end:
                in_window.append(t)

        if not in_window:
            behavior_snapshot = {
                "total_tx_in_window": 0,
                "total_volume_in_window": 0.0,
                "avg_tx_amount": 0.0,
                "max_tx_amount": 0.0,
                "crypto_percentage": 0.0
            }
        else:
            amts = [float(t.get("amount_usd", 0) or 0) for t in in_window]
            crypto_ct = sum(1 for t in in_window if t.get("channel") == "crypto")

            behavior_snapshot = {
                "total_tx_in_window": len(in_window),
                "total_volume_in_window": round(sum(amts), 2),
                "avg_tx_amount": round(mean(amts), 2) if amts else 0.0,
                "max_tx_amount": round(max(amts), 2) if amts else 0.0,
                # store percentage 0..100 (if you prefer 0..1, change here)
                "crypto_percentage": round((crypto_ct / len(in_window)) * 100.0, 2)
            }

        # ---- case_metadata (uses YOUR case schema)
        case_metadata = {
            "priority": case.get("case_priority"),
            "aggregated_score": case.get("aggregated_score", 0),
            "total_alerts": case.get("total_alerts", len(case_alert_ids)),
            "rule_types_triggered": sorted(list(case_rule_ids)),
            "pattern_present": pattern_present,
            "time_window": {
                "start": case_start.isoformat(),
                "end": case_end.isoformat()
            }
        }

        enriched_case = {
            "case_id": case_id,
            "customer_id": customer_id,
            "customer_snapshot": customer_snapshot,
            "case_metadata": case_metadata,
            "alerts_in_case": alerts_in_case,
            "flagged_transactions": flagged_transactions,
            "behavior_snapshot": behavior_snapshot
        }

        enriched_rows.append(enriched_case)

    # ----------------------------
    # Write outputs
    # ----------------------------
    write_jsonl(OUT_PATH, enriched_rows)

    RUNLOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(RUNLOG_PATH, "w") as f:
        json.dump({
            "timestamp": datetime.utcnow().isoformat(),
            "cases_in": len(cases),
            "cases_out": len(enriched_rows),
            "failures_count": len(failures),
            "warnings_count": len(warnings),
            "failures_sample": failures[:50],
            "warnings_sample": warnings[:50],
        }, f, indent=2)

    print(f"✅ Wrote enriched cases: {OUT_PATH}")
    print(f"📝 Run log: {RUNLOG_PATH}")
    print(f"Cases in: {len(cases)} | Cases out: {len(enriched_rows)}")
    print(f"Failures: {len(failures)} | Warnings: {len(warnings)}")


if __name__ == "__main__":
    enrich_cases()