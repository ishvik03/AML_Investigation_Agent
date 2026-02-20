import json
import math
import random
from datetime import datetime
from dateutil.relativedelta import relativedelta

def month_diff(a: datetime, b: datetime) -> int:
    """Whole-month difference between a and b, assuming a <= b."""
    return (b.year - a.year) * 12 + (b.month - a.month)

def load_customers(path= "../customer_profiles.json"):
    with open(path, "r") as f:
        return json.load(f)

def calibrate_rates(customers, txn_config):
    today = datetime.today()
    window_months = txn_config["simulation_window_months"]
    window_start = today - relativedelta(months=window_months)
    window_end = today

    base_rate = txn_config["base_monthly_rate_by_risk"]
    type_mult = txn_config["type_multiplier"]
    status_mult = txn_config["status_multiplier"]
    target_total = txn_config["target_total_transactions"]

    raw_expected_total = 0.0
    customer_meta = []

    for c in customers:
        onboarding = datetime.strptime(c["onboarding_date"], "%Y-%m-%d")
        active_start = max(onboarding, window_start)

        if active_start > window_end:
            months_active = 0
        else:
            months_active = max(1, month_diff(active_start, window_end))  # at least 1 if active

        rr = c["risk_rating"]
        ct = c["customer_type"]
        status = c["account_status"]

        raw_monthly = base_rate[rr] * type_mult[ct] * status_mult[status]
        raw_expected_total += raw_monthly * months_active

        customer_meta.append({
            "customer_id": c["customer_id"],
            "risk_rating": rr,
            "customer_type": ct,
            "account_status": status,
            "months_active": months_active,
            "raw_monthly_rate": raw_monthly
        })

    scale_factor = target_total / raw_expected_total if raw_expected_total > 0 else 1.0

    for m in customer_meta:
        m["final_monthly_rate"] = m["raw_monthly_rate"] * scale_factor

    return {
        "window_start": window_start.strftime("%Y-%m-%d"),
        "window_end": window_end.strftime("%Y-%m-%d"),
        "raw_expected_total": raw_expected_total,
        "scale_factor": scale_factor,
        "customer_rates": customer_meta
    }

# ---- Example usage ----
def load_config(path="txn_config.json"):
    with open(path, "r") as f:
        return json.load(f)

txn_config = load_config("txn_config.json")

customers = load_customers()

cal = calibrate_rates(customers, txn_config)

print("Window:", cal["window_start"], "→", cal["window_end"])
print("Raw expected total:", round(cal["raw_expected_total"], 2))
print("Scale factor:", round(cal["scale_factor"], 4))

# Optional: quick sanity check of average final rate
avg_final = sum(x["final_monthly_rate"] for x in cal["customer_rates"]) / len(cal["customer_rates"])
print("Avg final monthly rate:", round(avg_final, 2))
