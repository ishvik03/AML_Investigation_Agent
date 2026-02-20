import json
import random
import math

# ----------------------------
# LOAD INPUTS
# ----------------------------

def load_json(path):
    with open(path, "r") as f:
        return json.load(f)

customers = load_json(path= "../customer_profiles.json")
txn_config = load_json("txn_config.json")

base_rate = txn_config["base_monthly_rate_by_risk"]
type_mult = txn_config["type_multiplier"]
status_mult = txn_config["status_multiplier"]
channel_template = txn_config["channel_mix"]

# ----------------------------
# HELPER
# ----------------------------

def normalize_dict(d):
    s = sum(d.values())
    return {k: v/s for k, v in d.items()}

# 🌙 Clamp helper to prevent explosions
def clamp(val, low, high):
    return max(low, min(val, high))

# ----------------------------
# BUILD BEHAVIOR
# ----------------------------

behavior_profiles = []

for c in customers:

    rr = c["risk_rating"]
    ct = c["customer_type"]
    status = c["account_status"]
    income = max(c["annual_income_usd"], 1)  # 🌙 guard against zero
    sar = c["prior_sar_count"]
    pep = c["pep_flag"]
    sanctions = c["sanctions_match_flag"]

    # ---- Monthly rate
    monthly_tx_rate = base_rate[rr] * type_mult[ct] * status_mult[status]

    # 🌙 Add bounded stochastic noise (safer band)
    monthly_tx_rate *= random.uniform(0.92, 1.08)

    # 🌙 Prevent absurd transaction rates
    if status == "restricted":
        monthly_tx_rate = clamp(monthly_tx_rate, 1, 45)
    elif rr != "High":
        monthly_tx_rate = clamp(monthly_tx_rate, 1, 120)

    # ---- Amount distribution
    base_median = max(income / 120, 20)

    risk_mult = {"Low": 1.0, "Medium": 1.5, "High": 2.5}
    business_mult = 1.8 if ct == "business" else 1.0

    median = base_median * risk_mult[rr] * business_mult

    # 🌙 Prevent median from exceeding logical portion of annual income
    median = min(median, income * 0.4)

    sigma_map = {"Low": 0.6, "Medium": 0.9, "High": 1.3}
    sigma = sigma_map[rr]

    # ---- Channel mix
    mix = channel_template[rr].copy()

    if ct == "business":
        mix["wire"] += 0.05
        mix["card"] -= 0.05

    if rr == "High":
        mix["crypto"] += 0.05
        mix["card"] -= 0.05

    mix = normalize_dict(mix)

    # 🌙 Validate normalization (soft correction)
    if abs(sum(mix.values()) - 1.0) > 1e-6:
        mix = normalize_dict(mix)

    # ---- Cross border probability
    cross_border_map = {"Low": 0.03, "Medium": 0.12, "High": 0.35}
    cross_border_prob = cross_border_map[rr]

    if ct == "business":
        cross_border_prob *= 1.2

    if pep:
        cross_border_prob += 0.05

    # 🌙 Cap extreme probabilities
    cross_border_prob = clamp(cross_border_prob, 0.0, 0.8)

    # ---- High risk corridor probability
    corridor_map = {"Low": 0.001, "Medium": 0.01, "High": 0.08}
    high_risk_corridor_prob = corridor_map[rr]

    if sanctions:
        high_risk_corridor_prob = max(high_risk_corridor_prob, 0.15)

    # 🌙 Enforce monotonic ordering safeguard
    if rr == "Low":
        high_risk_corridor_prob = clamp(high_risk_corridor_prob, 0, 0.01)

    # ---- Pattern probabilities (fresh copy per customer 🌙)
    pattern_template = {
        "Low": {"structuring": 0.002, "velocity_spike": 0.001,
                "crypto_funnel": 0.0005, "mule_pattern": 0.0005,
                "round_amount_pattern": 0.005},

        "Medium": {"structuring": 0.01, "velocity_spike": 0.008,
                   "crypto_funnel": 0.005, "mule_pattern": 0.004,
                   "round_amount_pattern": 0.01},

        "High": {"structuring": 0.05, "velocity_spike": 0.04,
                 "crypto_funnel": 0.03, "mule_pattern": 0.02,
                 "round_amount_pattern": 0.02}
    }

    probs = pattern_template[rr].copy()

    if sar >= 3:
        probs = {k: min(v * 1.5, 0.2) for k, v in probs.items()}  # 🌙 capped

    # 🌙 Sanctions increase certain risks
    if sanctions:
        probs["structuring"] = min(probs["structuring"] * 2, 0.25)
        probs["mule_pattern"] = min(probs["mule_pattern"] * 2, 0.25)

    pattern_flags = {
        k: random.random() < v for k, v in probs.items()
    }

    behavior_profiles.append({
        "customer_id": c["customer_id"],
        "risk_rating": rr,  # 🌙 added for validation layer
        "customer_type": ct,  # 🌙 added for validation layer
        "account_status": status,  # 🌙 added
        "annual_income_usd": income,  # 🌙 added
        "monthly_tx_rate": monthly_tx_rate,
        "amount_distribution": {
            "median": median,
            "sigma": sigma
        },
        "channel_mix": mix,
        "cross_border_probability": cross_border_prob,
        "high_risk_corridor_probability": high_risk_corridor_prob,
        "pattern_probabilities": probs,  # 🌙 store raw probs for validation
        "pattern_flags": pattern_flags
    })

# ----------------------------
# SAVE
# ----------------------------

with open("customer_behavior_profiles.json", "w") as f:
    json.dump(behavior_profiles, f, indent=2)

print(f"Generated {len(behavior_profiles)} behavior profiles.")
