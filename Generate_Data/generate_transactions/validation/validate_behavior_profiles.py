import json
from collections import defaultdict

# ----------------------------
# LOAD
# ----------------------------

def load_json(path):
    with open(path, "r") as f:
        return json.load(f)

profiles = load_json("customer_behavior_profiles.json")

print("Loaded profiles:", len(profiles))

# ----------------------------
# BUCKET STORAGE
# ----------------------------

monthly_rates = defaultdict(list)
medians = defaultdict(list)
cross_border = defaultdict(list)
crypto_share = defaultdict(list)
corridor_prob = defaultdict(list)

violations = 0

# ----------------------------
# SCAN PROFILES
# ----------------------------

for p in profiles:

    rr = p["risk_rating"]
    rate = p["monthly_tx_rate"]
    median = p["amount_distribution"]["median"]
    income = p["annual_income_usd"]
    mix = p["channel_mix"]
    cb = p["cross_border_probability"]
    corridor = p["high_risk_corridor_probability"]

    monthly_rates[rr].append(rate)
    medians[rr].append(median)
    cross_border[rr].append(cb)
    crypto_share[rr].append(mix.get("crypto", 0))
    corridor_prob[rr].append(corridor)

    # ---- Hard sanity checks ----

    if rate > 150 and rr != "High":
        print("⚠️ Non-high risk customer with extreme monthly rate")
        violations += 1

    if median > income * 0.5:
        print("⚠️ Median too large relative to income")
        violations += 1

    if abs(sum(mix.values()) - 1.0) > 1e-6:
        print("⚠️ Channel mix not normalized")
        violations += 1

    if rr == "Low" and mix.get("crypto", 0) > 0.05:
        print("⚠️ Low risk crypto share too high")
        violations += 1

# ----------------------------
# AGGREGATE METRICS
# ----------------------------

def avg(lst):
    return sum(lst) / len(lst) if lst else 0

print("\n--- AVERAGE METRICS BY RISK ---\n")

for risk in ["Low", "Medium", "High"]:
    print(f"{risk}:")
    print("  Avg Monthly Rate:", round(avg(monthly_rates[risk]), 2))
    print("  Avg Median Amount:", round(avg(medians[risk]), 2))
    print("  Avg Cross Border:", round(avg(cross_border[risk]), 4))
    print("  Avg Crypto Share:", round(avg(crypto_share[risk]), 4))
    print("  Avg High Risk Corridor:", round(avg(corridor_prob[risk]), 4))
    print()

# ----------------------------
# MONOTONIC ORDER CHECK
# ----------------------------

print("--- MONOTONIC CHECK ---")

if not (
    avg(monthly_rates["Low"]) <
    avg(monthly_rates["Medium"]) <
    avg(monthly_rates["High"])
):
    print("⚠️ Monthly rate monotonic violation")
    violations += 1

if not (
    avg(medians["Low"]) <
    avg(medians["Medium"]) <
    avg(medians["High"])
):
    print("⚠️ Median monotonic violation")
    violations += 1

if not (
    avg(cross_border["Low"]) <
    avg(cross_border["Medium"]) <
    avg(cross_border["High"])
):
    print("⚠️ Cross-border monotonic violation")
    violations += 1

print("\nTotal violations:", violations)

if violations == 0:
    print("✅ Behavior layer validated successfully")
else:
    print("❌ Review violations before generating transactions")
