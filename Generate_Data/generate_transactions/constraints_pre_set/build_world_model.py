import json
import uuid
import random
from collections import Counter

# ----------------------------
# CONFIG
# ----------------------------

COUNTRIES = [
    "United States", "United Kingdom", "Canada",
    "Germany", "UAE", "Turkey", "India",
    "Iran", "North Korea", "Syria"
]

# ⭐⭐⭐ FIX #2 — Weighted country sampling
COUNTRY_WEIGHTS = {
    "United States": 0.35,
    "United Kingdom": 0.15,
    "Canada": 0.10,
    "Germany": 0.10,
    "UAE": 0.07,
    "Turkey": 0.08,
    "India": 0.10,
    "Iran": 0.02,
    "North Korea": 0.01,
    "Syria": 0.02
}

HIGH_RISK_COUNTRIES = ["Iran", "North Korea", "Syria"]

NUM_COUNTERPARTIES = 350

COUNTERPARTY_TYPE_WEIGHTS = {
    "merchant": 0.50,
    "business": 0.25,
    "exchange": 0.08,
    "shell_entity": 0.05,
    "msb": 0.07,
    "offshore_entity": 0.05
}

CATEGORY_BY_TYPE = {
    "merchant": ["groceries", "utilities", "airline", "retail", "restaurant"],
    "business": ["consulting", "manufacturing", "import_export", "logistics"],
    "exchange": ["crypto"],
    "shell_entity": ["offshore_services"],
    "msb": ["money_transfer"],
    "offshore_entity": ["offshore_services"]
}

# ⭐⭐⭐ FIX #3 — Restrict exchanges to major financial markets
EXCHANGE_ALLOWED_COUNTRIES = [
    "United States", "United Kingdom", "Canada",
    "Germany", "UAE", "India", "Turkey"
]

# ----------------------------
# 1A — BUILD CORRIDOR MAP
# ----------------------------

def build_corridor_map():
    corridor_map = {
        "high_risk_countries": HIGH_RISK_COUNTRIES,
        "domestic_risk": "low",
        "common_corridors": {
            "United States": ["United Kingdom", "Canada"],
            "Germany": ["Turkey"],
            "UAE": ["India"],
            "United Kingdom": ["United States"],
            "Canada": ["United States"]
        },
        "corridor_weights": {
            "domestic": 0.70,
            "common": 0.20,
            "other_cross_border": 0.09,
            "high_risk": 0.01
        }
    }

    # ⭐⭐⭐ Validation: ensure weights sum to 1
    weights_sum = sum(corridor_map["corridor_weights"].values())
    if abs(weights_sum - 1.0) > 1e-6:
        raise ValueError("Corridor weights do not sum to 1.0")

    with open("corridor_map.json", "w") as f:
        json.dump(corridor_map, f, indent=2)

    print("✅ corridor_map.json created")

# ----------------------------
# 1B — BUILD COUNTERPARTY UNIVERSE
# ----------------------------

def weighted_choice(weight_map):
    return random.choices(
        list(weight_map.keys()),
        weights=list(weight_map.values())
    )[0]

def weighted_country():
    return random.choices(
        list(COUNTRY_WEIGHTS.keys()),
        weights=list(COUNTRY_WEIGHTS.values())
    )[0]

def determine_risk_level(country, cp_type):
    if country in HIGH_RISK_COUNTRIES:
        return "high"

    if cp_type in ["shell_entity", "offshore_entity"]:
        return "high"

    if cp_type == "exchange":
        return random.choice(["medium", "high"])

    if cp_type == "msb":
        return "medium"

    return "low"

def generate_counterparties():
    counterparties = []
    used_names = set()

    for _ in range(NUM_COUNTERPARTIES):
        cp_type = weighted_choice(COUNTERPARTY_TYPE_WEIGHTS)

        # ⭐⭐⭐ FIX #3 — Restrict exchange geography
        if cp_type == "exchange":
            country = random.choice(EXCHANGE_ALLOWED_COUNTRIES)
        else:
            country = weighted_country()

        category = random.choice(CATEGORY_BY_TYPE[cp_type])
        risk_level = determine_risk_level(country, cp_type)

        # Prevent name collision
        while True:
            name = f"{category.capitalize()}_{random.randint(1000,9999)}"
            if name not in used_names:
                used_names.add(name)
                break

        counterparties.append({
            "counterparty_id": str(uuid.uuid4()),
            "name": name,
            "type": cp_type,
            "country": country,
            "category": category,
            "risk_level": risk_level
        })

    with open("counterparties.json", "w") as f:
        json.dump(counterparties, f, indent=2)

    print("✅ counterparties.json created")

    return counterparties

# ----------------------------
# VALIDATION CHECKS
# ----------------------------

def validate_counterparties(counterparties):
    print("\n🔎 VALIDATION SUMMARY")

    type_counts = Counter(cp["type"] for cp in counterparties)
    risk_counts = Counter(cp["risk_level"] for cp in counterparties)
    country_counts = Counter(cp["country"] for cp in counterparties)

    print("\nCounterparty Type Distribution:")
    for t, c in type_counts.items():
        print(f"{t}: {c}")

    print("\nRisk Level Distribution:")
    for r, c in risk_counts.items():
        print(f"{r}: {c}")

    print("\nTop Countries:")
    for country, count in country_counts.most_common(5):
        print(f"{country}: {count}")

    # High-risk country validation
    violations = 0
    for cp in counterparties:
        if cp["country"] in HIGH_RISK_COUNTRIES and cp["risk_level"] != "high":
            violations += 1

    if violations == 0:
        print("\n✅ All high-risk country counterparties correctly marked high risk")
    else:
        print(f"\n❌ Found {violations} high-risk country violations")

# ----------------------------
# MAIN
# ----------------------------

if __name__ == "__main__":
    build_corridor_map()
    cps = generate_counterparties()
    validate_counterparties(cps)
