import json
import random
import uuid
from datetime import datetime
from faker import Faker

fake = Faker()

# ----------------------------
# CONFIG
# ----------------------------

NUM_CUSTOMERS = 300

COUNTRIES = [
    "United States", "United Kingdom", "Canada",
    "Germany", "UAE", "Turkey", "India",
    "Iran", "North Korea", "Syria"
]

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

OCCUPATIONS = {
    "student": (0, 20000),
    "engineer": (60000, 150000),
    "executive": (150000, 500000),
    "unemployed": (0, 5000),
    "small_business_owner": (50000, 300000)
}

BUSINESS_INDUSTRIES = [
    "import_export",
    "crypto_exchange",
    "retail",
    "consulting",
    "shell_company"
]

# ✅ Realistic split (≈80/20)
CUSTOMER_TYPE_WEIGHTS = {
    "individual": 0.80,
    "business": 0.20
}

# ✅ Realistic SAR distribution (HIGHLY IMPORTANT FIX)
# Most customers have 0 SARs. Very few have 3+.
SAR_VALUES = [0, 1, 2, 3, 4, 5]
SAR_WEIGHTS = [0.90, 0.06, 0.025, 0.01, 0.003, 0.002]  # sums to 1.0

# ✅ Optional: Industry → income bands (more realistic)
BUSINESS_INCOME_BANDS = {
    "import_export": (80000, 600000),
    "crypto_exchange": (200000, 1500000),
    "retail": (50000, 400000),
    "consulting": (70000, 800000),
    "shell_company": (50000, 250000)
}

used_identity_keys = set()
used_customer_ids = set()

# ----------------------------
# HELPERS
# ----------------------------

def weighted_choice(weight_map: dict) -> str:
    return random.choices(list(weight_map.keys()), weights=list(weight_map.values()))[0]

# ----------------------------
# VALIDATION
# ----------------------------

def validate_customer(c):
    # Unique ID
    if c["customer_id"] in used_customer_ids:
        return False

    # Cross-field consistency
    if c["customer_type"] == "business":
        if c["first_name"] is not None:
            return False
        if c["last_name"] is not None:
            return False
        if c["date_of_birth"] is not None:
            return False
        if c["occupation"] is not None:
            return False
        if c["industry"] is None:
            return False

    if c["customer_type"] == "individual":
        if c["industry"] is not None:
            return False
        if c["first_name"] is None or c["last_name"] is None or c["date_of_birth"] is None:
            return False

    # Unique identity combo for individuals
    if c["customer_type"] == "individual":
        key = (c["first_name"], c["last_name"], c["date_of_birth"])
        if key in used_identity_keys:
            return False

        dob = datetime.strptime(c["date_of_birth"], "%Y-%m-%d")
        age = (datetime.now() - dob).days // 365
        if age < 18 or age > 90:
            return False

    # High-risk residence cannot be Low
    if c["country_of_residence"] in HIGH_RISK_COUNTRIES and c["risk_rating"] == "Low":
        return False

    # Sanctions must force High + restricted + enhanced KYC
    if c["sanctions_match_flag"]:
        if c["risk_rating"] != "High":
            return False
        if c["account_status"] != "restricted":
            return False
        if c["kyc_level"] != "enhanced":
            return False

    # PEP must be High
    if c["pep_flag"] and c["risk_rating"] != "High":
        return False

    # Many SARs cannot be Low
    if c["prior_sar_count"] >= 3 and c["risk_rating"] == "Low":
        return False

    # High must have enhanced KYC
    if c["risk_rating"] == "High" and c["kyc_level"] != "enhanced":
        return False

    # Income sanity
    if c["annual_income_usd"] < 0:
        return False

    # Business income floor
    if c["customer_type"] == "business" and c["annual_income_usd"] < 50000:
        return False

    return True

# ----------------------------
# GENERATION
# ----------------------------

def generate_customer():
    customer_id = str(uuid.uuid4())

    # Customer type (80/20)
    customer_type = weighted_choice(CUSTOMER_TYPE_WEIGHTS)

    # Weighted residence
    country = weighted_choice(COUNTRY_WEIGHTS)

    # ✅ High-quality realism: nationality correlated with residence
    # 80% same as residence, 20% different
    if random.random() < 0.80:
        nationality = country
    else:
        nationality = weighted_choice(COUNTRY_WEIGHTS)

    # ✅ Fixed SAR realism (not uniform!)
    prior_sar = random.choices(SAR_VALUES, weights=SAR_WEIGHTS)[0]

    # PEP (rare)
    pep_flag = random.random() < 0.03  # slightly lower than 5% for realism

    # ✅ Sanctions (very rare)
    if nationality in HIGH_RISK_COUNTRIES:
        sanctions_match_flag = random.random() < 0.005  # 0.5%
    else:
        sanctions_match_flag = random.random() < 0.001  # 0.1%

    # Identity / business attributes first (so risk uses them)
    occupation = None
    industry = None
    first_name = None
    last_name = None
    dob = None

    if customer_type == "individual":
        first_name = fake.first_name()
        last_name = fake.last_name()
        dob = fake.date_of_birth(minimum_age=18, maximum_age=85).isoformat()

        occupation = random.choice(list(OCCUPATIONS.keys()))
        inc_lo, inc_hi = OCCUPATIONS[occupation]
        annual_income = random.randint(inc_lo, inc_hi)

    else:
        industry = random.choice(BUSINESS_INDUSTRIES)
        inc_lo, inc_hi = BUSINESS_INCOME_BANDS[industry]
        annual_income = random.randint(inc_lo, inc_hi)

    # ----------------------------
    # ✅ CENTRALIZED RISK SCORING
    # ----------------------------

    # ----------------------------
    # ✅ REVISED CENTRALIZED RISK SCORING
    # ----------------------------

    risk_score = 0.0

    # High-risk residence
    if country in HIGH_RISK_COUNTRIES:
        risk_score += 2.5  # increased impact

    # SAR history
    if prior_sar >= 3:
        risk_score += 2.5
    elif prior_sar in [1, 2]:
        risk_score += 1.5

    # PEP
    if pep_flag:
        risk_score += 2.5

    # Industry risk (business)
    if industry in ["crypto_exchange", "shell_company"]:
        risk_score += 1.5

    # Executive exposure (individual)
    if occupation == "executive":
        risk_score += 1.0

    # Slight random baseline risk to avoid too many lows

    risk_score += random.uniform(0.5, 1.5)

    # ----------------------------
    # Revised Thresholds
    # ----------------------------

    if risk_score >= 4.0:
        risk_rating = "High"
    elif risk_score >= 2.0:
        risk_rating = "Medium"
    else:
        risk_rating = "Low"

    # ✅ Sanctions override (always High)
    if sanctions_match_flag:
        risk_rating = "High"

    # KYC
    kyc_level = "enhanced" if risk_rating in ["High", "Medium"] else "basic"

    onboarding_date = fake.date_between(start_date="-5y", end_date="today").isoformat()

    # Account status
    account_status = "active"
    if prior_sar >= 4 or sanctions_match_flag:
        account_status = "restricted"

    # Contact fields (synthetic)
    address = fake.address()
    email = fake.email()
    phone = fake.phone_number()

    return {
        "customer_id": customer_id,
        "customer_type": customer_type,
        "first_name": first_name,
        "last_name": last_name,
        "date_of_birth": dob,
        "country_of_residence": country,
        "nationality": nationality,
        "address": address,
        "email": email,
        "phone": phone,
        "occupation": occupation,
        "industry": industry,
        "onboarding_date": onboarding_date,
        "kyc_level": kyc_level,
        "risk_rating": risk_rating,
        "prior_sar_count": prior_sar,
        "account_status": account_status,
        "annual_income_usd": annual_income,
        "pep_flag": pep_flag,
        "sanctions_match_flag": sanctions_match_flag
    }

# ----------------------------
# MAIN LOOP
# ----------------------------

customers = []

while len(customers) < NUM_CUSTOMERS:
    c = generate_customer()
    if validate_customer(c):
        used_customer_ids.add(c["customer_id"])
        if c["customer_type"] == "individual":
            used_identity_keys.add((c["first_name"], c["last_name"], c["date_of_birth"]))
        customers.append(c)

# ----------------------------
# WRITE FILE
# ----------------------------

with open("customer_profiles.json", "w") as f:
    json.dump(customers, f, indent=2)

print(f"Generated {len(customers)} validated customer profiles.")
