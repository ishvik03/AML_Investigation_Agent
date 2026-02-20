import json
from collections import Counter

with open("customer_profiles.json", "r") as f:
    customers = json.load(f)

total = len(customers)

print(f"\nTotal Customers: {total}\n")

# ----------------------------
# Customer Type Distribution
# ----------------------------

type_counts = Counter(c["customer_type"] for c in customers)

print("Customer Type Distribution:")
for k, v in type_counts.items():
    print(f"{k}: {v} ({round(v/total*100,2)}%)")

# ----------------------------
# Risk Rating Distribution
# ----------------------------

risk_counts = Counter(c["risk_rating"] for c in customers)

print("\nRisk Rating Distribution:")
for k, v in risk_counts.items():
    print(f"{k}: {v} ({round(v/total*100,2)}%)")

# ----------------------------
# Country Risk Distribution
# ----------------------------

high_risk_count = sum(
    1 for c in customers if c["country_of_residence"] in ["Iran","North Korea","Syria"]
)

print("\nHigh-Risk Country Exposure:")
print(f"{high_risk_count} ({round(high_risk_count/total*100,2)}%)")

# ----------------------------
# PEP Distribution
# ----------------------------

pep_count = sum(1 for c in customers if c["pep_flag"])

print("\nPEP Distribution:")
print(f"{pep_count} ({round(pep_count/total*100,2)}%)")

# ----------------------------
# Sanctions Distribution
# ----------------------------

sanctions_count = sum(1 for c in customers if c["sanctions_match_flag"])

print("\nSanctions Matches:")
print(f"{sanctions_count} ({round(sanctions_count/total*100,2)}%)")

# ----------------------------
# KYC Distribution
# ----------------------------

kyc_counts = Counter(c["kyc_level"] for c in customers)

print("\nKYC Level Distribution:")
for k, v in kyc_counts.items():
    print(f"{k}: {v} ({round(v/total*100,2)}%)")

# ----------------------------
# SAR Distribution
# ----------------------------

sar_counts = Counter(c["prior_sar_count"] for c in customers)

print("\nSAR Count Distribution:")
for k, v in sorted(sar_counts.items()):
    print(f"{k}: {v} ({round(v/total*100,2)}%)")

# ----------------------------
# Business Industry Breakdown
# ----------------------------

businesses = [c for c in customers if c["customer_type"] == "business"]

industry_counts = Counter(c["industry"] for c in businesses)

print("\nBusiness Industry Distribution:")
for k, v in industry_counts.items():
    print(f"{k}: {v} ({round(v/len(businesses)*100,2)}%)")
