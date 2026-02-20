import json
from collections import Counter



import json
from collections import Counter

# Load customer profiles
with open("../customer_profiles.json", "r") as f:
    customers = json.load(f)

print(f"Total customers loaded: {len(customers)}")

# Basic sanity checks
required_fields = [
    "customer_id",
    "customer_type",
    "risk_rating",
    "country_of_residence",
    "onboarding_date"
]

for field in required_fields:
    missing = [c for c in customers if field not in c]
    if missing:
        print(f"❌ Missing field: {field}")
    else:
        print(f"✅ Field present: {field}")

# Distribution checks
risk_counts = Counter(c["risk_rating"] for c in customers)
type_counts = Counter(c["customer_type"] for c in customers)
country_counts = Counter(c["country_of_residence"] for c in customers)

print("\nRisk Distribution:")
print(risk_counts)

print("\nCustomer Type Distribution:")
print(type_counts)

print("\nTop 5 Countries:")
for country, count in country_counts.most_common(5):
    print(country, count)
