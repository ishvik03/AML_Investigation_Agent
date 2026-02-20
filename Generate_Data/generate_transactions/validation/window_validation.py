import json
from datetime import datetime
from dateutil.relativedelta import relativedelta

with open("../customer_profiles.json", "r") as f:
    customers = json.load(f)

today = datetime.today()
invalid_dates = 0

for c in customers:
    onboarding = datetime.strptime(c["onboarding_date"], "%Y-%m-%d")
    if onboarding > today:
        invalid_dates += 1

if invalid_dates > 0:
    print(f"❌ Found {invalid_dates} customers with future onboarding date")
else:
    print("✅ All onboarding dates valid")



window_start = today - relativedelta(months=12)

invalid_range = 0

for c in customers:
    onboarding = datetime.strptime(c["onboarding_date"], "%Y-%m-%d")
    if onboarding > today:
        invalid_range += 1
    if onboarding > today:
        invalid_range += 1

print("Window Start:", window_start.date())

customers_with_zero_window = 0

for c in customers:
    onboarding = datetime.strptime(c["onboarding_date"], "%Y-%m-%d")
    if onboarding > window_start:
        customers_with_zero_window += 1

print("Customers onboarded within last 12 months:", customers_with_zero_window)


#VERYYY  IMPORTANT TO KEEP IN MIND LATER ON :--
#start_date = max(onboarding_date, window_start)
