import json

input_file = "customer_profiles.json"
output_file = "customer_profiles.jsonl"

with open(input_file, "r") as f:
    data = json.load(f)

with open(output_file, "w") as f:
    for item in data:
        f.write(json.dumps(item) + "\n")

print("Converted to JSONL successfully.")
