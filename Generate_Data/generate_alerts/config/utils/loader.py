import json

def load_json(path: str):
    with open(path, "r") as f:
        return json.load(f)

def load_jsonl(path: str):
    with open(path, "r") as f:
        return [json.loads(line) for line in f if line.strip()]