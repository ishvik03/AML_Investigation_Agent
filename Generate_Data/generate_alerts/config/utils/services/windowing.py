from datetime import datetime

def parse_ts(ts: str) -> datetime:
    return datetime.fromisoformat(ts)