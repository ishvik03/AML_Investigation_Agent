from collections import defaultdict

def index_by_customer(transactions):
    tx_by_customer = defaultdict(list)
    for t in transactions:
        tx_by_customer[t["customer_id"]].append(t)

    for cid in tx_by_customer:
        tx_by_customer[cid].sort(key=lambda x: x["timestamp"])

    return tx_by_customer