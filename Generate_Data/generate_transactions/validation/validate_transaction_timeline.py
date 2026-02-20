import json
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from statistics import mean

BASE_DIR = Path(__file__).parent.parent.resolve()
TX_PATH = BASE_DIR / "generate_transactions" / "transactions.jsonl"

WINDOW_DAYS = 14


def load_jsonl(path):
    with open(path, "r") as f:
        return [json.loads(line) for line in f if line.strip()]


def cluster_transactions_by_time(timestamps):
    """
    Fixed 14-day window clustering.
    A cluster starts at first transaction,
    and includes all tx within 14 days of that first one.
    """

    timestamps_sorted = sorted(timestamps)

    clusters = []
    current_cluster = []
    cluster_start = None

    for ts in timestamps_sorted:
        if not current_cluster:
            current_cluster = [ts]
            cluster_start = ts
            continue

        if ts - cluster_start <= timedelta(days=WINDOW_DAYS):
            current_cluster.append(ts)
        else:
            clusters.append(current_cluster)
            current_cluster = [ts]
            cluster_start = ts

    if current_cluster:
        clusters.append(current_cluster)

    return clusters


def analyze_transaction_clusters():

    print("\n================ 14-DAY TRANSACTION CLUSTER ANALYSIS ================\n")

    transactions = load_jsonl(TX_PATH)

    tx_by_customer = defaultdict(list)

    # Group transactions by customer
    for tx in transactions:
        ts = datetime.fromisoformat(tx["timestamp"])
        tx_by_customer[tx["customer_id"]].append(ts)

    clusters_per_customer = {}

    for cid, timestamps in tx_by_customer.items():
        clusters = cluster_transactions_by_time(timestamps)
        clusters_per_customer[cid] = len(clusters)

    # Portfolio metrics
    cluster_counts = list(clusters_per_customer.values())

    avg_clusters = mean(cluster_counts)
    min_clusters = min(cluster_counts)
    max_clusters = max(cluster_counts)

    # Identify which customers
    customer_min = min(clusters_per_customer, key=clusters_per_customer.get)
    customer_max = max(clusters_per_customer, key=clusters_per_customer.get)

    print(f"Total Customers: {len(clusters_per_customer)}")
    print(f"\nAverage 14-Day Clusters Per Customer: {avg_clusters:.2f}")
    print(f"Minimum Clusters For A Customer: {min_clusters} (Customer: {customer_min})")
    print(f"Maximum Clusters For A Customer: {max_clusters} (Customer: {customer_max})")

    print("\n=============================================================\n")


if __name__ == "__main__":
    analyze_transaction_clusters()