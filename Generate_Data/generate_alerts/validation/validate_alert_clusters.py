import json
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from statistics import mean

BASE_DIR = Path(__file__).parent.parent.resolve()
ALERT_PATH = BASE_DIR / "generate_alerts" / "alerts.jsonl"

WINDOW_DAYS = 14


def load_jsonl(path):
    with open(path, "r") as f:
        return [json.loads(line) for line in f if line.strip()]


def cluster_alerts_by_event_time(alert_times):
    """
    Fixed 14-day window clustering.
    A cluster starts at the first alert_event_time
    and includes alerts within 14 days of that first one.
    """

    sorted_times = sorted(alert_times)

    clusters = []
    current_cluster = []
    cluster_start = None

    for ts in sorted_times:

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


def analyze_alert_clusters():

    print("\n================ 14-DAY ALERT CLUSTER ANALYSIS ================\n")

    alerts = load_jsonl(ALERT_PATH)

    alerts_by_customer = defaultdict(list)

    # Group alerts by customer using alert_event_time
    for alert in alerts:
        event_time = datetime.fromisoformat(alert["alert_event_time"])
        alerts_by_customer[alert["customer_id"]].append(event_time)

    clusters_per_customer = {}
    largest_cluster_sizes = []

    zero_span_clusters_total = 0
    zero_span_by_customer = defaultdict(int)

    for cid, alert_times in alerts_by_customer.items():

        clusters = cluster_alerts_by_event_time(alert_times)

        clusters_per_customer[cid] = len(clusters)

        largest_cluster_sizes.append(
            max(len(c) for c in clusters)
        )

        # --- ZERO SPAN CHECK ---
        for cluster in clusters:
            first = cluster[0]
            last = cluster[-1]
            span_days = (last - first).days

            if span_days == 0:
                zero_span_clusters_total += 1
                zero_span_by_customer[cid] += 1

    # ---- Portfolio metrics ----
    cluster_counts = list(clusters_per_customer.values())

    avg_clusters = mean(cluster_counts)
    min_clusters = min(cluster_counts)
    max_clusters = max(cluster_counts)

    customer_min = min(clusters_per_customer, key=clusters_per_customer.get)
    customer_max = max(clusters_per_customer, key=clusters_per_customer.get)

    max_cluster_size_overall = max(largest_cluster_sizes)

    print(f"Total Customers With Alerts: {len(clusters_per_customer)}")
    print(f"\nAverage 14-Day Alert Clusters Per Customer: {avg_clusters:.2f}")
    print(f"Minimum Clusters For A Customer: {min_clusters} (Customer: {customer_min})")
    print(f"Maximum Clusters For A Customer: {max_clusters} (Customer: {customer_max})")
    print(f"Largest Single 14-Day Alert Cluster Size: {max_cluster_size_overall}")

    # -------------------------------
    # ZERO SPAN ANALYSIS
    # -------------------------------

    print("\n------------ ZERO-SPAN CLUSTER ANALYSIS ------------")

    customers_with_zero_span = len(zero_span_by_customer)

    print(f"Total Zero-Span Clusters: {zero_span_clusters_total}")
    print(f"Customers With At Least One Zero-Span Cluster: {customers_with_zero_span}")

    if zero_span_by_customer:

        zero_counts = list(zero_span_by_customer.values())

        avg_zero = mean(zero_counts)
        min_zero = min(zero_counts)
        max_zero = max(zero_counts)

        customer_min_zero = min(zero_span_by_customer, key=zero_span_by_customer.get)
        customer_max_zero = max(zero_span_by_customer, key=zero_span_by_customer.get)

        print(f"Average Zero-Span Clusters Per Customer: {avg_zero:.2f}")
        print(f"Minimum Zero-Span Clusters: {min_zero} (Customer: {customer_min_zero})")
        print(f"Maximum Zero-Span Clusters: {max_zero} (Customer: {customer_max_zero})")

        print("\nUnique Customers With Zero-Span Clusters:")
        for cid in zero_span_by_customer:
            print(f"  {cid}")

    print("\n===============================================================\n")


if __name__ == "__main__":
    analyze_alert_clusters()