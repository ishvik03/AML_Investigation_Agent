from datetime import timedelta
from ..models.alert import create_alert
from .evaluator import match_conditions, eval_op
from ..config.utils.services.windowing import parse_ts

def window_to_timedelta(window):
    unit = window["unit"]
    v = window["value"]
    if unit == "hours":
        return timedelta(hours=v)
    if unit == "days":
        return timedelta(days=v)
    raise ValueError("Unsupported window unit")

def apply_aggregation_rules(tx_by_customer, rules, customers_by_id):
    alerts = []

    for rule in rules:
        w = window_to_timedelta(rule["window"])
        metric = rule["metric"]
        filt = rule.get("filter", [])

        for cid, txs in tx_by_customer.items():
            cust = customers_by_id.get(cid)
            if not cust:
                continue

            left = 0
            for right in range(len(txs)):
                t_right = parse_ts(txs[right]["timestamp"])

                while left <= right and (t_right - parse_ts(txs[left]["timestamp"])) > w:
                    left += 1

                window_slice = txs[left:right+1]

                if filt:
                    window_slice = [t for t in window_slice if match_conditions(t, filt)]

                if metric["type"] == "count":
                    count_val = len(window_slice)
                    if eval_op(count_val, metric["op"], metric["value"]):
                        triggered_ids = [t["transaction_id"] for t in window_slice]
                        if triggered_ids:
                            alerts.append(
                                create_alert(
                                    cust,
                                    rule,
                                    triggered_ids,
                                    txs[right]["timestamp"],
                                    txs[left]["timestamp"],
                                    txs[right]["timestamp"]
                                )
                            )
                            left = right

    return alerts
