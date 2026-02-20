
from .evaluator import match_conditions
from ..models.alert import  create_alert

def apply_single_transaction_rules(transactions, rules, customers_by_id):
    alerts = []

    for rule in rules:
        for tx in transactions:
            if match_conditions(tx, rule.get("conditions", [])):
                cust = customers_by_id.get(tx["customer_id"])
                if cust:
                    alerts.append(
                        create_alert(
                            cust,
                            rule,
                            [tx["transaction_id"]],
                            tx["timestamp"],
                            tx["timestamp"],
                            tx["timestamp"]
                        )
                    )

    return alerts
