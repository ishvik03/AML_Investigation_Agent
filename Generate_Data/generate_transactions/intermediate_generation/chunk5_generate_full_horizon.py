import json
from collections import defaultdict, Counter
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Import your generator + world index builder
from chunk3_sandbox_generate import generate_transactions_for_customer, build_counterparty_index


def load_json(path):
    with open(path, "r") as f:
        return json.load(f)


def main():
    today = datetime.today()

    customers = load_json("../customer_profiles.json")
    behaviors = load_json("customer_behavior_profiles.json")
    txn_config = load_json("txn_config.json")
    corridor_map = load_json("corridor_map.json")
    counterparties = load_json("counterparties.json")

    # Window (full horizon)
    window_months = int(txn_config["simulation_window_months"])
    window_end = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    window_start = window_end - relativedelta(months=window_months)


    out_name = "transactions.jsonl"

    # Indexes
    by_type, by_country = build_counterparty_index(counterparties)
    customers_by_id = {c["customer_id"]: c for c in customers}
    behavior_by_id = {b["customer_id"]: b for b in behaviors}
    counterparties_by_id = {cp["counterparty_id"]: cp for cp in counterparties}

    # Streaming write + light stats
    total_tx = 0
    missing_behavior = 0

    tx_count_by_customer = defaultdict(int)
    tx_by_risk = defaultdict(int)
    cross_border_by_risk = defaultdict(int)
    crypto_by_risk = defaultdict(int)
    wire_by_risk = defaultdict(int)
    hr_corr_by_risk = defaultdict(int)
    amount_sum_by_risk = defaultdict(float)

    out_of_window = 0
    bad_customer_ids = 0
    bad_counterparty_ids = 0
    non_pos_amount = 0

    with open(out_name, "w") as f:
        for c in customers:
            cid = c["customer_id"]
            b = behavior_by_id.get(cid)
            if not b:
                missing_behavior += 1
                continue

            txs = generate_transactions_for_customer(
                customer=c,
                behavior=b,
                txn_config=txn_config,
                corridor_map=corridor_map,
                by_type=by_type,
                by_country=by_country,
                window_start=window_start,
                window_end=window_end,
                # IMPORTANT: do NOT pass single_month_mode here
            )

            # Stream write + validate + aggregate
            rr = c["risk_rating"]

            for t in txs:
                total_tx += 1
                tx_count_by_customer[cid] += 1
                tx_by_risk[rr] += 1

                # Referential integrity
                if t.get("customer_id") not in customers_by_id:
                    bad_customer_ids += 1
                if t.get("counterparty_id") not in counterparties_by_id:
                    bad_counterparty_ids += 1

                # Time window
                ts = datetime.fromisoformat(t["timestamp"])
                if not (window_start <= ts <= window_end):
                    out_of_window += 1

                # Amount sanity
                amt = float(t.get("amount_usd", 0))
                if amt <= 0:
                    non_pos_amount += 1
                amount_sum_by_risk[rr] += amt

                # Risk metrics
                if t.get("is_cross_border"):
                    cross_border_by_risk[rr] += 1
                if t.get("is_high_risk_corridor"):
                    hr_corr_by_risk[rr] += 1
                if t.get("channel") == "crypto":
                    crypto_by_risk[rr] += 1
                if t.get("channel") == "wire":
                    wire_by_risk[rr] += 1

                f.write(json.dumps(t) + "\n")

    print(f"✅ Wrote {total_tx} transactions to {out_name}")
    print(f"Window: {window_start.date().isoformat()} → {window_end.date().isoformat()}")
    if missing_behavior:
        print(f"⚠️ Missing behavior profiles for {missing_behavior} customers")

    # ----------------------------
    # VALIDATION SUMMARY (full horizon)
    # ----------------------------
    failures = []
    warnings = []

    if out_of_window:
        failures.append(f"Time window failed: {out_of_window} tx outside horizon")
    if bad_customer_ids:
        failures.append(f"Referential integrity failed: {bad_customer_ids} tx with unknown customer_id")
    if bad_counterparty_ids:
        failures.append(f"Referential integrity failed: {bad_counterparty_ids} tx with unknown counterparty_id")
    if non_pos_amount:
        failures.append(f"Amount sanity failed: {non_pos_amount} tx with non-positive amount")

    # Per-customer: nobody active should be 0 tx unless restricted (soft check)
    zero_tx = [cid for cid, cnt in tx_count_by_customer.items() if cnt == 0]
    if zero_tx:
        warnings.append(f"{len(zero_tx)} customers had 0 transactions (inspect: restricted? onboarding after window?)")

    # Aggregate metrics by risk
    def safe_rate(numer, denom):
        return 0.0 if denom == 0 else numer / denom

    print("\n--- Aggregate Metrics by Risk (FULL horizon) ---")
    for risk in ["Low", "Medium", "High"]:
        n = tx_by_risk.get(risk, 0)
        if n == 0:
            print(f"{risk}: n=0")
            continue
        avg_amt = amount_sum_by_risk[risk] / n
        xb = safe_rate(cross_border_by_risk[risk], n)
        hr = safe_rate(hr_corr_by_risk[risk], n)
        crypto = safe_rate(crypto_by_risk[risk], n)
        wire = safe_rate(wire_by_risk[risk], n)

        print(
            f"{risk}: n={n}, avg_amt=${avg_amt:.2f}, "
            f"cross_border={xb*100:.2f}%, high_risk_corr={hr*100:.2f}%, "
            f"crypto={crypto*100:.2f}%, wire={wire*100:.2f}%"
        )

    # Monotonicity (soft warnings)
    # Expect: High > Medium > Low for avg amount, cross-border, crypto, wire, high-risk-corr
    # We do NOT strict-fail; just warn if inverted.
    def get_metric(risk):
        n = tx_by_risk.get(risk, 0)
        if n == 0:
            return None
        return {
            "avg_amt": amount_sum_by_risk[risk] / n,
            "xb": safe_rate(cross_border_by_risk[risk], n),
            "crypto": safe_rate(crypto_by_risk[risk], n),
            "wire": safe_rate(wire_by_risk[risk], n),
            "hr": safe_rate(hr_corr_by_risk[risk], n),
        }

    L = get_metric("Low")
    M = get_metric("Medium")
    H = get_metric("High")

    if L and M and H:
        def warn_if_not_increasing(field):
            if not (L[field] < M[field] < H[field]):
                warnings.append(f"Monotonicity weak for {field}: Low={L[field]:.4f}, Medium={M[field]:.4f}, High={H[field]:.4f}")

        warn_if_not_increasing("avg_amt")
        warn_if_not_increasing("xb")
        warn_if_not_increasing("crypto")
        warn_if_not_increasing("wire")
        # For hr corridor, Low/Medium may be ~0; still check but expect noise
        warn_if_not_increasing("hr")

    print("\n================ VALIDATION REPORT ================")
    if failures:
        print("❌ FAILURES (must fix):")
        for x in failures:
            print(" -", x)
    else:
        print("✅ No hard failures.")

    if warnings:
        print("\n⚠️ WARNINGS (investigate):")
        for x in warnings[:40]:
            print(" -", x)
        if len(warnings) > 40:
            print(f" - ... {len(warnings)-40} more warnings")
    else:
        print("\n✅ No warnings.")

    if failures:
        print("\nSTOP: Fix failures before using the dataset.")
    else:
        print("\n✅ Chunk 5 complete. Next step: build alerts/SAR labels + investigation cases on top of this transactions.jsonl")


if __name__ == "__main__":
    main()
