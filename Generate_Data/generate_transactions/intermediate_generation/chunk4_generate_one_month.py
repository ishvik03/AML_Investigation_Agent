import json
from collections import Counter, defaultdict
from datetime import datetime
from dateutil.relativedelta import relativedelta

# ✅ Import your real generator + index builder from Chunk 3
# Put your generator function + helper build_counterparty_index in txn_generator.py
from chunk3_sandbox_generate import generate_transactions_for_customer, build_counterparty_index


# ----------------------------
# LOADERS
# ----------------------------
def load_json(path):
    with open(path, "r") as f:
        return json.load(f)


def month_start_end(today: datetime):
    start = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end = start + relativedelta(months=1)
    return start, end


def channel_dist(transactions):
    c = Counter(t["channel"] for t in transactions)
    total = sum(c.values())
    return {k: (v / total if total else 0.0) for k, v in c.items()}


# ----------------------------
# MAIN
# ----------------------------
def main():
    today = datetime.today()
    m_start, m_end = month_start_end(today)
    out_name = f"transactions_month_{m_start.strftime('%Y_%m')}.jsonl"

    customers = load_json("../customer_profiles.json")
    behaviors = load_json("customer_behavior_profiles.json")
    txn_config = load_json("txn_config.json")
    corridor_map = load_json("corridor_map.json")
    counterparties = load_json("counterparties.json")

    # Indexes (used for validation + generator)
    by_type, by_country = build_counterparty_index(counterparties)
    customers_by_id = {c["customer_id"]: c for c in customers}
    behavior_by_id = {b["customer_id"]: b for b in behaviors}
    counterparties_by_id = {cp["counterparty_id"]: cp for cp in counterparties}

    # Generate one month for ALL customers
    all_tx = []
    missing_behavior = 0

    for c in customers:
        b = behavior_by_id.get(c["customer_id"])
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
            window_start=m_start,
            window_end=m_end,
            single_month_mode=True,
        )
        all_tx.extend(txs)

    # Write output
    with open(out_name, "w") as f:
        for t in all_tx:
            f.write(json.dumps(t) + "\n")

    print(f"✅ Wrote {len(all_tx)} transactions to {out_name}")
    print(f"Window: {m_start.isoformat()} → {m_end.isoformat()}")
    if missing_behavior:
        print(f"⚠️ Missing behavior profiles for {missing_behavior} customers")

    # ----------------------------
    # VALIDATIONS
    # ----------------------------
    failures = []
    warnings = []

    # A1: Referential integrity (customer_id)
    bad_customers = sum(1 for t in all_tx if t.get("customer_id") not in customers_by_id)
    if bad_customers:
        failures.append(f"Referential integrity failed: {bad_customers} tx with unknown customer_id")

    # A2: Referential integrity (counterparty_id)
    bad_cps = sum(1 for t in all_tx if t.get("counterparty_id") not in counterparties_by_id)
    if bad_cps:
        failures.append(f"Referential integrity failed: {bad_cps} tx with unknown counterparty_id")

    # A3: Timestamp window correctness
    out_of_window = 0
    for t in all_tx:
        ts = datetime.fromisoformat(t["timestamp"])
        if not (m_start <= ts < m_end):
            out_of_window += 1
    if out_of_window:
        failures.append(f"Time window failed: {out_of_window} tx outside month")

    # A4: Field sanity
    non_pos_amount = sum(1 for t in all_tx if t.get("amount_usd", 0) <= 0)
    if non_pos_amount:
        failures.append(f"Amount sanity failed: {non_pos_amount} tx with non-positive amount")

    bad_corridor_logic = sum(
        1 for t in all_tx
        if t.get("is_high_risk_corridor") and not t.get("is_cross_border")
    )
    if bad_corridor_logic:
        failures.append(f"Corridor sanity failed: {bad_corridor_logic} tx marked high-risk corridor but not cross-border")

    # A5: Unique transaction_id
    ids = [t.get("transaction_id") for t in all_tx if t.get("transaction_id")]
    dup = len(ids) - len(set(ids))
    if dup:
        failures.append(f"Uniqueness failed: {dup} duplicate transaction_id values")

    # --- Group by risk
    tx_by_risk = defaultdict(list)
    for t in all_tx:
        c = customers_by_id.get(t["customer_id"])
        if not c:
            continue
        tx_by_risk[c["risk_rating"]].append(t)

    def metrics_for(risk):
        txs = tx_by_risk.get(risk, [])
        n = len(txs)
        if n == 0:
            return {"n": 0}
        avg_amt = sum(t["amount_usd"] for t in txs) / n
        xb = sum(1 for t in txs if t["is_cross_border"]) / n
        hr = sum(1 for t in txs if t["is_high_risk_corridor"]) / n
        crypto = sum(1 for t in txs if t["channel"] == "crypto") / n
        wire = sum(1 for t in txs if t["channel"] == "wire") / n
        return {"n": n, "avg_amt": avg_amt, "xb": xb, "hr": hr, "crypto": crypto, "wire": wire}

    low = metrics_for("Low")
    med = metrics_for("Medium")
    high = metrics_for("High")

    print("\n--- Aggregate Metrics by Risk (1 month) ---")
    for r, m in [("Low", low), ("Medium", med), ("High", high)]:
        if m["n"] == 0:
            print(f"{r}: n=0")
            continue
        print(
            f"{r}: n={m['n']}, avg_amt=${m['avg_amt']:.2f}, "
            f"cross_border={m['xb']*100:.2f}%, high_risk_corr={m['hr']*100:.2f}%, "
            f"crypto={m['crypto']*100:.2f}%, wire={m['wire']*100:.2f}%"
        )

    # B1: Monotonicity checks with small-sample guard
    if high.get("n", 0) < 100:
        warnings.append(f"High-risk sample small (n={high.get('n',0)}). Monotonicity may be noisy for 1-month slice.")

    def mono_check(field, a, b, c):
        if not (a < b < c):
            warnings.append(f"Monotonicity weak for {field}: Low={a:.4f}, Medium={b:.4f}, High={c:.4f}")

    if low.get("n", 0) and med.get("n", 0) and high.get("n", 0):
        mono_check("avg_amt", low["avg_amt"], med["avg_amt"], high["avg_amt"])
        mono_check("cross_border", low["xb"], med["xb"], high["xb"])
        mono_check("crypto_rate", low["crypto"], med["crypto"], high["crypto"])
        mono_check("wire_rate", low["wire"], med["wire"], high["wire"])
        mono_check("high_risk_corridor", low["hr"], med["hr"], high["hr"])

    # B2: Channel drift baseline — compare against BEHAVIOR profiles, not txn_config template
    # (txn_config channel_mix is only a template; behavior layer modifies it per customer)
    behavior_by_risk = defaultdict(list)
    for b in behaviors:
        cid = b["customer_id"]
        rr = customers_by_id[cid]["risk_rating"]
        behavior_by_risk[rr].append(b["channel_mix"])

    def avg_mix(mixes):
        if not mixes:
            return {}
        keys = set().union(*[m.keys() for m in mixes])
        out = {}
        for k in keys:
            out[k] = sum(m.get(k, 0.0) for m in mixes) / len(mixes)
        return out

    tol = 0.08  # 8 percentage points tolerance per channel
    for risk in ["Low", "Medium", "High"]:
        expected = avg_mix(behavior_by_risk.get(risk, []))
        actual = channel_dist(tx_by_risk.get(risk, []))
        for ch, exp in expected.items():
            act = actual.get(ch, 0.0)
            if abs(act - exp) > tol:
                warnings.append(f"Channel drift ({risk}) {ch}: expected {exp:.2f}, actual {act:.2f}")

    # B3: Restricted accounts should transact less (within same risk)
    by_risk_status = defaultdict(int)
    for t in all_tx:
        c = customers_by_id.get(t["customer_id"])
        if not c:
            continue
        key = (c["risk_rating"], c["account_status"])
        by_risk_status[key] += 1

    for risk in ["Low", "Medium", "High"]:
        a = by_risk_status.get((risk, "active"), 0)
        r = by_risk_status.get((risk, "restricted"), 0)
        if r > 0 and a > 0 and r > a:
            warnings.append(f"Restricted anomaly ({risk}): restricted tx count ({r}) > active ({a}). Check status multiplier / restriction logic.")

    # C1: Exchange geography sanity
    ex_high_risk = 0
    ex_total = 0
    for t in all_tx:
        if t["channel"] == "crypto":
            cp = counterparties_by_id.get(t["counterparty_id"])
            if not cp:
                continue
            if cp["type"] == "exchange":
                ex_total += 1
                if cp["country"] in corridor_map["high_risk_countries"]:
                    ex_high_risk += 1
    if ex_total > 0:
        frac = ex_high_risk / ex_total
        print(f"\nExchange-in-high-risk-country rate: {frac*100:.2f}% ({ex_high_risk}/{ex_total})")
        if frac > 0.05:
            warnings.append("Too many crypto exchanges in high-risk countries. Restrict exchange geography in world model.")


    # D1: Per-customer monthly count sanity
    tx_count_by_customer = defaultdict(int)
    for t in all_tx:
        tx_count_by_customer[t["customer_id"]] += 1

    for cid, count in tx_count_by_customer.items():
        behavior = behavior_by_id.get(cid)
        customer = customers_by_id.get(cid)
        if not behavior or not customer:
            continue

        expected = behavior["monthly_tx_rate"]

        # Explosion detection (3x guardrail)
        if count > expected * 3:
            warnings.append(
                f"Volume anomaly: customer {cid[:8]} ({customer['risk_rating']}) "
                f"generated {count} tx vs expected {expected:.1f}"
            )

        # Extra strict check for Low-risk individuals
        if customer["risk_rating"] == "Low" and customer["customer_type"] == "individual":
            if count > 120:
                warnings.append(
                    f"Low-risk individual high volume: {cid[:8]} has {count} tx in one month"
                )

    # D2: Extreme amount sanity (relative to annual income)
    extreme_amounts = 0

    for t in all_tx:
        cid = t["customer_id"]
        customer = customers_by_id.get(cid)
        if not customer:
            continue

        income = customer.get("annual_income_usd")
        if not income or income <= 0:
            continue

        if t["amount_usd"] > income * 5:
            extreme_amounts += 1
            warnings.append(
                f"Extreme amount: tx {t['transaction_id'][:8]} "
                f"${t['amount_usd']:.2f} > 5x income (${income})"
            )

    if extreme_amounts > 0:
        warnings.append(f"{extreme_amounts} extreme transactions detected.")

    # D3: Crypto funnel correlation validation

    # Baseline crypto rate per risk
    baseline_crypto_by_risk = {}
    for risk in ["Low", "Medium", "High"]:
        txs = tx_by_risk.get(risk, [])
        if not txs:
            continue
        baseline_crypto_by_risk[risk] = sum(
            1 for t in txs if t["channel"] == "crypto"
        ) / len(txs)

    # Per-customer crypto share validation
    customer_tx_map = defaultdict(list)
    for t in all_tx:
        customer_tx_map[t["customer_id"]].append(t)

    for cid, txs in customer_tx_map.items():
        behavior = behavior_by_id.get(cid)
        customer = customers_by_id.get(cid)
        if not behavior or not customer:
            continue

        if behavior["pattern_flags"].get("crypto_funnel"):
            crypto_rate = sum(1 for t in txs if t["channel"] == "crypto") / len(txs)
            baseline = baseline_crypto_by_risk.get(customer["risk_rating"], 0)

            if crypto_rate <= baseline:
                warnings.append(
                    f"Crypto funnel inconsistency: {cid[:8]} "
                    f"crypto_rate={crypto_rate:.3f} baseline={baseline:.3f}"
                )

    # E1: Amount drift vs behavior median
    for cid, txs in customer_tx_map.items():
        behavior = behavior_by_id.get(cid)
        if not behavior:
            continue
        median_expected = behavior["amount_distribution"]["median"]
        avg_actual = sum(t["amount_usd"] for t in txs) / len(txs)

        if avg_actual > median_expected * 4:
            warnings.append(f"Amount drift anomaly for {cid[:8]}")

        if avg_actual > median_expected * 4:
            print(
                f"\nDrift Debug {cid[:8]} | risk={customer['risk_rating']}")
            print(f"  expected median={median_expected}")
            print(f"  actual avg={avg_actual}")
            print(f"  sigma={behavior['amount_distribution']['sigma']}")

    # Print report
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
        print("\n✅ No warnings. This is unusually clean.")

    if failures:
        print("\nSTOP: Fix failures before scaling further.")
    else:
        print("\nNEXT: If warnings are acceptable, proceed to Chunk 5 (full horizon).")


if __name__ == "__main__":
    main()
