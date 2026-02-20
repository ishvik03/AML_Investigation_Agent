import json
import uuid
import math
import random
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


# ----------------------------
# LOADERS
# ----------------------------

def load_json(path):
    with open(path, "r") as f:
        return json.load(f)

def load_jsonl(path):
    rows = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows

def write_jsonl(path, rows):
    with open(path, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


# ----------------------------
# DATE HELPERS
# ----------------------------

def month_start(d: datetime) -> datetime:
    return datetime(d.year, d.month, 1)

def month_end(d: datetime) -> datetime:
    next_m = d + relativedelta(months=1)
    return datetime(next_m.year, next_m.month, 1) - timedelta(seconds=1)

def months_between(a: datetime, b: datetime):
    # returns list of month starts between a..b inclusive
    cur = month_start(a)
    end = month_start(b)
    out = []
    while cur < end: #removed the <= to '<' right now !!!
        out.append(cur)
        cur = cur + relativedelta(months=1)
    return out

def clamp(x, lo, hi):
    return max(lo, min(hi, x))


# ----------------------------
# RANDOM HELPERS
# ----------------------------

def weighted_choice(items, weights):
    return random.choices(items, weights=weights, k=1)[0]

def normalize(d):
    s = sum(d.values())
    if s <= 0:
        return d
    return {k: v / s for k, v in d.items()}

def approx_lognormal_from_median(median, sigma):
    """
    If X ~ LogNormal(mu, sigma), median = exp(mu).
    So mu = ln(median).
    """
    mu = math.log(max(median, 1e-6))
    return mu, sigma

def sample_amount_usd(median, sigma, channel_mult=1.0):
    mu, sig = approx_lognormal_from_median(median, sigma)
    x = random.lognormvariate(mu, sig)
    x *= channel_mult
    # keep reasonable bounds
    x = clamp(x, 1.0, 250000.0)
    return round(x, 2)


# ----------------------------
# WORLD MODEL HELPERS
# ----------------------------

def build_counterparty_index(counterparties):
    by_type = defaultdict(list)
    by_country = defaultdict(list)
    for cp in counterparties:
        by_type[cp["type"]].append(cp)
        by_country[cp["country"]].append(cp)
    return by_type, by_country

def choose_counterparty(channel, is_cross_border, counterparty_country, by_type, by_country, pattern_flags):
    """
    Reasonable mapping:
    - card -> merchant
    - ach/p2p -> merchant or business
    - wire -> business/offshore/msb (and sometimes shell for high-risk narratives)
    - crypto -> exchange
    """
    if channel == "crypto":
        pool = by_type.get("exchange", [])
        return random.choice(pool) if pool else None

    if channel == "card":
        pool = by_type.get("merchant", [])
        # if cross-border, prefer merchants in that country if available
        if is_cross_border and counterparty_country in by_country and by_country[counterparty_country]:
            cand = [c for c in by_country[counterparty_country] if c["type"] == "merchant"]
            if cand:
                return random.choice(cand)
        return random.choice(pool) if pool else None

    if channel in ["ach", "p2p"]:
        pool = by_type.get("merchant", []) + by_type.get("business", [])
        if is_cross_border and counterparty_country in by_country and by_country[counterparty_country]:
            cand = [c for c in by_country[counterparty_country] if c["type"] in ["merchant", "business"]]
            if cand:
                return random.choice(cand)
        return random.choice(pool) if pool else None

    # wire
    wire_pool = (
        by_type.get("business", [])
        + by_type.get("msb", [])
        + by_type.get("offshore_entity", [])
        + by_type.get("shell_entity", [])
    )
    if pattern_flags.get("mule_pattern") or pattern_flags.get("structuring"):
        # bias toward higher-risk counterparties for story coherence
        wire_pool = by_type.get("offshore_entity", []) + by_type.get("shell_entity", []) + by_type.get("msb", []) + by_type.get("business", [])
    if is_cross_border and counterparty_country in by_country and by_country[counterparty_country]:
        cand = [c for c in by_country[counterparty_country] if c["type"] in ["business", "msb", "offshore_entity", "shell_entity"]]
        if cand:
            return random.choice(cand)
    return random.choice(wire_pool) if wire_pool else None


# ----------------------------
# CORRIDOR SAMPLING
# ----------------------------

def pick_counterparty_country(customer_country, corridor_map, high_risk_corridor=False):
    hr = set(corridor_map["high_risk_countries"])
    common = corridor_map["common_corridors"].get(customer_country, [])
    weights = corridor_map["corridor_weights"]

    # if explicitly high-risk corridor: force a high-risk country
    if high_risk_corridor:
        return random.choice(list(hr))

    # choose corridor class
    corridor_class = weighted_choice(
        ["domestic", "common", "other_cross_border", "high_risk"],
        [weights["domestic"], weights["common"], weights["other_cross_border"], weights["high_risk"]],
    )

    if corridor_class == "domestic":
        return customer_country

    if corridor_class == "common" and common:
        return random.choice(common)

    if corridor_class == "high_risk":
        return random.choice(list(hr))

    # other cross-border: any non-domestic, non-high-risk preferred
    candidates = [c for c in corridor_map["common_corridors"].keys()] + list(set(corridor_map["high_risk_countries"]))
    # fallback to full list if needed
    # but we’ll avoid domestic
    all_countries = list(set(candidates + common + [customer_country] + list(hr)))
    # we actually want broader, so just use customer_country’s universe:
    # (if you want, replace with global COUNTRIES list)
    pool = [c for c in all_countries if c != customer_country]
    if not pool:
        pool = [customer_country]
    return random.choice(pool)


# ----------------------------
# CHUNK 3: SINGLE-CUSTOMER GENERATION
# ----------------------------

def generate_transactions_for_customer(customer, behavior, txn_config, corridor_map, by_type, by_country, window_start, window_end, single_month_mode=False):
    rr = customer["risk_rating"]
    ct = customer["customer_type"]
    country = customer["country_of_residence"]

    # months active inside window
    onboarding = datetime.strptime(customer["onboarding_date"], "%Y-%m-%d")
    active_start = max(onboarding, window_start)
    if active_start > window_end:
        return []

    if single_month_mode:
        months = [window_start]
    else:
        months = months_between(active_start, window_end)

    base_monthly = behavior["monthly_tx_rate"]

    # small noise guardrails (keep within +/- 15% per month)
    noise_std = txn_config.get("monthly_noise_std", 0.15)

    channel_mix = behavior["channel_mix"]
    cross_border_prob = behavior["cross_border_probability"]
    high_risk_corridor_prob = behavior["high_risk_corridor_probability"]

    median = behavior["amount_distribution"]["median"]
    sigma = behavior["amount_distribution"]["sigma"]

    amt_mult = txn_config["amount_multipliers_by_channel"]
    weekend_bias = txn_config["weekend_activity_bias"][ct]

    pattern_flags = behavior["pattern_flags"]
    struct_threshold = txn_config["structuring_threshold"]
    velocity_thresh = txn_config["velocity_thresholds"]

    out = []
    tx_by_day = defaultdict(list)  # helps for velocity clustering visuals

    for m0 in months:
        m_start = max(m0, active_start)
        m_end = min(month_end(m0), window_end)

        # sample count around monthly rate (Poisson-like)
        noisy_rate = base_monthly * max(0.5, random.gauss(1.0, noise_std))
        monthly_count = max(0, int(random.poisson(lam=noisy_rate)) if hasattr(random, "poisson") else int(random.gauss(noisy_rate, math.sqrt(max(noisy_rate, 1.0)))))
        monthly_count = max(1, monthly_count)  # keep at least 1 if active

        # ---- 🌙 Velocity spike: inject a burst window
        burst_windows = []
        if pattern_flags.get("velocity_spike"):
            # pick 1-2 burst windows per month
            for _ in range(random.randint(1, 2)):
                burst_day = m_start + timedelta(days=random.randint(0, max(0, (m_end - m_start).days)))
                burst_start = datetime(burst_day.year, burst_day.month, burst_day.day, random.randint(9, 18), random.randint(0, 59))
                burst_windows.append((burst_start, burst_start + timedelta(hours=1)))

        for _ in range(monthly_count):
            # timestamp
            if burst_windows and random.random() < 0.25:
                bw = random.choice(burst_windows)
                ts = bw[0] + timedelta(minutes=random.randint(0, 59), seconds=random.randint(0, 59))
            else:
                # choose random day, with weekend bias
                days = max(1, (m_end - m_start).days + 1)
                day_offset = random.randint(0, days - 1)
                day = m_start + timedelta(days=day_offset)

                is_weekend = day.weekday() >= 5
                if (ct == "individual" and is_weekend and random.random() < weekend_bias) or (ct == "business" and (not is_weekend) and random.random() < (1 - weekend_bias)):
                    chosen_day = day
                else:
                    # retry once for bias
                    day_offset = random.randint(0, days - 1)
                    chosen_day = m_start + timedelta(days=day_offset)

                hour = random.randint(8, 22) if ct == "individual" else random.randint(7, 19)
                minute = random.randint(0, 59)
                second = random.randint(0, 59)
                ts = datetime(chosen_day.year, chosen_day.month, chosen_day.day, hour, minute, second)

            # direction
            direction = "debit" if (ct == "individual" and random.random() < 0.65) else random.choice(["debit", "credit"])

            # channel
            channels = list(channel_mix.keys())
            weights = list(channel_mix.values())
            channel = weighted_choice(channels, weights)

            # cross-border
            is_cross_border = (random.random() < cross_border_prob)
            # high-risk corridor conditional on being cross-border
            is_high_risk_corridor = False
            if is_cross_border and (random.random() < high_risk_corridor_prob):
                is_high_risk_corridor = True

            cp_country = country
            if is_cross_border:
                cp_country = pick_counterparty_country(country, corridor_map, high_risk_corridor=is_high_risk_corridor)

            # counterparty
            cp = choose_counterparty(channel, is_cross_border, cp_country, by_type, by_country, pattern_flags)
            if cp is None:
                continue

            # amount base
            amount = sample_amount_usd(median, sigma, channel_mult=amt_mult.get(channel, 1.0))

            # ---- 🌙 Structuring: near-threshold repeated amounts
            if pattern_flags.get("structuring") and direction == "debit" and channel in ["wire", "ach"]:
                if random.random() < 0.35:
                    amount = round(random.uniform(struct_threshold * 0.98, struct_threshold * 0.999), 2)

            # ---- 🌙 Round amounts
            if pattern_flags.get("round_amount_pattern") and random.random() < 0.25:

                rounded = float(int(round(amount / 1000.0)) * 1000)
                amount = rounded if rounded > 0 else amount

            # ---- 🌙 Crypto funnel: force crypto -> exchange and slightly larger amounts
            if pattern_flags.get("crypto_funnel"):
                if random.random() < 0.20:
                    channel = "crypto"
                    cp = random.choice(by_type.get("exchange", [cp]))
                    amount = sample_amount_usd(median * 1.8, sigma, channel_mult=amt_mult.get("crypto", 3.0))

            tx = {
                "transaction_id": str(uuid.uuid4()),
                "customer_id": customer["customer_id"],
                "timestamp": ts.isoformat(),
                "direction": direction,
                "amount_usd": amount,
                "currency": "USD",
                "channel": channel,
                "counterparty_id": cp["counterparty_id"],
                "counterparty_type": cp["type"],
                "counterparty_country": cp_country,
                "counterparty_category": cp["category"],
                "counterparty_risk_level": cp["risk_level"],
                "is_cross_border": is_cross_border,
                "is_high_risk_corridor": is_high_risk_corridor,
                "status": "completed",
            }

            out.append(tx)
            tx_by_day[ts.date()].append(tx)

    # Optional: mule pattern post-injection (simple, visible)
    if pattern_flags.get("mule_pattern") and out:
        # pick one day and inject: many incoming small -> 1 outgoing large
        day = random.choice(list(tx_by_day.keys()))
        small_in = []
        for _ in range(random.randint(6, 12)):
            ts = datetime(day.year, day.month, day.day, random.randint(9, 17), random.randint(0, 59), random.randint(0, 59))
            cp = random.choice(by_type.get("merchant", []) + by_type.get("business", []))
            amt = round(random.uniform(80, 450), 2)
            # amt = round(total_in * random.uniform(0.85, 1.05), 2)
            small_in.append({
                "transaction_id": str(uuid.uuid4()),
                "customer_id": customer["customer_id"],
                "timestamp": ts.isoformat(),
                "direction": "credit",
                "amount_usd": amt,
                "currency": "USD",
                "channel": random.choice(["ach", "p2p"]),
                "counterparty_id": cp["counterparty_id"],
                "counterparty_type": cp["type"],
                "counterparty_country": country,
                "counterparty_category": cp["category"],
                "counterparty_risk_level": cp["risk_level"],
                "is_cross_border": False,
                "is_high_risk_corridor": False,
                "status": "completed",
            })
        out.extend(small_in)

        # one outgoing wire
        ts = datetime(day.year, day.month, day.day, 18, random.randint(0, 59), random.randint(0, 59))
        cp = random.choice(by_type.get("offshore_entity", []) + by_type.get("shell_entity", []) + by_type.get("business", []))
        total_in = sum(x["amount_usd"] for x in small_in)
        out_amt = round(total_in * random.uniform(0.85, 1.05), 2)

        out.append({
            "transaction_id": str(uuid.uuid4()),
            "customer_id": customer["customer_id"],
            "timestamp": ts.isoformat(),
            "direction": "debit",
            "amount_usd": out_amt,
            "currency": "USD",
            "channel": "wire",
            "counterparty_id": cp["counterparty_id"],
            "counterparty_type": cp["type"],
            "counterparty_country": cp["country"],
            "counterparty_category": cp["category"],
            "counterparty_risk_level": cp["risk_level"],
            "is_cross_border": (cp["country"] != country),
            "is_high_risk_corridor": (cp["country"] in set(corridor_map["high_risk_countries"])),
            "status": "completed",
        })

    # sort by time
    out.sort(key=lambda x: x["timestamp"])
    return out


# ----------------------------
# SANDBOX CHECKS
# ----------------------------

def sandbox_report(customer, behavior, txs):
    print("\n" + "="*70)
    print(f"Customer: {customer['customer_id']} | risk={customer['risk_rating']} | type={customer['customer_type']} | status={customer['account_status']}")
    print(f"Expected monthly_tx_rate (behavior): {behavior['monthly_tx_rate']:.2f}")
    print(f"Expected cross_border_prob: {behavior['cross_border_probability']:.3f}")
    print(f"Expected high_risk_corridor_prob: {behavior['high_risk_corridor_probability']:.3f}")
    print(f"Pattern flags: {behavior['pattern_flags']}")

    print(f"\nGenerated transactions: {len(txs)}")

    # channel mix
    ch = Counter(t["channel"] for t in txs)
    total = len(txs) or 1
    print("\nChannel distribution (actual):")
    for k, v in ch.most_common():
        print(f"  {k}: {v} ({v/total:.2%})")

    # cross-border
    cb = sum(1 for t in txs if t["is_cross_border"])
    hr = sum(1 for t in txs if t["is_high_risk_corridor"])
    print(f"\nCross-border rate: {cb}/{total} = {cb/total:.2%}")
    print(f"High-risk corridor rate: {hr}/{total} = {hr/total:.2%}")

    # structuring visibility
    near_10k = [t for t in txs if 9800 <= t["amount_usd"] <= 9999.99]
    if near_10k:
        print("\nStructuring-like examples (amounts 9800–9999): showing up to 10")
        for t in near_10k[:10]:
            print(f"  {t['timestamp']} | {t['channel']} | {t['direction']} | ${t['amount_usd']} | {t['counterparty_type']} ({t['counterparty_country']})")

    # velocity spike visibility: show busiest hour
    # (simple: count per hour bucket)
    by_hour = Counter(t["timestamp"][:13] for t in txs)  # YYYY-MM-DDTHH
    if by_hour:
        top_hour, top_ct = by_hour.most_common(1)[0]
        if top_ct >= 5:
            print(f"\nVelocity spike-like hour: {top_hour}:xx has {top_ct} tx")
            examples = [t for t in txs if t["timestamp"].startswith(top_hour)][0:8]
            for t in examples:
                print(f"  {t['timestamp']} | {t['channel']} | ${t['amount_usd']}")

    # crypto funnel visibility
    crypto = [t for t in txs if t["channel"] == "crypto" or t["counterparty_type"] == "exchange"]
    if crypto:
        print("\nCrypto/exchange examples (up to 10):")
        for t in crypto[:10]:
            print(f"  {t['timestamp']} | {t['channel']} | ${t['amount_usd']} | {t['counterparty_type']} ({t['counterparty_country']})")


# ----------------------------
# MAIN
# ----------------------------

def main():
    customers = load_json("../customer_profiles.json")
    behaviors = load_json("customer_behavior_profiles.json")
    txn_config = load_json("txn_config.json")
    corridor_map = load_json("corridor_map.json")
    counterparties = load_json("counterparties.json")

    # index behavior by customer_id
    behavior_by_id = {b["customer_id"]: b for b in behaviors}

    # counterparty indexes
    by_type, by_country = build_counterparty_index(counterparties)

    today = datetime.today()
    window_months = txn_config["simulation_window_months"]
    window_start = today - relativedelta(months=window_months)
    window_end = today

    # pick 1 per risk class
    buckets = {"Low": [], "Medium": [], "High": []}
    for c in customers:
        if c["risk_rating"] in buckets:
            buckets[c["risk_rating"]].append(c)

    picks = {}
    for rr in ["Low", "Medium", "High"]:
        if not buckets[rr]:
            print(f"⚠️ No customers found for risk={rr}.")
            continue
        picks[rr] = random.choice(buckets[rr])

    for rr, cust in picks.items():
        beh = behavior_by_id[cust["customer_id"]]
        txs = generate_transactions_for_customer(
            cust, beh, txn_config, corridor_map, by_type, by_country, window_start, window_end
        )
        out_path = f"sandbox_transactions_{cust['customer_id']}.jsonl"
        write_jsonl(out_path, txs)
        sandbox_report(cust, beh, txs)
        print(f"\n✅ Wrote: {out_path}")

if __name__ == "__main__":
    main()
