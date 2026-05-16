"""
FinanceFlow Bank — Synthetic Dataset Generator
seed=42 | Period: Jan/2023 – Dec/2024
"""

import uuid
import random
import hashlib
from datetime import datetime, timedelta, date

import numpy as np
import pandas as pd

SEED = 42
rng = np.random.default_rng(SEED)
random.seed(SEED)

OUT_DIR = "gen/data"

# ─── helpers ────────────────────────────────────────────────────────────────

def gen_uuid(n):
    lo = rng.integers(0, 2**63, size=n, dtype=np.uint64)
    hi = rng.integers(0, 2**63, size=n, dtype=np.uint64)
    return [str(uuid.UUID(int=(int(hi[i]) << 64) | int(lo[i]))) for i in range(n)]

def rand_date(start: date, end: date, size: int) -> np.ndarray:
    delta = (end - start).days
    offsets = rng.integers(0, delta + 1, size)
    return np.array([start + timedelta(days=int(o)) for o in offsets])

def rand_datetime(start: date, end: date, size: int) -> np.ndarray:
    delta = int((datetime.combine(end, datetime.max.time()) -
                 datetime.combine(start, datetime.min.time())).total_seconds())
    offsets = rng.integers(0, delta, size)
    base = datetime.combine(start, datetime.min.time())
    return np.array([base + timedelta(seconds=int(o)) for o in offsets])

BRAZILIAN_CITIES = [
    ("SP", "São Paulo"), ("SP", "Campinas"), ("SP", "Santos"), ("SP", "Ribeirão Preto"),
    ("RJ", "Rio de Janeiro"), ("RJ", "Niterói"), ("RJ", "Campos dos Goytacazes"),
    ("MG", "Belo Horizonte"), ("MG", "Uberlândia"), ("MG", "Juiz de Fora"),
    ("RS", "Porto Alegre"), ("RS", "Caxias do Sul"), ("RS", "Pelotas"),
    ("PR", "Curitiba"), ("PR", "Londrina"), ("PR", "Maringá"),
    ("BA", "Salvador"), ("BA", "Feira de Santana"),
    ("CE", "Fortaleza"), ("CE", "Caucaia"),
    ("PE", "Recife"), ("PE", "Caruaru"),
    ("GO", "Goiânia"), ("GO", "Aparecida de Goiânia"),
    ("AM", "Manaus"), ("PA", "Belém"), ("SC", "Florianópolis"),
    ("MT", "Cuiabá"), ("MS", "Campo Grande"), ("ES", "Vitória"),
]

START = date(2023, 1, 1)
END = date(2024, 12, 31)

# ─── TABELA 1: customers ────────────────────────────────────────────────────

print("Gerando customers...")

N_CUST = 50_000
cust_ids = gen_uuid(N_CUST)

age_groups = ["18-25", "26-35", "36-45", "46-60", "60+"]
age_weights = [0.18, 0.30, 0.25, 0.18, 0.09]

channels = ["organic", "paid_search", "social_media", "partner", "referral"]
chan_w    = [0.30, 0.25, 0.20, 0.15, 0.10]

segments = ["starter", "regular", "premium", "vip"]
seg_w    = [0.40, 0.35, 0.18, 0.07]

city_idx = rng.integers(0, len(BRAZILIAN_CITIES), N_CUST)
states = [BRAZILIAN_CITIES[i][0] for i in city_idx]
cities = [BRAZILIAN_CITIES[i][1] for i in city_idx]

incomes_base = rng.lognormal(mean=8.5, sigma=0.7, size=N_CUST).round(2)
# inject 1% with income = 0
zero_income_mask = rng.random(N_CUST) < 0.01
incomes_base[zero_income_mask] = 0.0

signup_dates = rand_date(START, END, N_CUST)
birth_base = [1963, 1980, 2001]  # rough anchor years

ag_col = rng.choice(age_groups, size=N_CUST, p=age_weights)

def age_group_to_birthdate(ag, size):
    ranges = {
        "18-25": (1999, 2006),
        "26-35": (1989, 1998),
        "36-45": (1979, 1988),
        "46-60": (1964, 1978),
        "60+":   (1940, 1963),
    }
    lo, hi = ranges[ag]
    yr = random.randint(lo, hi)
    mo = random.randint(1, 12)
    dy = random.randint(1, 28)
    return date(yr, mo, dy)

birth_dates = [age_group_to_birthdate(g, 1) for g in ag_col]

cpf_hashes = [hashlib.sha256(str(i).encode()).hexdigest()[:16] for i in range(N_CUST)]

products_count = rng.integers(1, 5, N_CUST)  # 1-4

customers = pd.DataFrame({
    "customer_id":        cust_ids,
    "name":               [f"Cliente_{i:06d}" for i in range(N_CUST)],
    "cpf_hash":           cpf_hashes,
    "birth_date":         birth_dates,
    "age_group":          ag_col,
    "acquisition_channel": rng.choice(channels, size=N_CUST, p=chan_w),
    "state":              states,
    "city":               cities,
    "income_declared":    incomes_base,
    "signup_date":        signup_dates,
    "customer_segment":   rng.choice(segments, size=N_CUST, p=seg_w),
    "is_active":          rng.random(N_CUST) > 0.05,
    "products_count":     products_count,
})

customers.to_parquet(f"{OUT_DIR}/customers.parquet", index=False)
print(f"  customers: {len(customers):,} registros")

# ─── TABELA 2: contracts ────────────────────────────────────────────────────

print("Gerando contracts...")

N_CONT = 120_000
cont_ids = gen_uuid(N_CONT)

products = ["personal_loan", "vehicle_financing", "credit_card", "payroll_loan"]
prod_w   = [0.40, 0.30, 0.20, 0.10]

statuses = ["active", "settled", "defaulted", "renegotiated"]

# base default rates per product (before modifiers)
default_rate_by_product = {
    "personal_loan":     0.14,   # worst
    "vehicle_financing": 0.084,  # 40% lower than personal
    "credit_card":       0.11,
    "payroll_loan":      0.06,
}

collaterals = ["none", "vehicle", "property"]
coll_by_product = {
    "personal_loan":     [0.90, 0.06, 0.04],
    "vehicle_financing": [0.05, 0.90, 0.05],
    "credit_card":       [0.99, 0.005, 0.005],
    "payroll_loan":      [0.95, 0.03, 0.02],
}

cust_sample = rng.choice(cust_ids, size=N_CONT)
prod_col    = rng.choice(products, size=N_CONT, p=prod_w)

contract_dates = rand_date(START, END, N_CONT)
terms_months   = rng.integers(6, 61, N_CONT)  # 6-60 months

maturity_dates = [
    contract_dates[i] + timedelta(days=int(terms_months[i] * 30.5))
    for i in range(N_CONT)
]

principal = np.where(
    pd.Series(prod_col) == "personal_loan",
    rng.lognormal(9.5, 0.6, N_CONT),
    np.where(
        pd.Series(prod_col) == "vehicle_financing",
        rng.lognormal(10.5, 0.5, N_CONT),
        np.where(
            pd.Series(prod_col) == "credit_card",
            rng.lognormal(8.5, 0.5, N_CONT),
            rng.lognormal(9.0, 0.5, N_CONT),
        ),
    ),
).round(2)

interest_rates = np.where(
    pd.Series(prod_col) == "personal_loan",
    rng.uniform(0.025, 0.055, N_CONT),
    np.where(
        pd.Series(prod_col) == "vehicle_financing",
        rng.uniform(0.015, 0.030, N_CONT),
        np.where(
            pd.Series(prod_col) == "credit_card",
            rng.uniform(0.070, 0.120, N_CONT),
            rng.uniform(0.015, 0.025, N_CONT),
        ),
    ),
).round(4)

installments_total = terms_months.copy()
installments_paid  = np.array([
    rng.integers(0, t + 1) for t in installments_total
])

# Compute default status using modifiers
cust_df_idx = {cid: i for i, cid in enumerate(cust_ids)}

cont_status = []
for i in range(N_CONT):
    prod = prod_col[i]
    base_dr = default_rate_by_product[prod]

    # channel modifier
    cid = cust_sample[i]
    cust_pos = cust_df_idx.get(cid, None)
    ch = customers.iloc[cust_pos]["acquisition_channel"] if cust_pos is not None else "organic"
    ch_mod = 2.0 if ch == "paid_search" else 1.0  # organic 2x better

    # multi-product modifier (trend 3)
    pc = customers.iloc[cust_pos]["products_count"] if cust_pos is not None else 1
    mp_mod = 0.4 if pc >= 2 else 1.0

    eff_dr = base_dr * ch_mod * mp_mod
    eff_dr = min(eff_dr, 0.60)

    paid_ratio = installments_paid[i] / max(installments_total[i], 1)
    if rng.random() < eff_dr and paid_ratio < 0.8:
        status = "defaulted"
    elif paid_ratio >= 1.0:
        status = "settled"
    elif rng.random() < 0.05:
        status = "renegotiated"
    else:
        status = "active"
    cont_status.append(status)

collateral_col = [
    rng.choice(collaterals, p=coll_by_product[p]) for p in prod_col
]

contracts = pd.DataFrame({
    "contract_id":        cont_ids,
    "customer_id":        cust_sample,
    "product_type":       prod_col,
    "contract_date":      contract_dates,
    "maturity_date":      maturity_dates,
    "principal_amount":   principal,
    "interest_rate":      interest_rates,
    "installments_total": installments_total,
    "installments_paid":  installments_paid,
    "status":             cont_status,
    "collateral":         collateral_col,
})

contracts.to_parquet(f"{OUT_DIR}/contracts.parquet", index=False)
print(f"  contracts: {len(contracts):,} registros")

# ─── TABELA 3: payments ─────────────────────────────────────────────────────

print("Gerando payments...")

N_PAY = 800_000
pay_ids = gen_uuid(N_PAY)

cont_sample_idx = rng.integers(0, N_CONT, N_PAY)
pay_cont_ids = contracts.iloc[cont_sample_idx]["contract_id"].values
pay_cust_ids = contracts.iloc[cont_sample_idx]["customer_id"].values

pay_methods = ["pix", "boleto", "debit", "auto_debit"]
pay_method_w = [0.35, 0.30, 0.20, 0.15]
pay_method_col = rng.choice(pay_methods, size=N_PAY, p=pay_method_w)

amount_due = rng.lognormal(7.5, 0.6, N_PAY).round(2)

due_dates = rand_date(START, END, N_PAY)

# Determine days_late based on method and jan/feb seasonality
days_late = np.zeros(N_PAY, dtype=int)
pay_statuses = []
payment_dates = []
amounts_paid = []

for i in range(N_PAY):
    method = pay_method_col[i]
    month  = due_dates[i].month

    # auto_debit: 80% less default than boleto
    if method == "auto_debit":
        default_prob = 0.02
        late_prob    = 0.05
    elif method == "boleto":
        default_prob = 0.10
        late_prob    = 0.20
    elif method == "pix":
        default_prob = 0.03
        late_prob    = 0.08
    else:  # debit
        default_prob = 0.04
        late_prob    = 0.10

    # jan/feb seasonality: +40%
    if month in (1, 2):
        default_prob = min(default_prob * 1.40, 0.70)
        late_prob    = min(late_prob    * 1.40, 0.80)

    r = rng.random()
    if r < default_prob:
        pay_statuses.append("defaulted")
        days_late[i] = int(rng.integers(31, 180))
        payment_dates.append(None)
        amounts_paid.append(None)
    elif r < default_prob + late_prob:
        dl = int(rng.integers(1, 30))
        days_late[i] = dl
        pay_statuses.append("paid_late")
        pay_date = due_dates[i] + timedelta(days=dl)
        payment_dates.append(pay_date)
        amounts_paid.append(round(float(amount_due[i]) * rng.uniform(0.95, 1.02), 2))
    elif r < default_prob + late_prob + 0.05:
        # pending
        pay_statuses.append("pending")
        days_late[i] = 0
        payment_dates.append(None)
        amounts_paid.append(None)
    else:
        dl = int(rng.integers(-5, 1))  # on time or early
        days_late[i] = dl
        pay_statuses.append("paid_on_time")
        pay_date = due_dates[i] + timedelta(days=dl)
        payment_dates.append(pay_date)
        amounts_paid.append(round(float(amount_due[i]) * rng.uniform(0.99, 1.01), 2))

# Inject quality problem: 3% due_date before contract_date
bad_mask = rng.random(N_PAY) < 0.03
bad_cont_dates = contracts.iloc[cont_sample_idx[bad_mask]]["contract_date"].values
for idx, bd in zip(np.where(bad_mask)[0], bad_cont_dates):
    offset = int(rng.integers(1, 90))
    due_dates[idx] = bd - timedelta(days=offset)

payments = pd.DataFrame({
    "payment_id":     pay_ids,
    "contract_id":    pay_cont_ids,
    "customer_id":    pay_cust_ids,
    "due_date":       due_dates,
    "payment_date":   payment_dates,
    "amount_due":     amount_due,
    "amount_paid":    amounts_paid,
    "days_late":      days_late,
    "payment_method": pay_method_col,
    "status":         pay_statuses,
})

payments.to_parquet(f"{OUT_DIR}/payments.parquet", index=False)
print(f"  payments: {len(payments):,} registros")

# ─── TABELA 4: app_events ───────────────────────────────────────────────────

print("Gerando app_events...")

N_EVT = 2_000_000
evt_ids = gen_uuid(N_EVT)

event_types = [
    "login", "view_contract", "make_payment", "check_balance",
    "simulate_loan", "contact_support", "view_offer", "accept_offer", "reject_offer"
]
evt_w = [0.25, 0.18, 0.15, 0.15, 0.10, 0.07, 0.05, 0.03, 0.02]

channels_app = ["app_ios", "app_android", "web"]
chan_app_w   = [0.38, 0.45, 0.17]

# 2% will have non-existent customer_id (quality issue)
valid_cust = rng.choice(cust_ids, size=N_EVT)
ghost_mask = rng.random(N_EVT) < 0.02
ghost_ids  = gen_uuid(int(ghost_mask.sum()))
valid_cust[ghost_mask] = ghost_ids

evt_datetimes = rand_datetime(START, END, N_EVT)
session_dur   = rng.integers(10, 1800, N_EVT)  # 10s to 30min

app_events = pd.DataFrame({
    "event_id":                evt_ids,
    "customer_id":             valid_cust,
    "event_date":              evt_datetimes,
    "event_type":              rng.choice(event_types, size=N_EVT, p=evt_w),
    "channel":                 rng.choice(channels_app, size=N_EVT, p=chan_app_w),
    "session_duration_seconds": session_dur,
})

app_events.to_parquet(f"{OUT_DIR}/app_events.parquet", index=False)
print(f"  app_events: {len(app_events):,} registros")

# ─── TABELA 5: proposals ────────────────────────────────────────────────────

print("Gerando proposals...")

N_PROP = 200_000
prop_ids = gen_uuid(N_PROP)

decisions    = ["approved", "rejected", "pending", "cancelled"]
decision_w   = [0.55, 0.30, 0.08, 0.07]

rej_reasons  = ["score", "income", "documentation", "bureau", "capacity"]
rej_reason_w = [0.35, 0.25, 0.15, 0.15, 0.10]

prop_cust = rng.choice(cust_ids, size=N_PROP)
prop_dates = rand_date(START, END, N_PROP)
prop_products = rng.choice(products, size=N_PROP, p=prod_w)

requested_amount = rng.lognormal(9.5, 0.7, N_PROP).round(2)

decision_col = rng.choice(decisions, size=N_PROP, p=decision_w)

approved_amount = np.where(
    pd.Series(decision_col) == "approved",
    (requested_amount * rng.uniform(0.70, 1.00, N_PROP)).round(2),
    np.nan,
)

ir_offered = rng.uniform(0.018, 0.120, N_PROP).round(4)

rej_reason_col = np.where(
    pd.Series(decision_col) == "rejected",
    rng.choice(rej_reasons, size=N_PROP, p=rej_reason_w),
    None,
)

# bureau score: age 18-25 vs 36-45 inversion (trend 5)
prop_cust_df = pd.DataFrame({"customer_id": prop_cust}).merge(
    customers[["customer_id", "age_group"]], on="customer_id", how="left"
)
age_grp_col = prop_cust_df["age_group"].fillna("26-35").values

bureau_scores = np.zeros(N_PROP, dtype=float)
for i in range(N_PROP):
    ag = age_grp_col[i]
    if ag == "18-25":
        bureau_scores[i] = rng.integers(300, 620)   # low score…
    elif ag == "36-45":
        bureau_scores[i] = rng.integers(320, 660)   # similar low but pays worse
    elif ag == "26-35":
        bureau_scores[i] = rng.integers(450, 750)
    elif ag == "46-60":
        bureau_scores[i] = rng.integers(500, 800)
    else:
        bureau_scores[i] = rng.integers(550, 850)

# inject 5% missing bureau_score
missing_bureau = rng.random(N_PROP) < 0.05
bureau_scores[missing_bureau] = np.nan

proposals = pd.DataFrame({
    "proposal_id":          prop_ids,
    "customer_id":          prop_cust,
    "proposal_date":        prop_dates,
    "product_type":         prop_products,
    "requested_amount":     requested_amount,
    "approved_amount":      approved_amount,
    "interest_rate_offered": ir_offered,
    "decision":             decision_col,
    "rejection_reason":     rej_reason_col,
    "bureau_score":         bureau_scores,
})

proposals.to_parquet(f"{OUT_DIR}/proposals.parquet", index=False)
print(f"  proposals: {len(proposals):,} registros")

# ─── TABELA 6: collections ──────────────────────────────────────────────────

print("Gerando collections...")

N_COLL = 30_000
coll_ids = gen_uuid(N_COLL)

# only defaulted contracts
defaulted_mask = pd.Series(cont_status) == "defaulted"
defaulted_cont = contracts[defaulted_mask]

if len(defaulted_cont) < N_COLL:
    coll_cont_idx = rng.choice(len(defaulted_cont), size=N_COLL, replace=True)
else:
    coll_cont_idx = rng.choice(len(defaulted_cont), size=N_COLL, replace=False)

coll_cont_sample = defaulted_cont.iloc[coll_cont_idx]

coll_channels = ["sms", "email", "whatsapp", "phone", "letter"]
coll_chan_w   = [0.30, 0.25, 0.20, 0.20, 0.05]

outcomes     = ["paid", "renegotiated", "no_contact", "legal_action", "written_off", None]
# outcome depends on days_overdue at trigger (trend 8)
days_overdue_trigger = rng.integers(1, 180, N_COLL)

outcome_col      = []
recovery_amounts = []
resolution_days  = []

for i in range(N_COLL):
    dot = days_overdue_trigger[i]

    if dot <= 3:          # 85% recovery
        r = rng.random()
        if r < 0.70:
            out = "paid"
        elif r < 0.85:
            out = "renegotiated"
        else:
            out = "no_contact"
    elif dot <= 30:
        r = rng.random()
        if r < 0.40:
            out = "paid"
        elif r < 0.60:
            out = "renegotiated"
        elif r < 0.80:
            out = "no_contact"
        else:
            out = "legal_action"
    else:                 # 23% recovery after day 30
        r = rng.random()
        if r < 0.13:
            out = "paid"
        elif r < 0.23:
            out = "renegotiated"
        elif r < 0.55:
            out = "no_contact"
        elif r < 0.70:
            out = "legal_action"
        else:
            out = "written_off"

    # 8% without defined outcome (quality issue)
    if rng.random() < 0.08:
        out = None

    outcome_col.append(out)

    if out in ("paid", "renegotiated"):
        principal_val = coll_cont_sample.iloc[i]["principal_amount"]
        recovery_amounts.append(round(float(principal_val) * rng.uniform(0.20, 0.90), 2))
        resolution_days.append(int(rng.integers(1, 120)))
    else:
        recovery_amounts.append(None)
        resolution_days.append(None)

trigger_dates = rand_date(START, END, N_COLL)

collections = pd.DataFrame({
    "collection_id":          coll_ids,
    "contract_id":            coll_cont_sample["contract_id"].values,
    "customer_id":            coll_cont_sample["customer_id"].values,
    "trigger_date":           trigger_dates,
    "days_overdue_at_trigger": days_overdue_trigger,
    "channel_used":           rng.choice(coll_channels, size=N_COLL, p=coll_chan_w),
    "outcome":                outcome_col,
    "recovery_amount":        recovery_amounts,
    "resolution_days":        resolution_days,
})

collections.to_parquet(f"{OUT_DIR}/collections.parquet", index=False)
print(f"  collections: {len(collections):,} registros")

# ─── RELATÓRIO FINAL ────────────────────────────────────────────────────────

def compute_stats():
    # Reload for accurate stats
    pay = pd.read_parquet(f"{OUT_DIR}/payments.parquet")
    cont = pd.read_parquet(f"{OUT_DIR}/contracts.parquet")
    cust = pd.read_parquet(f"{OUT_DIR}/customers.parquet")

    total_pays = len(pay)
    default_pays = (pay["status"] == "defaulted").sum()
    general_default_rate = default_pays / total_pays * 100

    pay["due_month"] = pd.to_datetime(pay["due_date"]).dt.month
    janfev = pay[pay["due_month"].isin([1, 2])]
    janfev_default = (janfev["status"] == "defaulted").sum() / len(janfev) * 100

    avg_ticket = cont["principal_amount"].mean()
    ltv = (cont["principal_amount"] * cont["installments_total"] / 12 * (1 + cont["interest_rate"])).mean()

    top_channel = cust["acquisition_channel"].value_counts().idxmax()

    prod_default = cont.groupby("product_type")["status"].apply(
        lambda x: (x == "defaulted").sum() / len(x) * 100
    )
    best_prod = prod_default.idxmin()
    worst_prod = prod_default.idxmax()

    return {
        "customers":      len(cust),
        "contracts":      len(cont),
        "payments":       len(pay),
        "app_events":     N_EVT,
        "proposals":      N_PROP,
        "collections":    N_COLL,
        "default_geral":  general_default_rate,
        "default_janfev": janfev_default,
        "avg_ticket":     avg_ticket,
        "ltv":            ltv,
        "top_channel":    top_channel,
        "best_product":   best_prod,
        "worst_product":  worst_prod,
    }

s = compute_stats()

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

border_h = "=" * 46
border_top    = "+" + border_h + "+"
border_mid    = "+" + border_h + "+"
border_bottom = "+" + border_h + "+"

def row(text):
    return f"| {text:<44} |"

print()
print(border_top)
print(row("FINANCEFLOW -- DATASET REPORT"))
print(border_mid)
print(row(f"customers:    {s['customers']:>10,} registros"))
print(row(f"contracts:    {s['contracts']:>10,} registros"))
print(row(f"payments:     {s['payments']:>10,} registros"))
print(row(f"app_events:   {s['app_events']:>10,} registros"))
print(row(f"proposals:    {s['proposals']:>10,} registros"))
print(row(f"collections:  {s['collections']:>10,} registros"))
print(border_mid)
print(row(f"INADIMPLENCIA GERAL:     {s['default_geral']:>5.1f}%"))
print(row(f"INADIMPLENCIA JAN/FEV:   {s['default_janfev']:>5.1f}%"))
print(row(f"TICKET MEDIO:            R$ {s['avg_ticket']:>10,.0f}"))
print(row(f"LTV MEDIO:               R$ {s['ltv']:>10,.0f}"))
print(border_mid)
print(row(f"TOP CANAL AQUISICAO:     {s['top_channel']}"))
print(row(f"MELHOR PRODUTO (risk):   {s['best_product']}"))
print(row(f"PIOR PRODUTO (risk):     {s['worst_product']}"))
print(border_bottom)

# Confirm all files exist
import os
files = [
    "customers.parquet", "contracts.parquet", "payments.parquet",
    "app_events.parquet", "proposals.parquet", "collections.parquet"
]
print()
all_ok = True
for f in files:
    path = f"{OUT_DIR}/{f}"
    exists = os.path.exists(path)
    size_mb = os.path.getsize(path) / 1_048_576 if exists else 0
    mark = "OK" if exists else "FAIL"
    print(f"  [{mark}] {f:<30} {size_mb:>7.1f} MB")
    if not exists:
        all_ok = False

print()
print("Todos os arquivos gerados com sucesso!" if all_ok else "ERRO: arquivos faltando!")
