# Databricks notebook source
# =============================================================================
# Notebook 01 — Generate Synthetic Bronze Data
# Customer Health Command Center
# Workspace: https://dbc-69445b27-9472.cloud.databricks.com
# =============================================================================
# Generates realistic synthetic SaaS data and writes it to Unity Catalog
# managed Delta tables in the bronze schema.
#
# Run on: any Databricks cluster (serverless compatible)
# Run time: ~2–3 minutes
# Idempotent: yes — uses overwrite mode throughout
# =============================================================================

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
import random
import uuid

# ---------------------------------------------------------------------------
# Config — change these if you need a different catalog or schema
# ---------------------------------------------------------------------------
CATALOG       = "dn_saas_demo"
BRONZE_SCHEMA = "bronze"
BRONZE        = f"{CATALOG}.{BRONZE_SCHEMA}"

RANDOM_SEED   = 42
np.random.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)

# Anchor date — all time-relative fields are computed from this
AS_OF_DATE = date(2025, 6, 1)

print(f"Target catalog : {CATALOG}")
print(f"Target schema  : {BRONZE_SCHEMA}")
print(f"AS_OF_DATE     : {AS_OF_DATE}")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------

SEGMENTS   = ["Enterprise", "Mid-Market", "SMB", "Startup"]
REGIONS    = ["AMER", "EMEA", "APAC", "LATAM"]
INDUSTRIES = ["Fintech", "Healthcare", "E-Commerce", "Logistics", "Media", "EdTech", "HR Tech"]

PLAN_TIERS = ["Starter", "Growth", "Professional", "Enterprise"]

# ARR ranges and user counts by plan tier (lo, hi)
PLAN_ARR = {
    "Starter":      (5_000,   30_000),
    "Growth":       (30_000,  120_000),
    "Professional": (120_000, 500_000),
    "Enterprise":   (500_000, 2_000_000),
}

PLAN_USERS = {
    "Starter":      (1,  5),
    "Growth":       (5,  30),
    "Professional": (20, 100),
    "Enterprise":   (80, 500),
}

SEGMENT_PLAN_WEIGHTS = {
    "Enterprise":  [0,    0,    0.20, 0.80],
    "Mid-Market":  [0,    0.10, 0.60, 0.30],
    "SMB":         [0.30, 0.50, 0.20, 0],
    "Startup":     [0.60, 0.30, 0.10, 0],
}

CSM_NAMES = [
    "Jordan Lee", "Priya Nair", "Marcus Chen", "Sofia Reyes",
    "Tom Okafor",  "Anika Patel", "David Kim",  "Laura Müller",
]

USER_ROLES  = ["Admin", "Power User", "Standard User", "Viewer"]
EVENT_TYPES = ["login", "report_view", "export", "api_call", "dashboard_view",
               "integration_setup", "alert_config", "user_invite", "data_upload", "search"]
FEATURES    = ["Analytics", "Reporting", "Integrations", "Alerts",
               "Data Import", "API Access", "Admin Console", "Collaboration"]
DEVICES     = ["Desktop", "Mobile", "Tablet"]

TICKET_CATEGORIES   = ["Billing", "Feature Request", "Bug", "Onboarding", "Performance", "Access"]
TICKET_SEVERITIES   = ["P1", "P2", "P3", "P4"]
TICKET_STATUSES     = ["Open", "In Progress", "Resolved", "Closed"]

CONTRACT_STATUSES   = ["Active", "Active", "Active", "Churned", "At Risk"]
PAYMENT_STATUSES    = ["Current", "Current", "Current", "Overdue", "Failed"]

# COMMAND ----------

# ---------------------------------------------------------------------------
# Helper: risk profile per account
# Higher risk → weaker engagement, more support issues, nearer renewal
# This is used internally during generation; it is NOT stored as a column.
# ---------------------------------------------------------------------------

def assign_risk_profile(n: int) -> np.ndarray:
    """
    Returns an array of risk probabilities in [0, 1] for n accounts.
    ~60% low risk, ~25% medium, ~15% high.
    """
    return np.random.beta(a=1.5, b=4.0, size=n)  # skewed toward low risk


# COMMAND ----------

# ---------------------------------------------------------------------------
# 1. bronze_accounts (500 rows)
# ---------------------------------------------------------------------------

N_ACCOUNTS = 500
risk_profiles = assign_risk_profile(N_ACCOUNTS)

accounts = []
for i in range(N_ACCOUNTS):
    risk          = risk_profiles[i]
    segment       = np.random.choice(SEGMENTS, p=[0.15, 0.30, 0.35, 0.20])
    plan_tier     = np.random.choice(PLAN_TIERS, p=SEGMENT_PLAN_WEIGHTS[segment])
    arr_lo, arr_hi = PLAN_ARR[plan_tier]

    # Riskier accounts tend to be on lower ARR (less invested in the platform)
    arr_raw = np.random.uniform(arr_lo, arr_hi)
    arr     = round(arr_raw * (1 - 0.2 * risk))

    # Riskier accounts have nearer renewals
    days_to_renewal = max(7, int(np.random.uniform(10, 365) * (1 - 0.5 * risk)))
    renewal_date    = AS_OF_DATE + timedelta(days=days_to_renewal)

    start_days_ago  = np.random.randint(180, 1095)
    start_date      = AS_OF_DATE - timedelta(days=start_days_ago)

    accounts.append({
        "account_id":   f"ACC-{i+1:04d}",
        "account_name": f"{np.random.choice(INDUSTRIES)} Co {i+1}",
        "segment":      segment,
        "region":       np.random.choice(REGIONS, p=[0.50, 0.25, 0.18, 0.07]),
        "industry":     np.random.choice(INDUSTRIES),
        "arr":          int(arr),
        "plan_tier":    plan_tier,
        "start_date":   start_date.isoformat(),
        "renewal_date": renewal_date.isoformat(),
        "csm_name":     np.random.choice(CSM_NAMES),
        # internal only — used to distribute correlated signals to other tables
        "_risk":        round(float(risk), 4),
    })

df_accounts = pd.DataFrame(accounts)

# Build a lookup dict for downstream tables
account_risk = dict(zip(df_accounts["account_id"], df_accounts["_risk"]))
account_plan = dict(zip(df_accounts["account_id"], df_accounts["plan_tier"]))

# Drop internal column before writing
df_accounts_out = df_accounts.drop(columns=["_risk"])

sdf_accounts = spark.createDataFrame(df_accounts_out)
sdf_accounts.write.mode("overwrite").saveAsTable(f"{BRONZE}.bronze_accounts")
print(f"bronze_accounts        : {sdf_accounts.count():,} rows")

# COMMAND ----------

# ---------------------------------------------------------------------------
# 2. bronze_users (8,000 rows)
# ---------------------------------------------------------------------------

N_USERS = 8_000

account_ids = df_accounts["account_id"].tolist()

# Distribute users proportionally to plan tier
user_pool = []
for acc_id in account_ids:
    plan          = account_plan[acc_id]
    risk          = account_risk[acc_id]
    lo, hi        = PLAN_USERS[plan]
    n_users       = np.random.randint(lo, hi + 1)
    user_pool.extend([acc_id] * n_users)

# Sample to exactly N_USERS
random.shuffle(user_pool)
user_pool = (user_pool * ((N_USERS // len(user_pool)) + 1))[:N_USERS]

users = []
for j, acc_id in enumerate(user_pool):
    risk          = account_risk[acc_id]
    created_days  = np.random.randint(1, 730)
    created_at    = AS_OF_DATE - timedelta(days=created_days)

    # Riskier accounts have longer login recency (users not logging in)
    last_login_offset = int(np.random.exponential(scale=5 + 60 * risk))
    last_login_at     = AS_OF_DATE - timedelta(days=min(last_login_offset, 90))

    # Role distribution: fewer admins in riskier accounts
    role_weights = [max(0.02, 0.10 - 0.06 * risk), 0.20, 0.55, 0.25]
    role_weights = np.array(role_weights)
    role_weights /= role_weights.sum()

    users.append({
        "user_id":       f"USR-{j+1:05d}",
        "account_id":    acc_id,
        "user_role":     np.random.choice(USER_ROLES, p=role_weights),
        "created_at":    created_at.isoformat(),
        "last_login_at": last_login_at.isoformat(),
        "is_admin":      bool(np.random.random() < max(0.05, 0.15 - 0.10 * risk)),
    })

df_users = pd.DataFrame(users)
sdf_users = spark.createDataFrame(df_users)
sdf_users.write.mode("overwrite").saveAsTable(f"{BRONZE}.bronze_users")
print(f"bronze_users           : {sdf_users.count():,} rows")

# COMMAND ----------

# ---------------------------------------------------------------------------
# 3. bronze_product_events (~250,000 rows, 90 days)
# ---------------------------------------------------------------------------

# Target: ~250,000 events across 90 days and 500 accounts.
# Riskier accounts generate fewer events (lower engagement).

events = []
event_counter = 0

for acc_id in account_ids:
    risk     = account_risk[acc_id]
    plan     = account_plan[acc_id]

    # Events per day baseline scales with plan tier
    base_events_per_day = {"Starter": 5, "Growth": 20, "Professional": 60, "Enterprise": 120}[plan]
    # Risk depresses event volume
    daily_events = max(0, int(base_events_per_day * (1 - 0.75 * risk)))

    # Get users for this account
    acct_users = df_users[df_users["account_id"] == acc_id]["user_id"].tolist()
    if not acct_users:
        continue

    for day_offset in range(90):
        event_date = AS_OF_DATE - timedelta(days=89 - day_offset)

        # Riskier accounts have more zero-event days
        if np.random.random() < 0.05 + 0.40 * risk:
            continue

        n_events_today = max(0, int(np.random.poisson(daily_events)))

        for _ in range(n_events_today):
            # Feature breadth is lower for risky accounts
            n_features_available = max(2, int(len(FEATURES) * (1 - 0.5 * risk)))
            available_features   = FEATURES[:n_features_available]

            events.append({
                "event_id":       f"EVT-{event_counter:08d}",
                "event_time":     datetime.combine(
                    event_date,
                    datetime.min.time()
                ).replace(hour=np.random.randint(7, 22)).isoformat(),
                "account_id":     acc_id,
                "user_id":        np.random.choice(acct_users),
                "event_type":     np.random.choice(EVENT_TYPES),
                "feature_name":   np.random.choice(available_features),
                "session_minutes": max(1, int(np.random.exponential(8 * (1 - 0.5 * risk)))),
                "device_type":    np.random.choice(DEVICES, p=[0.65, 0.25, 0.10]),
            })
            event_counter += 1

df_events = pd.DataFrame(events)
sdf_events = spark.createDataFrame(df_events)
sdf_events.write.mode("overwrite").saveAsTable(f"{BRONZE}.bronze_product_events")
print(f"bronze_product_events  : {sdf_events.count():,} rows")

# COMMAND ----------

# ---------------------------------------------------------------------------
# 4. bronze_subscriptions (500 rows — one per account)
# ---------------------------------------------------------------------------

subscriptions = []
for _, row in df_accounts.iterrows():
    risk = row["_risk"]
    mrr  = round(row["arr"] / 12)

    # Contract status is worse for riskier accounts
    if risk > 0.70:
        contract_status = np.random.choice(["Active", "At Risk", "Churned"], p=[0.30, 0.50, 0.20])
        payment_status  = np.random.choice(["Current", "Overdue", "Failed"],  p=[0.40, 0.40, 0.20])
    elif risk > 0.40:
        contract_status = np.random.choice(["Active", "At Risk"],             p=[0.70, 0.30])
        payment_status  = np.random.choice(["Current", "Overdue"],            p=[0.80, 0.20])
    else:
        contract_status = "Active"
        payment_status  = "Current"

    subscriptions.append({
        "subscription_id":  f"SUB-{row['account_id']}",
        "account_id":       row["account_id"],
        "plan_tier":        row["plan_tier"],
        "mrr":              mrr,
        "contract_status":  contract_status,
        "renewal_date":     row["renewal_date"],
        "payment_status":   payment_status,
    })

df_subs = pd.DataFrame(subscriptions)
sdf_subs = spark.createDataFrame(df_subs)
sdf_subs.write.mode("overwrite").saveAsTable(f"{BRONZE}.bronze_subscriptions")
print(f"bronze_subscriptions   : {sdf_subs.count():,} rows")

# COMMAND ----------

# ---------------------------------------------------------------------------
# 5. bronze_support_tickets (12,000 rows)
# ---------------------------------------------------------------------------

N_TICKETS = 12_000

tickets = []
for t in range(N_TICKETS):
    # Weight toward riskier accounts generating more tickets
    weights   = np.array([1 + 4 * account_risk[a] for a in account_ids], dtype=float)
    weights  /= weights.sum()
    acc_id    = np.random.choice(account_ids, p=weights)
    risk      = account_risk[acc_id]

    created_days_ago = np.random.randint(1, 90)
    created_at       = AS_OF_DATE - timedelta(days=created_days_ago)

    # Severity: riskier accounts have more P1/P2
    sev_weights = [
        0.02 + 0.15 * risk,
        0.10 + 0.20 * risk,
        0.50 - 0.10 * risk,
        0.38 - 0.25 * risk,
    ]
    sev_weights = np.clip(sev_weights, 0.01, None)
    sev_weights = sev_weights / sev_weights.sum()
    severity = np.random.choice(TICKET_SEVERITIES, p=sev_weights)

    # Resolution hours: riskier accounts experience slower resolution
    base_hours = {"P1": 4, "P2": 12, "P3": 48, "P4": 120}[severity]
    resolution_hours = max(1, int(np.random.exponential(base_hours * (1 + 2 * risk))))

    # CSAT: riskier accounts give lower scores (or none at all)
    if np.random.random() < 0.3 + 0.4 * risk:
        csat_score = None  # no response
    else:
        csat_score = max(1, min(5, int(np.random.normal(4.2 - 2.0 * risk, 0.8))))

    status = np.random.choice(
        TICKET_STATUSES,
        p=[0.05 + 0.10 * risk, 0.10, 0.45, 0.40 - 0.10 * risk]
    )

    tickets.append({
        "ticket_id":         f"TKT-{t+1:05d}",
        "account_id":        acc_id,
        "created_at":        created_at.isoformat(),
        "severity":          severity,
        "category":          np.random.choice(TICKET_CATEGORIES),
        "resolution_hours":  resolution_hours,
        "csat_score":        csat_score,
        "status":            status,
    })

df_tickets = pd.DataFrame(tickets)
sdf_tickets = spark.createDataFrame(df_tickets)
sdf_tickets.write.mode("overwrite").saveAsTable(f"{BRONZE}.bronze_support_tickets")
print(f"bronze_support_tickets : {sdf_tickets.count():,} rows")

# COMMAND ----------

print("\n" + "="*60)
print("Bronze layer complete. All tables written to Unity Catalog.")
print("="*60)
spark.sql(f"SHOW TABLES IN {BRONZE}").show()

# COMMAND ----------

# =============================================================================
# PRESENTER NOTES
# =============================================================================
# What this notebook does (explain this live):
#
# "This is our data generation notebook — in a real deployment this layer
#  would be replaced by Fivetran connectors, Autoloader streams, or
#  existing Databricks pipelines landing raw data into the same bronze schema.
#
#  We generate 500 SaaS accounts across Enterprise, Mid-Market, SMB, and
#  Startup segments, 8,000 users, a quarter-million product events over 90 days,
#  subscription records, and 12,000 support tickets.
#
#  Critically: the data is correlated. Higher-risk accounts have weaker
#  engagement, more P1 tickets, slower resolution, lower CSAT, and payment
#  issues. This makes the downstream health scores meaningful — the app tells
#  a consistent story about each account.
#
#  Everything writes to Unity Catalog managed Delta tables in the bronze schema.
#  No dbfs:/ paths. No local files. Fully serverless-compatible."
# =============================================================================
