# Databricks notebook source
# =============================================================================
# Notebook 02 — Bronze to Silver
# Customer Health Command Center
# Workspace: https://dbc-69445b27-9472.cloud.databricks.com
# =============================================================================
# Reads raw bronze tables and produces clean, trusted, aggregated silver tables
# that the gold scoring notebook consumes.
#
# Run on: any Databricks cluster (serverless compatible)
# Depends on: notebook 01 having populated the bronze tables
# Idempotent: yes — uses CREATE OR REPLACE TABLE throughout
# =============================================================================

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

CATALOG        = "dn_saas_demo"
BRONZE_SCHEMA  = "bronze"
SILVER_SCHEMA  = "silver"
BRONZE         = f"{CATALOG}.{BRONZE_SCHEMA}"
SILVER         = f"{CATALOG}.{SILVER_SCHEMA}"

# Anchor date — keep consistent with notebook 01 so all windows align
AS_OF_DATE = "2025-06-01"

print(f"Bronze source  : {BRONZE}")
print(f"Silver target  : {SILVER}")
print(f"AS_OF_DATE     : {AS_OF_DATE}")

# COMMAND ----------

# ---------------------------------------------------------------------------
# 1. silver_accounts — clean account dimension
# ---------------------------------------------------------------------------
# Canonicalize types, compute tenure, flag near-renewal accounts.
# This is the account spine that every other silver table joins to.

spark.sql(f"""
CREATE OR REPLACE TABLE {SILVER}.silver_accounts AS
SELECT
    account_id,
    account_name,
    segment,
    region,
    industry,
    CAST(arr AS BIGINT)                                       AS arr,
    plan_tier,
    csm_name,
    CAST(start_date   AS DATE)                               AS start_date,
    CAST(renewal_date AS DATE)                               AS renewal_date,
    DATEDIFF(CAST(renewal_date AS DATE), DATE('{AS_OF_DATE}')) AS days_to_renewal,
    DATEDIFF(DATE('{AS_OF_DATE}'), CAST(start_date AS DATE))   AS tenure_days
FROM {BRONZE}.bronze_accounts
WHERE account_id IS NOT NULL
""")

print(f"silver_accounts             : {spark.table(f'{SILVER}.silver_accounts').count():,} rows")

# COMMAND ----------

# ---------------------------------------------------------------------------
# 2. silver_user_activity_30d — per-account login and session aggregates
# ---------------------------------------------------------------------------
# "Active" means the user logged in at least once in the last 30 days.
# login_recency_days: days since the most recent login across all users on the account.

spark.sql(f"""
CREATE OR REPLACE TABLE {SILVER}.silver_user_activity_30d AS
SELECT
    u.account_id,
    COUNT(DISTINCT u.user_id)                                               AS total_users,
    COUNT(DISTINCT
        CASE WHEN DATEDIFF(DATE('{AS_OF_DATE}'), CAST(u.last_login_at AS DATE)) <= 30
             THEN u.user_id END)                                            AS active_users_30d,
    MIN(DATEDIFF(DATE('{AS_OF_DATE}'), CAST(u.last_login_at AS DATE)))     AS login_recency_days
FROM {BRONZE}.bronze_users u
GROUP BY u.account_id
""")

print(f"silver_user_activity_30d    : {spark.table(f'{SILVER}.silver_user_activity_30d').count():,} rows")

# COMMAND ----------

# ---------------------------------------------------------------------------
# 3. silver_feature_adoption_30d — feature breadth and session depth
# ---------------------------------------------------------------------------
# adopted_features_30d: distinct features used in the last 30 days.
# total_sessions_30d and avg_session_minutes_30d: volume and depth of engagement.

spark.sql(f"""
CREATE OR REPLACE TABLE {SILVER}.silver_feature_adoption_30d AS
SELECT
    account_id,
    COUNT(DISTINCT feature_name)                                             AS adopted_features_30d,
    COUNT(*)                                                                 AS total_sessions_30d,
    ROUND(AVG(session_minutes), 1)                                           AS avg_session_minutes_30d
FROM {BRONZE}.bronze_product_events
WHERE DATEDIFF(DATE('{AS_OF_DATE}'), CAST(event_time AS DATE)) <= 30
GROUP BY account_id
""")

print(f"silver_feature_adoption_30d : {spark.table(f'{SILVER}.silver_feature_adoption_30d').count():,} rows")

# COMMAND ----------

# ---------------------------------------------------------------------------
# 4. silver_support_summary_30d — support burden per account
# ---------------------------------------------------------------------------
# Tickets created in the last 30 days.
# High ticket volume + slow resolution + low CSAT → negative health signal.

spark.sql(f"""
CREATE OR REPLACE TABLE {SILVER}.silver_support_summary_30d AS
SELECT
    account_id,
    COUNT(*)                                                                 AS ticket_count_30d,
    SUM(CASE WHEN severity IN ('P1','P2') THEN 1 ELSE 0 END)               AS critical_tickets_30d,
    ROUND(AVG(resolution_hours), 1)                                          AS avg_resolution_hours_30d,
    ROUND(AVG(CAST(csat_score AS DOUBLE)), 2)                               AS avg_csat_30d,
    SUM(CASE WHEN status IN ('Open','In Progress') THEN 1 ELSE 0 END)      AS open_tickets_30d
FROM {BRONZE}.bronze_support_tickets
WHERE DATEDIFF(DATE('{AS_OF_DATE}'), CAST(created_at AS DATE)) <= 30
GROUP BY account_id
""")

print(f"silver_support_summary_30d  : {spark.table(f'{SILVER}.silver_support_summary_30d').count():,} rows")

# COMMAND ----------

# ---------------------------------------------------------------------------
# 5. silver_subscription_status — clean subscription dimension
# ---------------------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE {SILVER}.silver_subscription_status AS
SELECT
    account_id,
    plan_tier,
    CAST(mrr AS BIGINT)        AS mrr,
    contract_status,
    CAST(renewal_date AS DATE) AS renewal_date,
    payment_status
FROM {BRONZE}.bronze_subscriptions
WHERE account_id IS NOT NULL
""")

print(f"silver_subscription_status  : {spark.table(f'{SILVER}.silver_subscription_status').count():,} rows")

# COMMAND ----------

# ---------------------------------------------------------------------------
# 6. silver_account_usage_daily — daily event counts per account (90d)
# ---------------------------------------------------------------------------
# Used for trend charts and to detect "dark period" patterns (sudden drops).

spark.sql(f"""
CREATE OR REPLACE TABLE {SILVER}.silver_account_usage_daily AS
SELECT
    account_id,
    CAST(event_time AS DATE)   AS event_date,
    COUNT(*)                   AS daily_events,
    COUNT(DISTINCT user_id)    AS daily_active_users,
    SUM(session_minutes)       AS daily_session_minutes
FROM {BRONZE}.bronze_product_events
WHERE DATEDIFF(DATE('{AS_OF_DATE}'), CAST(event_time AS DATE)) <= 90
GROUP BY account_id, CAST(event_time AS DATE)
""")

print(f"silver_account_usage_daily  : {spark.table(f'{SILVER}.silver_account_usage_daily').count():,} rows")

# COMMAND ----------

print("\n" + "="*60)
print("Silver layer complete.")
print("="*60)
spark.sql(f"SHOW TABLES IN {SILVER}").show()

# COMMAND ----------

# ---------------------------------------------------------------------------
# Spot-check queries (run these cells individually during a demo)
# ---------------------------------------------------------------------------

display(spark.sql(f"""
-- Top 10 accounts by active users in the last 30 days
SELECT
    a.account_name,
    a.segment,
    a.plan_tier,
    u.active_users_30d,
    u.login_recency_days,
    f.adopted_features_30d,
    f.avg_session_minutes_30d
FROM {SILVER}.silver_accounts       a
JOIN {SILVER}.silver_user_activity_30d    u USING (account_id)
JOIN {SILVER}.silver_feature_adoption_30d f USING (account_id)
ORDER BY u.active_users_30d DESC
LIMIT 10
"""))

# COMMAND ----------

# =============================================================================
# PRESENTER NOTES
# =============================================================================
# What "silver" means (explain this live):
#
# "Silver is the trusted, analytics-ready layer. Bronze is raw — we never
#  touch it after it lands. Silver is where we clean types, handle nulls,
#  apply business-meaningful time windows, and produce the aggregates that
#  feed scoring.
#
#  Every silver table is keyed on account_id. That design choice is
#  intentional — it makes the gold join simple and auditable.
#
#  The 30-day rolling windows are the core business signal: what happened
#  in the last month? That's what CSMs care about when they ask 'is this
#  account healthy right now?'
#
#  Nothing here is opinionated about what 'healthy' means — that lives in
#  gold. Silver just produces reliable numbers."
#
# Validation to run live:
#   spark.sql("SELECT COUNT(*) FROM dn_saas_demo.silver.silver_accounts").show()
#   spark.sql("DESCRIBE dn_saas_demo.silver.silver_feature_adoption_30d").show()
# =============================================================================
