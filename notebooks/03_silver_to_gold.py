# Databricks notebook source
# =============================================================================
# Notebook 03 — Silver to Gold
# Customer Health Command Center
# Workspace: https://dbc-69445b27-9472.cloud.databricks.com
# =============================================================================
# Joins all silver signals into a composite customer health score.
# Produces three gold tables consumed directly by the Databricks App.
#
# Scoring model: explainable heuristic (no black-box ML required).
# Each dimension contributes a sub-score (0–100); sub-scores are weighted
# into a final health_score (0–100).
#
# Run on: any Databricks cluster (serverless compatible)
# Depends on: notebook 02 having populated the silver tables
# Idempotent: yes — uses CREATE OR REPLACE TABLE throughout
# =============================================================================

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG       = "dn_saas_demo"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA   = "gold"
SILVER        = f"{CATALOG}.{SILVER_SCHEMA}"
GOLD          = f"{CATALOG}.{GOLD_SCHEMA}"

print(f"Silver source  : {SILVER}")
print(f"Gold target    : {GOLD}")

# COMMAND ----------

# =============================================================================
# STEP 1 — Assemble the per-account feature set
# =============================================================================
# Left-join all silver tables onto the account spine.
# COALESCE fills nulls so every account gets a score even if it has no events.

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD}._gold_features AS
SELECT
    a.account_id,
    a.account_name,
    a.segment,
    a.region,
    a.industry,
    a.arr,
    a.plan_tier,
    a.csm_name,
    a.days_to_renewal,
    a.tenure_days,

    -- User activity
    COALESCE(u.total_users,          0)   AS total_users,
    COALESCE(u.active_users_30d,     0)   AS active_users_30d,
    COALESCE(u.login_recency_days,  90)   AS login_recency_days,

    -- Feature adoption & session depth
    COALESCE(f.adopted_features_30d, 0)   AS adopted_features_30d,
    COALESCE(f.total_sessions_30d,   0)   AS total_sessions_30d,
    COALESCE(f.avg_session_minutes_30d, 0) AS avg_session_minutes_30d,

    -- Support burden
    COALESCE(s.ticket_count_30d,       0) AS ticket_count_30d,
    COALESCE(s.critical_tickets_30d,   0) AS critical_tickets_30d,
    COALESCE(s.avg_resolution_hours_30d, 0) AS avg_resolution_hours_30d,
    COALESCE(s.avg_csat_30d,         3.0) AS avg_csat_30d,
    COALESCE(s.open_tickets_30d,       0) AS open_tickets_30d,

    -- Subscription
    sub.contract_status,
    sub.payment_status,
    sub.mrr

FROM {SILVER}.silver_accounts             a
LEFT JOIN {SILVER}.silver_user_activity_30d    u ON a.account_id = u.account_id
LEFT JOIN {SILVER}.silver_feature_adoption_30d f ON a.account_id = f.account_id
LEFT JOIN {SILVER}.silver_support_summary_30d  s ON a.account_id = s.account_id
LEFT JOIN {SILVER}.silver_subscription_status  sub ON a.account_id = sub.account_id
""")

print(f"_gold_features : {spark.table(f'{GOLD}._gold_features').count():,} rows")

# COMMAND ----------

# =============================================================================
# STEP 2 — Score each dimension (0–100 each)
# =============================================================================
# Scoring rules are intentionally simple and explainable.
# All LEAST/GREATEST calls keep values in [0, 100].
#
# USAGE SCORE (weight 40%)
#   + active_users_30d relative to total users (adoption rate)
#   + adopted_features_30d (breadth)
#   + total_sessions_30d (volume)
#   - login_recency_days (staleness penalty)
#
# SUPPORT SCORE (weight 30%)
#   Starts at 100; deductions for ticket volume, severity, slow resolution, low CSAT
#
# GROWTH SCORE (weight 15%)
#   Contract health + payment status + days to renewal
#
# RELATIONSHIP SCORE (weight 15%)
#   Tenure as a signal of stickiness; penalized if contract is at-risk

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD}._gold_scored AS
SELECT
    *,

    -- ── USAGE SCORE ──────────────────────────────────────────────────────────
    GREATEST(0, LEAST(100,
        -- Adoption rate: what fraction of users are active?
        CASE WHEN total_users > 0
             THEN (active_users_30d * 1.0 / total_users) * 40
             ELSE 0 END
        -- Feature breadth: up to 8 features available
        + LEAST(30, adopted_features_30d * 30.0 / 8)
        -- Session volume: normalise against 200 sessions as "excellent"
        + LEAST(20, total_sessions_30d * 20.0 / 200)
        -- Staleness penalty: -1 point per day of recency beyond 7 days, max -10
        - GREATEST(0, LEAST(10, (login_recency_days - 7) * 0.5))
    )) AS usage_score,

    -- ── SUPPORT SCORE ────────────────────────────────────────────────────────
    GREATEST(0, LEAST(100,
        100
        -- Too many tickets is a bad sign (-3 per ticket above 5)
        - GREATEST(0, (ticket_count_30d - 5)) * 3
        -- Critical tickets are worse (-8 each)
        - critical_tickets_30d * 8
        -- Slow resolution (-0.1 per hour beyond 24h)
        - GREATEST(0, (avg_resolution_hours_30d - 24)) * 0.1
        -- CSAT: 4.5 = no penalty; each 0.1 below 4.5 = -2 points
        - GREATEST(0, (4.5 - avg_csat_30d) * 20)
    )) AS support_score,

    -- ── GROWTH SCORE ─────────────────────────────────────────────────────────
    GREATEST(0, LEAST(100,
        -- Start with full marks; deduct for at-risk/churned contract and payment issues
        CASE contract_status
            WHEN 'Active'  THEN 70
            WHEN 'At Risk' THEN 40
            WHEN 'Churned' THEN 0
            ELSE 50
        END
        + CASE payment_status
            WHEN 'Current' THEN 30
            WHEN 'Overdue' THEN 10
            WHEN 'Failed'  THEN 0
            ELSE 20
        END
        -- Imminent renewal is a risk multiplier
        - CASE WHEN days_to_renewal < 30  THEN 15
               WHEN days_to_renewal < 60  THEN 8
               WHEN days_to_renewal < 90  THEN 3
               ELSE 0
          END
    )) AS growth_score,

    -- ── RELATIONSHIP SCORE ───────────────────────────────────────────────────
    GREATEST(0, LEAST(100,
        -- Tenure up to 2 years gives full marks
        LEAST(70, tenure_days * 70.0 / 730)
        + CASE contract_status
              WHEN 'Active'  THEN 30
              WHEN 'At Risk' THEN 10
              ELSE 0
          END
    )) AS relationship_score

FROM {GOLD}._gold_features
""")

print(f"_gold_scored : {spark.table(f'{GOLD}._gold_scored').count():,} rows")

# COMMAND ----------

# =============================================================================
# STEP 3 — Composite health score, churn risk band, risk reason, next action
# =============================================================================

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD}.gold_account_health AS
SELECT
    account_id,
    account_name,
    segment,
    region,
    industry,
    arr,
    plan_tier,
    csm_name,
    days_to_renewal,
    tenure_days,
    active_users_30d,
    login_recency_days,
    adopted_features_30d,
    total_sessions_30d,
    avg_session_minutes_30d,
    ticket_count_30d,
    avg_resolution_hours_30d,
    ROUND(avg_csat_30d, 2)    AS avg_csat_30d,
    contract_status,
    payment_status,

    -- ── Composite health score (weighted average of four dimensions) ─────────
    ROUND(
        usage_score        * 0.40
        + support_score    * 0.30
        + growth_score     * 0.15
        + relationship_score * 0.15
    , 1) AS health_score,

    -- ── Churn risk band ───────────────────────────────────────────────────────
    CASE
        WHEN (usage_score * 0.40 + support_score * 0.30
              + growth_score * 0.15 + relationship_score * 0.15) < 40
             OR payment_status IN ('Overdue','Failed')
             OR (days_to_renewal <= 30 AND active_users_30d = 0)
        THEN 'High'

        WHEN (usage_score * 0.40 + support_score * 0.30
              + growth_score * 0.15 + relationship_score * 0.15) < 65
             OR days_to_renewal <= 60
             OR contract_status = 'At Risk'
        THEN 'Medium'

        ELSE 'Low'
    END AS churn_risk_band,

    -- ── Primary risk reason (human-readable, used in the app) ─────────────────
    CASE
        WHEN payment_status IN ('Overdue','Failed')
            THEN 'Payment issue detected'
        WHEN active_users_30d = 0
            THEN 'No product activity in 30 days'
        WHEN login_recency_days > 30
            THEN 'Users not logging in recently'
        WHEN adopted_features_30d <= 1
            THEN 'Low feature adoption'
        WHEN critical_tickets_30d >= 3
            THEN 'High-severity support burden'
        WHEN avg_csat_30d < 3.0
            THEN 'Low customer satisfaction'
        WHEN days_to_renewal <= 30 AND
             (usage_score * 0.40 + support_score * 0.30
              + growth_score * 0.15 + relationship_score * 0.15) < 60
            THEN 'Renewal at risk — low health'
        WHEN contract_status = 'At Risk'
            THEN 'Contract flagged as at risk'
        ELSE 'Healthy — no primary risk signal'
    END AS risk_reason,

    -- ── Next best action for the CSM ─────────────────────────────────────────
    CASE
        WHEN payment_status IN ('Overdue','Failed')
            THEN 'Engage billing contact; escalate to account manager'
        WHEN active_users_30d = 0
            THEN 'Schedule re-engagement call with champion'
        WHEN login_recency_days > 30
            THEN 'Send personalised outreach with quick-win use cases'
        WHEN adopted_features_30d <= 1
            THEN 'Book feature enablement session'
        WHEN critical_tickets_30d >= 3
            THEN 'Coordinate support escalation review'
        WHEN avg_csat_30d < 3.0
            THEN 'Conduct executive business review'
        WHEN days_to_renewal <= 60
            THEN 'Initiate renewal conversation with expansion offer'
        ELSE 'Continue regular cadence; share new feature updates'
    END AS next_best_action

FROM {GOLD}._gold_scored
""")

print(f"gold_account_health : {spark.table(f'{GOLD}.gold_account_health').count():,} rows")

# COMMAND ----------

# =============================================================================
# STEP 4 — gold_exec_kpis (single-row executive summary)
# =============================================================================

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD}.gold_exec_kpis AS
SELECT
    COUNT(*)                                                  AS total_accounts,
    SUM(arr)                                                  AS total_arr,
    ROUND(AVG(health_score), 1)                              AS avg_health_score,
    ROUND(100.0 * SUM(CASE WHEN churn_risk_band = 'High'
                            THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_high_risk,
    SUM(CASE WHEN churn_risk_band IN ('High','Medium')
             THEN arr ELSE 0 END)                             AS net_revenue_at_risk,
    ROUND(AVG(avg_csat_30d), 2)                              AS avg_csat,
    ROUND(AVG(active_users_30d), 1)                          AS avg_active_users_30d
FROM {GOLD}.gold_account_health
""")

display(spark.table(f"{GOLD}.gold_exec_kpis"))

# COMMAND ----------

# =============================================================================
# STEP 5 — gold_risk_segments (by segment, plan tier, region)
# =============================================================================

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD}.gold_risk_segments AS
SELECT
    segment,
    plan_tier,
    region,
    COUNT(*)                                                  AS account_count,
    SUM(arr)                                                  AS total_arr,
    ROUND(AVG(health_score), 1)                              AS avg_health_score,
    SUM(CASE WHEN churn_risk_band = 'High'   THEN 1 ELSE 0 END) AS high_risk_count,
    SUM(CASE WHEN churn_risk_band = 'Medium' THEN 1 ELSE 0 END) AS medium_risk_count,
    SUM(CASE WHEN churn_risk_band = 'Low'    THEN 1 ELSE 0 END) AS low_risk_count,
    SUM(CASE WHEN churn_risk_band IN ('High','Medium')
             THEN arr ELSE 0 END)                             AS arr_at_risk
FROM {GOLD}.gold_account_health
GROUP BY segment, plan_tier, region
""")

print(f"gold_risk_segments  : {spark.table(f'{GOLD}.gold_risk_segments').count():,} rows")

# COMMAND ----------

# Clean up internal staging table
spark.sql(f"DROP TABLE IF EXISTS {GOLD}._gold_features")
spark.sql(f"DROP TABLE IF EXISTS {GOLD}._gold_scored")

print("\n" + "="*60)
print("Gold layer complete.")
print("="*60)
spark.sql(f"SHOW TABLES IN {GOLD}").show()

# COMMAND ----------

# Quick validation — what does the health score distribution look like?
display(spark.sql(f"""
SELECT
    churn_risk_band,
    COUNT(*)                    AS accounts,
    ROUND(AVG(health_score), 1) AS avg_score,
    SUM(arr)                    AS total_arr,
    ROUND(AVG(avg_csat_30d), 2) AS avg_csat
FROM {GOLD}.gold_account_health
GROUP BY churn_risk_band
ORDER BY avg_score
"""))

# COMMAND ----------

# =============================================================================
# PRESENTER NOTES
# =============================================================================
# Why gold is the app-serving layer (explain this live):
#
# "Gold is the one layer the business user sees. Every column name is a
#  business term — no underscores from raw logs, no internal IDs we'd need
#  to look up. The app queries gold and displays it directly.
#
#  The scoring model is intentionally a weighted average of four signals:
#  usage, support, contract health, and relationship depth. I can tell a
#  business user exactly why a score is 34 — 'you have zero active users,
#  a P1 ticket open, and you're 18 days from renewal.' That's far more
#  valuable in a live demo than a black-box ML model.
#
#  The next_best_action column is the key column for CSMs — it tells them
#  exactly what to do next, which maps to your CSM playbook tooling.
#
#  Gold also enables BI (Databricks AI/BI dashboards), AI (Genie), and
#  agent use cases on top — this same table can power a natural-language
#  assistant with zero additional ETL."
# =============================================================================
