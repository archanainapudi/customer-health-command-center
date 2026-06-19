# Customer Health Command Center

A production-style Databricks demo for a digital-native B2B SaaS company.
Built for a Senior Solutions Architect interview showcase.

**Workspace:** `https://dbc-69445b27-9472.cloud.databricks.com`

---

## Business Scenario

A B2B SaaS company has 500 accounts and $28M in ARR. Their Customer Success team
manages renewals manually, using intuition rather than data. Product telemetry,
billing records, and support tickets live in three separate systems. Nobody has
connected the signals.

This demo shows how Databricks unifies those sources into a scored, ranked
account health view — surfaced through a Streamlit app that any CSM can open
without a Databricks login.

---

## Why This is a Strong Digital-Native SaaS Databricks Demo

| Capability | How it shows up |
|------------|----------------|
| Unity Catalog governance | Single catalog, three schemas, fine-grained SELECT grants on gold only |
| Medallion architecture | Bronze (raw) → Silver (trusted aggregates) → Gold (business-ready scores) |
| Serverless compatibility | No dbfs:/ writes; all outputs are Unity Catalog managed Delta tables |
| Explainable scoring | Weighted heuristic with human-readable risk_reason and next_best_action columns |
| Databricks Apps | Streamlit app deployed inside the workspace; SP auth is automatic |
| Extensibility | Gold table is immediately usable for AI/BI dashboards, Genie, or AI agents |

---

## Repository Structure

```
customer-health-command-center/
├── .gitignore
├── README.md
├── app/
│   ├── app.py               Streamlit Databricks App
│   ├── app.yaml             Databricks Apps manifest
│   └── requirements.txt
├── notebooks/
│   ├── 01_generate_synthetic_data.py
│   ├── 02_bronze_to_silver.py
│   ├── 03_silver_to_gold.py
│   └── 04_optional_churn_model.py
├── sql/
│   └── 00_setup_catalog_and_permissions.sql
├── scripts/
│   └── render_grants_template.py
└── docs/
    ├── demo_story.md
    └── talk_track.md
```

---

## Data Model

### Catalog: `dn_saas_demo`

#### Bronze — raw ingested data

| Table | Rows | Description |
|-------|------|-------------|
| `bronze_accounts` | 500 | Account master: segment, ARR, plan tier, renewal date, CSM |
| `bronze_users` | 8,000 | Users per account: role, last login |
| `bronze_product_events` | ~250,000 | Product telemetry: feature, session minutes, device |
| `bronze_subscriptions` | 500 | One subscription per account: MRR, contract status, payment |
| `bronze_support_tickets` | 12,000 | Tickets: severity, resolution hours, CSAT, status |

#### Silver — 30-day aggregates per account

| Table | Description |
|-------|-------------|
| `silver_accounts` | Clean account dimension + `days_to_renewal`, `tenure_days` |
| `silver_user_activity_30d` | `active_users_30d`, `login_recency_days` |
| `silver_feature_adoption_30d` | `adopted_features_30d`, `total_sessions_30d`, `avg_session_minutes_30d` |
| `silver_support_summary_30d` | `ticket_count_30d`, `critical_tickets_30d`, `avg_resolution_hours_30d`, `avg_csat_30d` |
| `silver_subscription_status` | Clean subscription with `contract_status`, `payment_status` |
| `silver_account_usage_daily` | Daily event counts per account (90-day window) |

#### Gold — app-facing health scores

| Table | Description |
|-------|-------------|
| `gold_account_health` | One row per account: composite health score, churn risk band, risk reason, next best action |
| `gold_exec_kpis` | Single-row executive summary: total ARR, avg health score, % high risk, ARR at risk |
| `gold_risk_segments` | Risk counts and ARR at risk by segment × plan tier × region |

---

## Medallion Architecture

```mermaid
flowchart LR
    subgraph Sources
        A1[Product Telemetry]
        A2[Billing / Stripe]
        A3[Support / Zendesk]
    end

    subgraph Unity Catalog — dn_saas_demo
        direction TB
        B[bronze\nRaw tables\n5 tables] --> S[silver\nClean aggregates\n6 tables]
        S --> G[gold\nHealth scores\n3 tables]
    end

    subgraph Consumers
        APP[Streamlit\nDatabricks App]
        BI[AI/BI Dashboard]
        GEN[Genie / AI Agent]
    end

    Sources --> B
    G --> APP
    G --> BI
    G --> GEN
```

### Health Score Formula

```
health_score = usage_score × 0.40
             + support_score × 0.30
             + growth_score × 0.15
             + relationship_score × 0.15
```

| Dimension | Signals | Weight |
|-----------|---------|--------|
| Usage | Active users %, feature breadth, session volume, login recency | 40% |
| Support | Ticket volume, critical tickets, resolution speed, CSAT | 30% |
| Growth | Contract status, payment status, renewal proximity | 15% |
| Relationship | Tenure, contract health | 15% |

**Churn Risk Bands:**
- `High` — health score < 40, OR payment overdue/failed, OR renewal ≤ 30 days with zero activity
- `Medium` — health score < 65, OR renewal ≤ 60 days, OR contract flagged At Risk
- `Low` — all other accounts

---

## Setup Steps

### Prerequisites

- Databricks workspace at `https://dbc-69445b27-9472.cloud.databricks.com`
- Unity Catalog enabled
- Permission to create catalogs (or use an existing one)
- A SQL Warehouse (Serverless recommended)
- Databricks CLI configured: `databricks configure --host https://dbc-69445b27-9472.cloud.databricks.com`

---

### Step 1 — Push to GitHub and sync to Databricks

```bash
git init
git add .
git commit -m "Initial commit: Customer Health Command Center"
git remote add origin https://github.com/archanainapudi/customer-health-command-center.git
git push -u origin main
```

In Databricks: **Repos → Add Repo** → paste the GitHub URL.
Path will be: `/Workspace/Repos/archanainapudi@gmail.com/customer-health-command-center`

---

### Step 2 — Run the SQL setup file

Open `sql/00_setup_catalog_and_permissions.sql` in the Databricks SQL editor.

Run the CREATE CATALOG and CREATE SCHEMA statements first:

```sql
CREATE CATALOG IF NOT EXISTS dn_saas_demo;
CREATE SCHEMA IF NOT EXISTS dn_saas_demo.bronze;
CREATE SCHEMA IF NOT EXISTS dn_saas_demo.silver;
CREATE SCHEMA IF NOT EXISTS dn_saas_demo.gold;
```

**Skip the GRANT statements for now** — the App SP client ID is not available
until after the app is created in Step 5.

---

### Step 3 — Run notebooks in order

Open each notebook in the Databricks workspace and run on a **Serverless** or
**Single Node** cluster (DBR 14.x+, Python 3.10+):

| Order | Notebook | Run time |
|-------|----------|----------|
| 1 | `notebooks/01_generate_synthetic_data.py` | ~90 seconds |
| 2 | `notebooks/02_bronze_to_silver.py` | ~60 seconds |
| 3 | `notebooks/03_silver_to_gold.py` | ~30 seconds |
| 4 | `notebooks/04_optional_churn_model.py` | Optional |

---

### Step 4 — Confirm gold tables exist

In the SQL editor:

```sql
SHOW TABLES IN dn_saas_demo.gold;

-- Expected output:
-- gold_account_health
-- gold_exec_kpis
-- gold_risk_segments

SELECT * FROM dn_saas_demo.gold.gold_exec_kpis;
```

---

### Step 5 — Deploy the Databricks App

**Option A — Databricks CLI**

```bash
databricks apps create \
  --name customer-health-command-center \
  --description "Customer Health Command Center — Streamlit"

databricks apps deploy customer-health-command-center \
  --source-code-path \
    /Workspace/Repos/archanainapudi@gmail.com/customer-health-command-center/app
```

**Option B — Databricks UI**

1. Navigate to **Apps → Create App**
2. Name: `customer-health-command-center`
3. Source: Workspace path to the `app/` subdirectory
4. After creation, add environment variable:
   `DATABRICKS_WAREHOUSE_HTTP_PATH` = `/sql/1.0/warehouses/<your-warehouse-id>`

---

### Step 6 — Configure permissions for the App service principal

After the app is created, find its service principal client ID:
**Apps → [your app] → Permissions or service principal details**

**Option A — Render and run the grant SQL**

```bash
python scripts/render_grants_template.py <APP_SERVICE_PRINCIPAL_CLIENT_ID>
```

Copy the output and run it in the Databricks SQL editor.

**Option B — Edit and run the template directly**

Open `sql/00_setup_catalog_and_permissions.sql`, replace
`<APP_SERVICE_PRINCIPAL_CLIENT_ID>` with the real value, and run the GRANT block.

---

## Serverless and Storage Note

> **This demo does not use `dbfs:/` writable paths.**
>
> All pipeline outputs are written to Unity Catalog managed Delta tables
> (`dn_saas_demo.bronze.*`, `dn_saas_demo.silver.*`, `dn_saas_demo.gold.*`).
>
> `dbfs:/` is not writable on Databricks Serverless compute and is not governed
> by Unity Catalog. Using it would break on Serverless clusters and produce
> data artifacts without lineage or access control. Every output in this demo
> is a governed, auditable, portable Unity Catalog table.

---

## App Service Principal Permissions

The Databricks App runs as a managed service principal. It needs:

| Permission | Object | How to grant |
|------------|--------|-------------|
| `USE CATALOG` | `dn_saas_demo` | SQL: `GRANT USE CATALOG ON CATALOG dn_saas_demo TO ...` |
| `USE SCHEMA` | `dn_saas_demo.gold` | SQL: `GRANT USE SCHEMA ON SCHEMA dn_saas_demo.gold TO ...` |
| `SELECT` | `gold_account_health` | SQL: `GRANT SELECT ON TABLE ... TO ...` |
| `SELECT` | `gold_exec_kpis` | SQL: `GRANT SELECT ON TABLE ... TO ...` |
| `SELECT` | `gold_risk_segments` | SQL: `GRANT SELECT ON TABLE ... TO ...` |
| `CAN USE` | SQL Warehouse | Databricks UI: SQL Warehouses → [warehouse] → Permissions |

> **Note:** `CAN USE` on the SQL warehouse cannot be granted via SQL DDL.
> It must be set through the Databricks UI or REST API.
> See `sql/00_setup_catalog_and_permissions.sql` for full details.

---

## Suggested Demo Flow (10–15 minutes)

| Time | Action |
|------|--------|
| 0:00 | 30-second opening — business problem and solution |
| 0:30 | Architecture overview — medallion diagram, three layers |
| 2:30 | Notebook 01 — show bronze tables in Unity Catalog Explorer |
| 4:00 | Notebook 03 — walk through the scoring logic in the gold notebook |
| 6:00 | Open the Streamlit app |
| 6:30 | Walk through KPI row and executive summary |
| 7:30 | Apply sidebar filters (High Risk, Enterprise) |
| 8:30 | Select the worst-scoring account in the detail panel |
| 9:30 | Walk through the three charts |
| 11:00 | Q&A — use the talk track in `docs/talk_track.md` |
| 13:00 | Crisp close |

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `Table not found: dn_saas_demo.bronze.bronze_accounts` | Run notebook 01 first |
| `Table not found: dn_saas_demo.silver.*` | Run notebook 02 |
| `Table not found: dn_saas_demo.gold.*` | Run notebook 03 |
| App shows "DATABRICKS_TOKEN is not set" | Deploy via Databricks Apps (not local) or set a PAT |
| App shows "SQL Warehouse HTTP path not configured" | Set `DATABRICKS_WAREHOUSE_HTTP_PATH` in app env vars |
| GRANT fails with "Principal not found" | App must be created first; SP exists after first deployment |
| `PermissionDenied` on gold table from app | Run the GRANT statements from Step 6; check CAN USE on warehouse |
| Notebook fails with "Cannot write to dbfs:/" | This repo uses Unity Catalog tables only — check for local env overrides |

---

## Backup Plan

If the app is unavailable during the interview, use the Databricks SQL editor:

```sql
-- Executive view
SELECT * FROM dn_saas_demo.gold.gold_exec_kpis;

-- High-risk accounts sorted by ARR
SELECT account_name, segment, arr, health_score,
       churn_risk_band, risk_reason, next_best_action
FROM dn_saas_demo.gold.gold_account_health
WHERE churn_risk_band = 'High'
ORDER BY arr DESC
LIMIT 20;

-- ARR at risk by segment
SELECT segment,
       SUM(CASE WHEN churn_risk_band = 'High'   THEN arr END) AS high_risk_arr,
       SUM(CASE WHEN churn_risk_band = 'Medium' THEN arr END) AS medium_risk_arr,
       ROUND(AVG(health_score), 1) AS avg_health_score
FROM dn_saas_demo.gold.gold_account_health
GROUP BY segment
ORDER BY high_risk_arr DESC NULLS LAST;
```

The story is identical — the app is a display layer on top of these queries.

---

*Contact: archanainapudi@gmail.com*
