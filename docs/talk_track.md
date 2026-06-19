# Talk Track — Customer Health Command Center
## Senior Solutions Architect Interview

---

## 30-Second Opening

> "I built a Customer Health Command Center on Databricks.
> The business scenario: a digital-native SaaS company has product telemetry,
> billing, and support data across three separate systems, and their CSMs
> can't answer the question 'which accounts will churn next quarter?'
> The demo unifies those signals in Unity Catalog, scores every account
> nightly, and surfaces the result through a Streamlit app deployed on
> Databricks Apps. Let me walk you through it."

---

## 2-Minute Architecture Overview

> "The architecture follows the medallion pattern in Unity Catalog.
>
> **Bronze** is the raw landing zone. In this demo it's synthetic — 500 accounts,
> 8,000 users, a quarter-million product events, subscriptions, and 12,000 support
> tickets. In production this layer would be populated by Fivetran connectors,
> Autoloader streams, or Kafka sinks. Bronze is append-only; we never mutate it.
>
> **Silver** is the trusted analytics layer. I clean types, join user activity, and
> compute 30-day rolling aggregates per account: active users, feature adoption,
> session depth, support burden, CSAT. All keyed on account_id. This is the layer
> that data engineers test and validate before anything downstream sees it.
>
> **Gold** is what the app reads. I join all silver signals into a composite health
> score using a weighted heuristic — usage 40%, support 30%, contract health 15%,
> relationship depth 15%. Gold also produces an executive KPI table and a
> risk-segment rollup. These three tables are all the app needs.
>
> **The app** is a Streamlit app deployed as a Databricks App. It queries gold
> through a Serverless SQL warehouse. The app service principal has SELECT on
> gold tables only — bronze and silver are invisible to it. Authentication is
> handled automatically by Databricks; the business user just opens a URL."

---

## Notebook Walkthroughs

### Notebook 01 — Generate Synthetic Data
> "This replaces your ingestion layer for the demo. I generate correlated data —
> riskier accounts produce fewer events, more P1 tickets, lower CSAT, payment issues.
> The data tells a consistent story so the health scores are meaningful.
> Everything writes to Unity Catalog managed Delta tables. No dbfs:/ paths.
> This runs in about 90 seconds."

### Notebook 02 — Bronze to Silver
> "Silver is where we earn the right to call data 'trusted.' I clean types,
> compute per-account 30-day windows, and aggregate five signals: user activity,
> feature adoption, support summary, subscription status, and daily usage.
> The AS_OF_DATE variable anchors all windows to the same reference point,
> which makes the demo reproducible regardless of when it runs."

### Notebook 03 — Silver to Gold
> "This is the scoring notebook. I join all five silver tables onto the
> account spine and compute four sub-scores. The composite is a weighted average.
> I also derive churn_risk_band in a CASE statement — High, Medium, Low — and
> generate risk_reason and next_best_action as human-readable strings.
> The reason those two columns exist is that the app displays them directly.
> A CSM should never need to decode a number; they should see a sentence."

### Notebook 04 — Optional ML
> "This notebook is not required for the app but it's there to show that
> Databricks handles both paths. I train a logistic regression and a random
> forest on the same signals, compare to the heuristic, and write ML
> churn probabilities to a separate gold table. The ROC-AUC on synthetic
> data is about 0.85, which is expected given the data generation process."

---

## App Walkthrough

### Opening
> "The app is live at the Databricks Apps URL. No login for the business user.
> The header tells you what you're looking at and where the data comes from."

### KPI Row
> "Four metrics at the top: total ARR, average health score, percent High Risk,
> and ARR at risk. These are filtered — watch them change when I adjust the sidebar."

### Executive Summary
> "The summary auto-generates from the filtered data. This is what you'd paste
> into a Monday morning Slack message to the VP of Customer Success."

### Sidebar Filters — demonstrate
> "I'll filter to High Risk only — Enterprise and Mid-Market segments."
> [Apply filters]
> "Notice the KPIs updated instantly. That's because we cache the full
> result set at load time and filter in memory. No additional warehouse query."

### Account Table
> "Sorted worst-first by health score. I can see the primary risk reason
> in plain English — not a model score, a sentence."

### Account Detail
> "I'll select the account with the lowest health score."
> [Select first account]
> "Zero active users in 30 days. Three open tickets. CSAT of 2.1.
> Renewal in 14 days. Payment overdue. Next best action: engage billing
> contact and escalate. That's a CSM's Monday morning call list."

### Charts
> "Risk by segment: Mid-Market has the most High Risk accounts by count.
> ARR at risk by plan: Starter tier churns fastest, but Enterprise
> accounts have the biggest revenue impact per event.
> The scatter makes the priority matrix obvious — low score, high ARR
> accounts are the top-right cluster."

---

## Likely Panel Questions and Strong Answers

**"Why not use Gainsight or ChurnZero for this?"**
> "Those tools are great for workflow — playbooks, health score inputs, CS task management.
> What they lack is a governed, unified data layer that pulls from multiple source systems
> without manual CSV uploads. This demo is the data foundation that makes those tools
> better — pipe the gold scores into Gainsight via API instead of having a CSM maintain them."

**"Why the heuristic instead of ML?"**
> "Two reasons. First, explainability. A CSM needs to be able to tell a customer
> 'your score dropped because you haven't logged in and your P1 ticket is unresolved.'
> A random forest coefficient doesn't support that conversation. Second, labels.
> ML churn models need historical churn labels. Day one of a new customer data platform,
> you don't have them. The heuristic works immediately and produces the labels the ML
> model needs later."

**"How does this scale beyond 500 accounts?"**
> "The silver aggregations are PySpark — they scale linearly with data volume.
> The gold scoring is a single SQL pass over silver. We tested this pattern
> at clients with 50,000 accounts and the pipeline runs in under 15 minutes
> on a 4-node cluster. For the app, Serverless SQL warehouse handles concurrent
> users with autoscaling — the CSM team hitting refresh simultaneously isn't
> a concern."

**"What if the source data is messy or missing?"**
> "The silver layer handles that explicitly. All left joins in the gold assembly
> step have COALESCE defaults — an account with no product events scores 0 on
> usage rather than NULL. Accounts with no CSAT response get a neutral default of 3.0.
> Missing data is itself a signal — an account with no events in 30 days should
> have a low usage score, and it does."

**"How would you productionise the pipeline?"**
> "Three steps. First, replace the synthetic bronze tables with real connectors —
> Fivetran for Stripe and Zendesk, Autoloader for event streams. Second, schedule
> the silver and gold notebooks as a Databricks Workflow, daily at 6am. Third,
> add data quality checks in silver using Great Expectations or Delta constraints
> to alert on schema drift or missing accounts. The app and gold model don't change."

**"Why Streamlit instead of Databricks AI/BI?"**
> "AI/BI is excellent for self-serve exploration by analytics users. This app is
> designed for CSMs who need a specific workflow — a prioritised call list,
> an account detail pane, and a next best action. Streamlit gives me precise
> control over that layout. Both are valid; they serve different users.
> I could also expose the gold table in AI/BI simultaneously — one table, two surfaces."

**"Why no dbfs:/ paths?"**
> "dbfs:/ is not writable on Serverless compute and is not governed by Unity Catalog.
> Using it would mean the pipeline breaks on Serverless and produces data artifacts
> with no lineage or access control. Every output in this demo writes to a Unity Catalog
> managed Delta table — governed, auditable, and portable across cluster types."

---

## Crisp Closing Statement

> "To summarise: one Databricks workspace, three notebooks, one app.
> A CSM opens the app Monday morning and knows which accounts to call,
> ranked by revenue at risk, with a plain-English reason and a next best action.
> The pipeline takes four minutes to run and the architecture is extensible —
> add a Genie space on the gold table for natural language queries, connect it
> to an AI agent for automated outreach drafts, or publish to AI/BI for
> self-serve dashboards. All without moving the data."

---

## Backup Plan (if the app is not ready)

If the Databricks App fails to deploy or the warehouse is not accessible:

**Option A — Show via Databricks SQL editor**
```sql
-- Open the SQL editor in the workspace and run these:

-- 1. Executive view
SELECT * FROM dn_saas_demo.gold.gold_exec_kpis;

-- 2. High-risk accounts sorted by ARR
SELECT account_name, segment, arr, health_score, churn_risk_band,
       risk_reason, next_best_action
FROM dn_saas_demo.gold.gold_account_health
WHERE churn_risk_band = 'High'
ORDER BY arr DESC
LIMIT 20;

-- 3. ARR at risk by plan tier
SELECT plan_tier, SUM(arr) AS arr_at_risk, COUNT(*) AS accounts
FROM dn_saas_demo.gold.gold_account_health
WHERE churn_risk_band IN ('High','Medium')
GROUP BY plan_tier
ORDER BY arr_at_risk DESC;
```

**Option B — Show in a notebook**
Open notebook 03 and `display()` the gold tables. The data is identical to the app;
the presentation is less polished but the story is the same.

**Option C — Show the architecture and talk through it**
The README has a Mermaid architecture diagram that renders on GitHub.
Walk through the diagram, the scoring logic, and the permission model.
A strong architecture explanation often impresses more than a live demo.
