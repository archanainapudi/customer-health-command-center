# Demo Story — Customer Health Command Center

## The Business Problem

A digital-native B2B SaaS company is growing fast. They have 500 accounts, a Customer Success team of 12 people, and a renewal book of $28M ARR. They also have a churn problem nobody can quantify.

The issue is not a lack of data — it's a lack of *unified* data:

- Product telemetry lives in a Kafka-backed event store or a Snowplow-style collector
- Subscription and billing records are in Stripe or a billing system
- Support tickets are in Zendesk or Intercom
- NPS responses come in through Delighted
- Nobody has connected these signals

The result: CSMs manage by instinct and relationship. Renewals are surprises. Finance sees ARR drop and asks why nobody flagged the account. The VP of Customer Success runs a weekly spreadsheet exercise that is obsolete by Tuesday.

This demo shows how Databricks solves that problem end-to-end.

---

## Why Digital-Native SaaS is a Strong Databricks Use Case

Digital-native SaaS companies have three properties that make Databricks a natural fit:

**1. Data already flows through cloud infrastructure.**
Product events, subscription changes, and support tickets are API-native. They land in object storage (S3/GCS/ADLS) through connectors or Kafka sinks with minimal effort. Databricks Autoloader and Delta tables handle the rest.

**2. The business question is a join problem.**
"Is this account healthy?" requires combining three or more data sources that no single operational system owns. That is exactly what a lakehouse is designed to do.

**3. Speed of iteration matters.**
A Customer Success team iterates on health score weights monthly. A data science team wants to experiment with churn models. A new CSM wants a different filter. All of these are configuration changes in the gold notebook — not re-architecting a pipeline.

---

## Why Unity Catalog + Medallion + App is the Right Architecture

### Unity Catalog
Single governance layer across all data. The app service principal has SELECT on gold tables only — bronze and silver are pipeline-internal. Fine-grained access control, full audit trail, no shadow copies of data.

### Medallion (Bronze / Silver / Gold)
Each layer has a clear contract:

| Layer | Guarantee | Who touches it |
|-------|-----------|----------------|
| Bronze | Faithful replica of source. Never mutated after landing. | Ingestion pipelines only |
| Silver | Clean types. Business-meaningful aggregates. 30-day windows. Trusted. | Analytics engineers |
| Gold | Business-ready. App-facing. Joinable by account_id. Explainable. | App, BI, AI agents |

This separation means: when a CSM asks "where does that number come from?" we can trace it from the app metric back through gold → silver → bronze → raw event in under five minutes.

### Databricks App
Deployed as a Databricks App means:
- No external server to provision or maintain
- Authentication handled by Databricks (short-lived OAuth token per request)
- The app SP's permissions are Unity Catalog-enforced
- The business user sees a URL — no Databricks knowledge required

---

## Why No dbfs:/ Writable Paths

Databricks File System (dbfs:/) mount points are not writable on Serverless compute. They also create governance ambiguity — a file on DBFS is not tracked by Unity Catalog, has no lineage, and cannot have column-level access control.

This demo uses only Unity Catalog managed Delta tables for all persistence. That means:
- Every table has a Unity Catalog owner
- Every SELECT is audited
- Every column can be masked or governed independently
- The demo works on Serverless clusters, Classic clusters, and SQL warehouses without modification

---

## Why Explainable Heuristics Over ML for a Live Demo

The gold scoring model is a weighted average of four sub-scores. This is a deliberate choice, not a limitation.

**In a live interview, you need to narrate a number.**

"This account scores 34. Usage sub-score is 8 — they had zero active users in the last 30 days. Support sub-score is 41 — two P1 tickets unresolved. Growth sub-score is 30 — renewal is 18 days out and payment is overdue."

You cannot do that with a random forest coefficient vector.

**Heuristics are also production-valid in this context.** Customer health scoring is a business process, not a pure prediction task. CSMs need to understand and trust the score. An opaque ML model that achieves 87% AUC but can't explain why a $500K account is flagged as High Risk is operationally useless.

**The optional notebook 04 adds ML** to show that Databricks is the right platform for both — a lightweight logistic regression and random forest trained on the same signals, with feature importance for stakeholder conversations. But the app works entirely without it.

---

## The Story in One Paragraph

*A digital-native SaaS company has product, billing, and support data spread across three systems. They can't answer "which accounts will churn next quarter?" We land all three into Unity Catalog bronze tables, aggregate them into per-account 30-day signals in silver, and score every account on four dimensions in gold. A Databricks App surfaces the result for the Customer Success team every morning. The pipeline runs in under 4 minutes on Serverless compute. The app requires no Databricks login. The scoring model is explainable to any CSM. And the architecture is extensible — swap synthetic data for real connectors, add Genie for natural language, or publish the gold table to an AI/BI dashboard without touching anything else.*
