-- =============================================================================
-- Customer Health Command Center
-- Catalog, Schema, and Permission Setup
-- Workspace: https://dbc-69445b27-9472.cloud.databricks.com
-- =============================================================================
-- Run this file once as a metastore admin or catalog owner before running
-- any notebooks. It is safe to re-run (all statements use IF NOT EXISTS).
--
-- PLACEHOLDER: Replace <APP_SERVICE_PRINCIPAL_CLIENT_ID> with the actual
-- client ID of the Databricks App service principal AFTER the app is created.
-- You can find it in: Databricks workspace > Apps > [your app] > Permissions,
-- or in the service principal list under Settings > Identity & Access.
-- =============================================================================


-- ---------------------------------------------------------------------------
-- 1. Catalog
-- ---------------------------------------------------------------------------

CREATE CATALOG IF NOT EXISTS dn_saas_demo
  COMMENT 'Customer Health Command Center demo catalog — bronze / silver / gold';

USE CATALOG dn_saas_demo;


-- ---------------------------------------------------------------------------
-- 2. Schemas (medallion layers)
-- ---------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS dn_saas_demo.bronze
  COMMENT 'Raw ingested data — accounts, users, events, subscriptions, tickets';

CREATE SCHEMA IF NOT EXISTS dn_saas_demo.silver
  COMMENT 'Cleansed and aggregated per-account signals (30-day windows)';

CREATE SCHEMA IF NOT EXISTS dn_saas_demo.gold
  COMMENT 'Business-ready health scores, KPIs, and risk segments — app-facing';


-- ---------------------------------------------------------------------------
-- 3. Grants for the Databricks App service principal
-- ---------------------------------------------------------------------------
-- Replace <APP_SERVICE_PRINCIPAL_CLIENT_ID> with the real value before running.
-- The placeholder format uses backticks because Databricks GRANT syntax
-- expects the principal as an identifier.
-- ---------------------------------------------------------------------------

-- Allow the app SP to see the catalog
GRANT USE CATALOG ON CATALOG dn_saas_demo
  TO `<APP_SERVICE_PRINCIPAL_CLIENT_ID>`;

-- Allow the app SP to see each schema
GRANT USE SCHEMA ON SCHEMA dn_saas_demo.bronze
  TO `<APP_SERVICE_PRINCIPAL_CLIENT_ID>`;

GRANT USE SCHEMA ON SCHEMA dn_saas_demo.silver
  TO `<APP_SERVICE_PRINCIPAL_CLIENT_ID>`;

GRANT USE SCHEMA ON SCHEMA dn_saas_demo.gold
  TO `<APP_SERVICE_PRINCIPAL_CLIENT_ID>`;

-- Allow the app SP to read app-facing gold tables/views
-- The app only queries gold; bronze and silver are pipeline-internal.
GRANT SELECT ON TABLE dn_saas_demo.gold.gold_account_health
  TO `<APP_SERVICE_PRINCIPAL_CLIENT_ID>`;

GRANT SELECT ON TABLE dn_saas_demo.gold.gold_exec_kpis
  TO `<APP_SERVICE_PRINCIPAL_CLIENT_ID>`;

GRANT SELECT ON TABLE dn_saas_demo.gold.gold_risk_segments
  TO `<APP_SERVICE_PRINCIPAL_CLIENT_ID>`;

-- NOTE: The app service principal also needs CAN USE on the SQL warehouse.
-- This is typically granted through the Databricks UI or API rather than SQL:
--   Databricks workspace > SQL Warehouses > [warehouse] > Permissions
--   Add the service principal with "Can use" permission.
-- There is no SQL DDL syntax for warehouse permissions in Unity Catalog.


-- ---------------------------------------------------------------------------
-- 4. Validation — confirm grants were applied
-- ---------------------------------------------------------------------------

SHOW GRANTS ON CATALOG dn_saas_demo;

SHOW GRANTS ON SCHEMA dn_saas_demo.gold;

-- Run these after the notebooks have created the tables:
-- SHOW GRANTS ON TABLE dn_saas_demo.gold.gold_account_health;
-- SHOW GRANTS ON TABLE dn_saas_demo.gold.gold_exec_kpis;
-- SHOW GRANTS ON TABLE dn_saas_demo.gold.gold_risk_segments;
