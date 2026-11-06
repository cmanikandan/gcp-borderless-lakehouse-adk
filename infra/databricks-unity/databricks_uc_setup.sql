-- =============================================================================
-- Databricks Unity Catalog Preparation (Standalone Databricks Portal / Free Edition)
-- Run in the Databricks SQL Editor directly inside your Databricks Portal!
-- =============================================================================

-- ARCHITECT IMPORTANT NOTE (Databricks Free / Community Edition & Enterprise):
-- 1. Enable "External data access" in your Databricks Unity Catalog Metastore settings.
-- 2. Google Cloud Lakehouse requires Unity Catalog schemas/tables to be backed by a
--    registered Unity Catalog External Location (e.g., an external object bucket URI
--    configured under Catalog -> External Data -> External Locations), NOT Databricks
--    internal default metastore storage.
CREATE CATALOG IF NOT EXISTS omniretail_merch;

CREATE SCHEMA IF NOT EXISTS omniretail_merch.supply_chain
MANAGED LOCATION '<YOUR_UNITY_CATALOG_EXTERNAL_LOCATION_URI>/supply_chain/';

-- 1. Supplier Risk Scores (Delta with Apache Iceberg UniForm metadata enabled)
CREATE OR REPLACE TABLE omniretail_merch.supply_chain.supplier_risk_scores (
  supplier_id STRING NOT NULL,
  supplier_name STRING NOT NULL,
  tier STRING,
  country_code STRING,
  on_time_rate DOUBLE,
  geopolitical_risk_score DOUBLE,
  lead_time_days INT,
  updated_at TIMESTAMP
)
TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.enableIcebergCompatV2' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

INSERT INTO omniretail_merch.supply_chain.supplier_risk_scores VALUES
  ('SUP-101', 'Apex MicroSystems', 'Tier-1', 'TW', 0.94, 0.78, 21, CURRENT_TIMESTAMP()),
  ('SUP-204', 'Nordic ColdChain Logistics', 'Tier-1', 'SE', 0.99, 0.14, 7, CURRENT_TIMESTAMP()),
  ('SUP-309', 'Pacific Rim Textiles', 'Tier-2', 'VN', 0.81, 0.69, 28, CURRENT_TIMESTAMP()),
  ('SUP-412', 'Great Lakes Packaging', 'Tier-1', 'US', 0.97, 0.11, 4, CURRENT_TIMESTAMP());

-- 2. SKU Margin & Cost Tiers (Governed by Databricks Unity Catalog)
CREATE OR REPLACE TABLE omniretail_merch.supply_chain.sku_margins (
  sku_id STRING NOT NULL,
  product_name STRING NOT NULL,
  category STRING,
  supplier_id STRING NOT NULL,
  unit_cost_usd DOUBLE,
  target_gross_margin_pct DOUBLE
)
TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.enableIcebergCompatV2' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

INSERT INTO omniretail_merch.supply_chain.sku_margins VALUES
  ('SKU-9001', 'Pro Studio Wireless Dock', 'Electronics', 'SUP-101', 84.50, 0.42),
  ('SKU-9002', 'Insulated Thermal Carrier', 'Home & Outdoor', 'SUP-204', 29.00, 0.55),
  ('SKU-9003', 'Merino Performance Jacket', 'Apparel', 'SUP-309', 46.20, 0.60),
  ('SKU-9004', 'EcoSmart Shipping Tote', 'Packaging', 'SUP-412', 4.80, 0.48);

-- 3. Grant Base Metadata Sync & Credential Vending Privileges to Service Principal
-- CRITICAL: EXTERNAL USE SCHEMA is mandatory so Unity Catalog vends short-lived tokens!
GRANT USE CATALOG ON CATALOG omniretail_merch TO `<SERVICE_PRINCIPAL_APP_ID>`;
GRANT USE SCHEMA, SELECT, EXTERNAL USE SCHEMA
  ON SCHEMA omniretail_merch.supply_chain TO `<SERVICE_PRINCIPAL_APP_ID>`;

-- 4. (Optional) Grant Interactive Query Privileges for End-User Identity Federation
GRANT USE CATALOG ON CATALOG omniretail_merch TO `supply_chain_analysts`;
GRANT USE SCHEMA, SELECT, EXTERNAL USE SCHEMA
  ON SCHEMA omniretail_merch.supply_chain TO `supply_chain_analysts`;
