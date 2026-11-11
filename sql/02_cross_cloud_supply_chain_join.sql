-- =============================================================================
-- Zero-ETL Cross-Cloud Join: BigQuery + Databricks Unity Catalog + AWS Glue
-- Federated Iceberg tables use 4-part paths: `project.catalog.namespace.table`
-- =============================================================================
SELECT
  o.customer_region,
  m.category,
  m.product_name,
  r.supplier_name,
  r.geopolitical_risk_score,
  s.carrier,
  s.shipment_status,
  s.delay_hours,
  COUNT(DISTINCT o.order_id) AS affected_orders,
  ROUND(SUM(o.quantity * o.unit_sale_price_usd), 2) AS gross_revenue_at_risk_usd,
  ROUND(
    SUM(o.quantity * (o.unit_sale_price_usd - m.unit_cost_usd)),
    2
  ) AS gross_margin_exposure_usd
FROM
  -- 1. Native Google Cloud BigQuery table (us-east4)
  `omni-retail-lakehouse-prod.commerce_core.orders` AS o
INNER JOIN
  -- 2. Federated Databricks Unity Catalog (Standalone Databricks Portal + Local Cache)
  `omni-retail-lakehouse-prod.dbx_unity_merch.supply_chain.sku_margins` AS m
  ON o.sku_id = m.sku_id
INNER JOIN
  `omni-retail-lakehouse-prod.dbx_unity_merch.supply_chain.supplier_risk_scores` AS r
  ON m.supplier_id = r.supplier_id
INNER JOIN
  -- 3. Federated AWS Glue Data Catalog on Amazon S3 / S3 Tables (OIDC + Local Cache)
  `omni-retail-lakehouse-prod.aws_glue_logistics.fulfillment.shipment_events` AS s
  ON o.order_id = s.order_id
WHERE
  o.order_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND (s.delay_hours > 12 OR r.geopolitical_risk_score >= 0.65)
GROUP BY
  1, 2, 3, 4, 5, 6, 7, 8
ORDER BY
  gross_margin_exposure_usd DESC;
