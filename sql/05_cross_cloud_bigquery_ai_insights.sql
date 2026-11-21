-- =============================================================================
-- In-Warehouse Gemini 4 Enrichment (`AI.GENERATE`) & Zero-Shot `AI.FORECAST`
-- Directly Over Cross-Cloud Federated Apache Iceberg Tables (Nov 2026 GA)
--
-- Showcases two Google Data Cloud superpowers on top of Lakehouse federation:
--   1. Structured `AI.GENERATE` using `gemini-4-hydrogen` (`output_schema`)
--      over a 3-platform join (BigQuery + standalone Databricks + AWS Glue)
--      with warm regional Parquet cache eliminating repeat S3 egress.
--   2. Zero-shot `AI.FORECAST` (TimesFM) projecting carrier delay hours across
--      federated AWS Glue shipment series without training custom ML models.
-- =============================================================================

-- Part A: Structured Supply Chain Mitigation Briefs via `AI.GENERATE` (`gemini-4-hydrogen`)
WITH cross_cloud_exposure AS (
  SELECT
    o.customer_region,
    m.category,
    m.product_name,
    r.supplier_name,
    r.geopolitical_risk_score,
    s.carrier,
    s.delay_hours,
    ROUND(
      SUM(o.quantity * (o.unit_sale_price_usd - m.unit_cost_usd)),
      2
    ) AS gross_margin_exposure_usd
  FROM
    `omni-retail-lakehouse-prod.commerce_core.orders` AS o
  INNER JOIN
    `omni-retail-lakehouse-prod.dbx_unity_merch.supply_chain.sku_margins` AS m
    ON o.sku_id = m.sku_id
  INNER JOIN
    `omni-retail-lakehouse-prod.dbx_unity_merch.supply_chain.supplier_risk_scores` AS r
    ON m.supplier_id = r.supplier_id
  INNER JOIN
    `omni-retail-lakehouse-prod.aws_glue_logistics.fulfillment.shipment_events` AS s
    ON o.order_id = s.order_id
  WHERE
    o.order_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    AND (s.delay_hours > 12 OR r.geopolitical_risk_score >= 0.65)
  GROUP BY
    1, 2, 3, 4, 5, 6, 7
)
SELECT
  customer_region,
  category,
  supplier_name,
  carrier,
  delay_hours,
  gross_margin_exposure_usd,
  AI.GENERATE(
    CONCAT(
      'Generate a concise supply chain mitigation plan for ',
      category, ' (', product_name, ') from supplier ', supplier_name,
      ' (geopolitical risk=', CAST(geopolitical_risk_score AS STRING),
      ', carrier=', carrier, ', delay_hours=', CAST(delay_hours AS STRING),
      ', gross_margin_exposure_usd=$', CAST(gross_margin_exposure_usd AS STRING), ').'
    ),
    endpoint => 'gemini-4-hydrogen',
    output_schema => 'expedite_priority STRING, recommended_carrier_action STRING, executive_note STRING'
  ) AS mitigation_plan
FROM
  cross_cloud_exposure
ORDER BY
  gross_margin_exposure_usd DESC;

-- Part B: Zero-Shot Time-Series Forecasting (`AI.FORECAST` / TimesFM) Over Federated AWS Glue Iceberg Data
SELECT
  carrier,
  forecast_timestamp,
  ROUND(forecast_value, 2) AS projected_avg_delay_hours,
  ROUND(prediction_interval_lower_bound, 2) AS lower_bound_hours,
  ROUND(prediction_interval_upper_bound, 2) AS upper_bound_hours
FROM
  AI.FORECAST(
    (
      SELECT
        TIMESTAMP_TRUNC(event_ts, DAY) AS event_day,
        carrier,
        AVG(delay_hours) AS avg_delay_hours
      FROM
        `omni-retail-lakehouse-prod.aws_glue_logistics.fulfillment.shipment_events`
      GROUP BY
        event_day, carrier
    ),
    data_col => 'avg_delay_hours',
    timestamp_col => 'event_day',
    id_cols => ['carrier'],
    horizon => 7,
    confidence_level => 0.90
  )
ORDER BY
  carrier, forecast_timestamp;
