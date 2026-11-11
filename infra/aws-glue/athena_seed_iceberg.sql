-- =============================================================================
-- Amazon Athena Engine v3 SQL — Create & Seed Apache Iceberg Table on Amazon S3
-- Executed automatically by `infra/aws-glue/00_bootstrap_aws_s3_glue_iceberg.sh`
-- Uses Iceberg v2 MERGE INTO so re-running is 100% idempotent (zero duplicate rows).
-- (POC Note: Refer to official AWS Glue & Athena Iceberg docs for production use)
-- =============================================================================

-- Statement 1: Create Apache Iceberg v2 table in AWS Glue Data Catalog (`fulfillment`)
CREATE TABLE IF NOT EXISTS fulfillment.shipment_events (
  event_id string,
  order_id string,
  sku_id string,
  carrier string,
  origin_port string,
  shipment_status string,
  delay_hours int,
  event_ts timestamp
)
LOCATION 's3://${S3_ICEBERG_BUCKET}/warehouse/fulfillment/shipment_events/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet',
  'write_compression' = 'zstd'
);

-- Statement 2: Idempotent upsert via Iceberg v2 MERGE INTO
MERGE INTO fulfillment.shipment_events AS target
USING (
  VALUES
    ('SHP-5001', 'ORD-8801', 'SKU-9001', 'OceanicExpress', 'KAOHSIUNG', 'PORT_CONGESTION_HOLD', 28, current_timestamp),
    ('SHP-5002', 'ORD-8802', 'SKU-9003', 'PacificAirCargo', 'HO_CHI_MINH', 'CUSTOMS_INSPECTION_DELAY', 19, current_timestamp),
    ('SHP-5003', 'ORD-8803', 'SKU-9002', 'NordicFreight', 'GOTHENBURG', 'IN_TRANSIT_ON_TIME', 2, current_timestamp),
    ('SHP-5004', 'ORD-8804', 'SKU-9004', 'GreatLakesRail', 'CHICAGO', 'WEATHER_REROUTE_DELAY', 36, current_timestamp)
) AS source (event_id, order_id, sku_id, carrier, origin_port, shipment_status, delay_hours, event_ts)
ON target.event_id = source.event_id
WHEN MATCHED THEN UPDATE SET
  shipment_status = source.shipment_status,
  delay_hours = source.delay_hours,
  event_ts = source.event_ts
WHEN NOT MATCHED THEN INSERT (
  event_id, order_id, sku_id, carrier, origin_port, shipment_status, delay_hours, event_ts
) VALUES (
  source.event_id, source.order_id, source.sku_id, source.carrier, source.origin_port,
  source.shipment_status, source.delay_hours, source.event_ts
);
