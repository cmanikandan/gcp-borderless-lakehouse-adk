-- =============================================================================
-- Seed Native Google Cloud BigQuery E-Commerce Orders Dataset (us-east4)
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS `omni-retail-lakehouse-prod.commerce_core`
OPTIONS (location = 'us-east4');

CREATE OR REPLACE TABLE `omni-retail-lakehouse-prod.commerce_core.orders` (
  order_id STRING NOT NULL,
  order_ts TIMESTAMP NOT NULL,
  customer_region STRING NOT NULL,
  sku_id STRING NOT NULL,
  quantity INT64 NOT NULL,
  unit_sale_price_usd FLOAT64 NOT NULL,
  promised_delivery_date DATE NOT NULL
)
PARTITION BY DATE(order_ts)
CLUSTER BY customer_region, sku_id;

INSERT INTO `omni-retail-lakehouse-prod.commerce_core.orders` VALUES
  ('ORD-8801', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR), 'US-NORTHEAST', 'SKU-9001', 120, 149.00, CURRENT_DATE() + 3),
  ('ORD-8802', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 MINUTE), 'US-NORTHEAST', 'SKU-9003', 340, 118.00, CURRENT_DATE() + 2),
  ('ORD-8803', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 45 MINUTE), 'US-MIDWEST', 'SKU-9002', 85, 68.00, CURRENT_DATE() + 4),
  ('ORD-8804', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 MINUTE), 'US-WEST', 'SKU-9004', 1500, 9.50, CURRENT_DATE() + 5);
