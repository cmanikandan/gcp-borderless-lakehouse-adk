# Google Cloud Lakehouse and BigQuery for Real-Time Cross-Cloud Analytics

*How to query external data across Databricks and AWS Glue in seconds—using cross-cloud caching, BigQuery AI, Gemini 4, and Google Agent Development Kit with zero data movement.*

![Google Cloud Lakehouse cross-cloud architecture at a glance](https://raw.githubusercontent.com/cmanikandan/gcp-borderless-lakehouse-adk/main/docs/images/hero_borderless_lakehouse.png)

When an East Coast port bottleneck stalls freight on Monday morning, retail operations leaders have minutes—not hours—to spot which delayed shipments threaten their highest-margin customer orders and reroute inventory before customer commitments break. Yet in most enterprises, that single answer is trapped across three platforms: live orders in **BigQuery**, supplier margins in **Databricks Unity Catalog**, and carrier telemetry on **Amazon S3** behind **AWS Glue**—leaving teams stuck between **4–6 hour batch pipeline delays** or **~$31,104/month** in repeat cloud egress fees when AI agents query remote storage directly.

With the General Availability of [**cross-cloud federated catalogs** and **cross-cloud caching** in **Google Cloud Lakehouse**](https://docs.cloud.google.com/lakehouse/docs/release-notes) (formerly BigLake), you can join cross-cloud **Apache Iceberg** tables in seconds from **BigQuery**, protect margins in real time, and cut repeat cloud egress by **92%–98%** at **$0 additional Google Cloud cache fee**.

> **Proof-of-Concept Note:** This post and the [companion GitHub repository (`cmanikandan/gcp-borderless-lakehouse-adk`)](https://github.com/cmanikandan/gcp-borderless-lakehouse-adk) are an educational cloud practitioner proof of concept. Consult the official [Google Cloud Lakehouse](https://docs.cloud.google.com/lakehouse/docs/about-cross-cloud-data-access), [Databricks](https://docs.databricks.com/en/data-governance/unity-catalog/index.html), and [AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html) documentation before production deployment.

---

## Why Google Cloud Lakehouse Outperforms Batch Pipelines and Naive Federation

![Why Google Cloud Lakehouse outperforms legacy ETL and uncached federation](https://raw.githubusercontent.com/cmanikandan/gcp-borderless-lakehouse-adk/main/docs/images/lakehouse_vs_alternatives.png)

* **Zero-Copy Catalog Synchronization:** Automatically syncs Apache Iceberg metadata pointers from **Databricks Unity Catalog**, **AWS Glue**, **Snowflake Horizon**, and **Workday Data Lake**—zero data copies or remote cluster spin-up.
* **Permission-Verified Regional Caching at $0 Google Cloud Fee:** Caches hot Parquet columns inside your Google Cloud region while checking remote snapshots and access policies on every query so results are never stale.
* **Zero Static Keys and Unified Governance:** Vends short-lived storage tokens, propagates caller identities into Unity Catalog audit logs, and pairs with **Google Cloud Knowledge Catalog** lineage and **Apache Iceberg** row/column masking.
* **One Open Catalog for BigQuery, Apache Spark, and Gemini:** Powers **BigQuery**, in-warehouse **BigQuery AI** (`AI.GENERATE`, `AI.FORECAST`), **Google Cloud Managed Service for Apache Spark**, and **Google Agent Development Kit** agents from the same snapshots.

---

## Building the Cross-Cloud Supply Chain Solution

In our *OmniRetail Horizon* proof of concept, **Google Cloud Lakehouse** and **BigQuery** join three separate platforms in place without data movement (run `make install && make demo-finops` for a 30-second offline simulation, or run the full cloud bootstrap scripts in [`infra/`](https://github.com/cmanikandan/gcp-borderless-lakehouse-adk/tree/main/infra)):

| Business Dataset | Platform | Query Path in BigQuery |
| :--- | :--- | :--- |
| **Live Checkout Orders** | **Google Cloud BigQuery** (`us-east4`) | `` `omni-retail-lakehouse-poc.commerce_core.orders` `` |
| **Supplier Risk & Margins** | **Standalone Databricks** (`Unity Catalog` UniForm) | `` `omni-retail-lakehouse-poc.dbx_unity_merch.supply_chain.*` `` |
| **Carrier Delay Events** | **AWS Cloud** (`AWS Glue` + `Amazon S3`, `us-east-1`) | `` `omni-retail-lakehouse-poc.aws_glue_logistics.fulfillment.shipment_events` `` |

![OmniRetail Horizon proof-of-concept architecture](https://raw.githubusercontent.com/cmanikandan/gcp-borderless-lakehouse-adk/main/docs/images/gcp_lakehouse_architecture.png)

### 1. Federate AWS Glue and Standalone Databricks Unity Catalog into Google Cloud Lakehouse

[`setup_glue_federation.sh`](https://github.com/cmanikandan/gcp-borderless-lakehouse-adk/blob/main/infra/aws-glue/setup_glue_federation.sh) and [`setup_unity_federation.sh`](https://github.com/cmanikandan/gcp-borderless-lakehouse-adk/blob/main/infra/databricks-unity/setup_unity_federation.sh) register both external catalogs in `us-east4` with secretless OpenID Connect trust and regional caching enabled:

```bash
# 1. Federate AWS Glue (OpenID Connect trust, zero static keys)
gcloud biglake iceberg catalogs create aws_glue_logistics \
  --project="${PROJECT_ID}" --catalog-type="federated" --federated-catalog-type="glue" \
  --glue-warehouse="${AWS_ACCOUNT_ID}" --glue-aws-region="us-east-1" \
  --glue-aws-role-arn="arn:aws:iam::${AWS_ACCOUNT_ID}:role/lakehouse_glue_federation_role" \
  --cross-cloud-cache="enabled" --primary-location="us-east4"

# 2. Federate standalone Databricks Unity Catalog with end-user identity propagation
gcloud biglake iceberg catalogs create dbx_unity_merch \
  --project="${PROJECT_ID}" --catalog-type="federated" --federated-catalog-type="unity" \
  --unity-instance-name="dbc-a1b2c3d4-e5f6.cloud.databricks.com" \
  --unity-catalog-name="omniretail_merch" \
  --unity-service-principal-application-id="${DBX_SP_CLIENT_ID}" \
  --federated-catalog-identity-mode="user-identity-federation" \
  --cross-cloud-cache="enabled" --primary-location="us-east4"
```

![End-to-end user identity federation to Databricks Unity Catalog](https://raw.githubusercontent.com/cmanikandan/gcp-borderless-lakehouse-adk/main/docs/images/identity_federation_flow.png)

### 2. Join and Act in Place with BigQuery, BigQuery AI, and Apache Spark

Using **4-part table identifiers** (`` `project.catalog.namespace.table` ``), **BigQuery** joins all three platforms in one query and calls **`AI.GENERATE`** (`gemini-4-hydrogen`) to draft an immediate carrier expedite plan ([`05_cross_cloud_bigquery_ai_insights.sql`](https://github.com/cmanikandan/gcp-borderless-lakehouse-adk/blob/main/sql/05_cross_cloud_bigquery_ai_insights.sql)):

```sql
SELECT
  o.customer_region, m.category, r.supplier_name, s.carrier, s.delay_hours,
  ROUND(SUM(o.quantity * (o.unit_sale_price_usd - m.unit_cost_usd)), 2) AS gross_margin_exposure_usd,
  AI.GENERATE(
    CONCAT('Recommend an expedite action for ', m.category, ' on ', s.carrier,
           ' delayed ', CAST(s.delay_hours AS STRING), 'h.'),
    endpoint => 'gemini-4-hydrogen',
    output_schema => 'expedite_priority STRING, recommended_action STRING'
  ) AS action_plan
FROM `omni-retail-lakehouse-poc.commerce_core.orders` AS o
JOIN `omni-retail-lakehouse-poc.dbx_unity_merch.supply_chain.sku_margins` AS m ON o.sku_id = m.sku_id
JOIN `omni-retail-lakehouse-poc.dbx_unity_merch.supply_chain.supplier_risk_scores` AS r ON m.supplier_id = r.supplier_id
JOIN `omni-retail-lakehouse-poc.aws_glue_logistics.fulfillment.shipment_events` AS s ON o.order_id = s.order_id
WHERE o.order_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND (s.delay_hours > 12 OR r.geopolitical_risk_score >= 0.65)
GROUP BY 1, 2, 3, 4, 5
ORDER BY gross_margin_exposure_usd DESC;
```

![One open Iceberg catalog across BigQuery, BigQuery AI, and Apache Spark](https://raw.githubusercontent.com/cmanikandan/gcp-borderless-lakehouse-adk/main/docs/images/iceberg_rest_credential_vending.png)

### 3. Eliminate Repeat Egress with Google Cloud Lakehouse Caching

![How Google Cloud Lakehouse cross-cloud caching works](https://raw.githubusercontent.com/cmanikandan/gcp-borderless-lakehouse-adk/main/docs/images/intelligent_cache_flow.png)

Running [`run_cross_cloud_join.sh`](https://github.com/cmanikandan/gcp-borderless-lakehouse-adk/blob/main/scripts/run_cross_cloud_join.sh) with `--nouse_cache` verifies storage-level caching via `statistics.query.objectStorageStats`—serving **96.25% (`46.2 GB`)** locally from `us-east4`.

### 4. Automate Root-Cause Analysis with Gemini 4 and Google Agent Development Kit

![Tiered Gemini 4 multi-agent workflow with Google Agent Development Kit](https://raw.githubusercontent.com/cmanikandan/gcp-borderless-lakehouse-adk/main/docs/images/adk_multi_agent_cache_sequence.png)

In [`agent.py`](https://github.com/cmanikandan/gcp-borderless-lakehouse-adk/blob/main/src/lakehouse_agent/agent.py), **Google Agent Development Kit** matches each task to the right **Gemini 4** model—**`gemini-4-neon`** for fast routing, **`gemini-4-carbon`** for deep 4-part SQL & `AI.FORECAST`, and **`gemini-4-hydrogen`** for Flash-tier cache telemetry audits.

---

## Business Outcomes Achieved and Why This Solution Wins

On *OmniRetail Horizon*, the copilot pinpoints **$39,202 in exposed gross margin** across **$72,250 of delayed orders** in under 5 seconds:

| Business Metric | Legacy Batch Pipelines / Uncached Federation | Google Cloud Lakehouse + Gemini | Business Value Delivered |
| :--- | :--- | :--- | :--- |
| **Decision Speed** | `4–6 hours` (batch sync lag) | **`< 5 seconds`** (live 3-cloud join) | Reroute high-margin orders before customer commitments break |
| **30-Day Cloud Egress** | `$31,104/mo` (`345.6 TB` transferred) | **`$2,488/mo`** (`$553/mo` w/ Cross-Cloud Interconnect) | **92%–98.2% egress savings** at **$0 extra Google Cloud cache fee** |
| **Operational Overhead** | Fragile sync jobs + stored cloud keys | **`0` copy pipelines · `0` static keys** | Shift engineering hours from pipeline maintenance to AI insights |

* **Faster and leaner than batch copy pipelines:** Zero duplicate storage bills, zero broken sync schedules, and live visibility across clouds.
* **Smarter than naive query federation:** Regional Parquet caching absorbs iterative business intelligence and AI query bursts at local speed while verifying permissions on every read—and pairs with **BigQuery Project Caps** to bound slot usage.
* **Open by design:** Data stays in open **Apache Iceberg** formats across AWS, Databricks, Snowflake, Workday, **BigQuery**, and **Apache Spark**.

*(When finished testing the proof of concept, run `bash infra/teardown_all.sh` to remove all cloud resources cleanly.)*

---

## Where Else This Works and Next Steps

* **Post-Merger Financial Close (`Snowflake Horizon` + `Databricks` + `BigQuery`):** Reconcile acquired billing ledgers and core finance actuals on day one without a 6-month migration.
* **Workforce and Fulfillment Planning (`Workday Data Lake` + `AWS Glue` + `BigQuery`):** Match live shipment volume against shift staffing while keeping human-resources data masked in place.
* **Autonomous Cyber Threat Hunting (`Amazon S3` + `BigQuery` + `Gemini 4`):** Investigate petabytes of cloud logs with **Gemini 4 Carbon** at **$0 repeat egress** on follow-up pivot queries.
* **Customer 360 and Demand Forecasting (`Apache Spark` + `BigQuery AI`):** Combine PySpark clickstream cohorts with zero-shot **`AI.FORECAST`** demand planning.
* **Regulated Clinical Research (`Databricks Unity Catalog` + `BigQuery`):** Analyze multi-site cohorts with per-researcher row masking and end-to-end user audit trails.

### Get Started
1. **Run the 30-second local simulator:** Clone [`cmanikandan/gcp-borderless-lakehouse-adk`](https://github.com/cmanikandan/gcp-borderless-lakehouse-adk) and run `make install && make test && make demo-finops`.
2. **Deploy the cross-cloud proof of concept:** Follow [`README.md`](https://github.com/cmanikandan/gcp-borderless-lakehouse-adk#readme) and run `bash infra/teardown_all.sh` when done.

### Key Links
* [Google Cloud Lakehouse Release Notes](https://docs.cloud.google.com/lakehouse/docs/release-notes) · [Cross-Cloud Caching](https://docs.cloud.google.com/lakehouse/docs/about-cross-cloud-data-access#intelligent-caching) · [Databricks Setup](https://docs.cloud.google.com/lakehouse/docs/set-up-cross-cloud-connection-databricks) · [AWS Glue Setup](https://docs.cloud.google.com/lakehouse/docs/set-up-cross-cloud-connection-aws-glue) · [Snowflake Horizon](https://docs.cloud.google.com/lakehouse/docs/set-up-cross-cloud-connection-snowflake) · [Workday Data Lake](https://docs.cloud.google.com/lakehouse/docs/set-up-cross-cloud-connection-workday) · [BigQuery AI](https://docs.cloud.google.com/bigquery/docs/generative-ai-overview) · [Google Agent Development Kit for BigQuery](https://adk.dev/integrations/bigquery/)
