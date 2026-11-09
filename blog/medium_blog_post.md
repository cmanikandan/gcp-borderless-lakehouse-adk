# Zero-ETL Meets Zero-Egress Shock: A Hands-On Google Cloud Lakehouse POC with Databricks, AWS Glue, and Gemini 3.8 Flash

*A cloud practitioner’s end-to-end Proof of Concept exploring Google Cloud Lakehouse’s newly GA Cross-Cloud Federated Catalogs, Intelligent Caching, and multi-agent analytics with Google ADK.*

![Google Cloud Borderless Lakehouse Overview](../docs/images/hero_borderless_lakehouse.jpg)

> **Proof-of-Concept (POC) Disclaimer**: Everything in this article and the [companion GitHub repository](https://github.com/cmanikandan/gcp-borderless-lakehouse-adk) is an educational **Proof of Concept (POC)** built from a cloud practitioner’s perspective to test cross-cloud capabilities hands-on. Before building production workloads, always refer to the official [**Google Cloud Lakehouse**](https://docs.cloud.google.com/lakehouse/docs/about-cross-cloud-data-access), [**Databricks Unity Catalog**](https://docs.databricks.com/en/data-governance/unity-catalog/index.html), and [**AWS Glue & S3**](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html) documentation for security hardening, IAM boundaries, networking, and production limits.

---

## Why Google Cloud Lakehouse Changes the Multi-Cloud Equation

In almost every modern organization, data is already distributed across multiple platforms—and migrating petabytes into a single cloud before you can run analytics or AI rarely makes business sense. Yet querying across clouds has historically forced an unpleasant choice: maintain fragile hourly ETL copy pipelines, or suffer slow cross-cloud scans and steep storage egress bills.

**Google Cloud Lakehouse** (built on BigLake and BigQuery) tackles this head-on by combining an open table foundation with cloud-spanning governance and caching. Before we build our POC, it helps to understand the **five core advantages** that set Google Cloud Lakehouse apart:

1. **Open-Format Interoperability Without Lock-In**: Google Cloud Lakehouse treats **Apache Iceberg** (as well as Delta Lake tables with Iceberg UniForm enabled) as a first-class citizen via an open Iceberg REST Catalog. The exact same table snapshots can be queried simultaneously by **BigQuery**, **Google Cloud Managed Service for Apache Spark**, and open-source **Apache Spark**—with zero data duplication.
2. **Zero-ETL Cross-Cloud Federated Catalogs (Now GA)**: Instead of moving data, Lakehouse connects directly to external catalogs—**Databricks Unity Catalog**, **AWS Glue Data Catalog**, **Snowflake Horizon Catalog**, and **Workday Data Lake**—automatically synchronizing Iceberg table metadata in the background so external tables are queryable in place.
3. **Intelligent Cross-Cloud Caching (Now GA)**: Disabled by default (`--cross-cloud-cache="disabled"`), opting into caching stores active remote Parquet/Iceberg data blocks locally inside your Google Cloud region. Cold reads fetch only required column blocks; subsequent BI and AI queries read from regional cache at local speed and **\$0 repeat remote egress**.
4. **Unified Security, Credential Vending & End-User Identity**: Compute engines never hold static AWS keys or long-lived tokens—Lakehouse vends short-lived, scoped storage credentials automatically. Traffic can route over TLS 1.3 or privately over **Cross-Cloud Interconnect** inside **VPC Service Controls** perimeters, and for **Databricks Unity Catalog**, **User Identity Federation** propagates end-user OIDC identities straight into Databricks row/column policies and audit logs.
5. **An AI- & Agent-Ready Substrate**: Because federated tables appear natively in BigQuery, teams can attach **Google Agent Development Kit (`google-adk`)** agents and **`gemini-3.8-flash`** directly to cross-platform datasets—while Intelligent Caching absorbs the iterative SQL loops that AI agents generate.

Both [**Cross-Cloud Federated Catalogs** and **Intelligent Cross-Cloud Caching** are now Generally Available (GA)](https://docs.cloud.google.com/lakehouse/docs/release-notes). Let’s put them to the test in a complete, from-scratch POC.

---

## The POC Use Case: *OmniRetail Horizon* Margin-at-Risk

Suppose a global retailer faces sudden port congestion and supplier lead-time spikes. Analysts need real-time **Gross Margin Exposure** across three separate platforms—without building a single ETL pipeline:

* **Google Cloud (`us-east4`)**: Live customer orders in native BigQuery (`commerce_core.orders`).
* **Standalone Databricks Platform**: Supplier geopolitical risk scores and SKU cost margins governed in **Databricks Unity Catalog** (`omniretail_merch.supply_chain.*`), accessed directly via the standalone **Databricks Portal** (*Databricks Free / Community Edition* or standard workspace).
* **AWS Cloud (`us-east-1`)**: Carrier shipment delay events stored as Apache Iceberg on Amazon S3 and registered in **AWS Glue Data Catalog** (`fulfillment.shipment_events`).

![End-to-End Cross-Cloud Lakehouse Architecture](../docs/images/gcp_drawio_architecture.jpg)

> **Companion GitHub Repo (Zero-to-One Scripts Included)**: Clone [**github.com/cmanikandan/gcp-borderless-lakehouse-adk**](https://github.com/cmanikandan/gcp-borderless-lakehouse-adk) for every bootstrap script, SQL query, PySpark job, Google ADK agent, and one-command teardown script shown below.

---

## Step-by-Step POC Implementation & Testing

Don’t worry if you haven’t set up AWS Glue, Databricks Unity Catalog, or BigQuery yet—the repository includes bootstrap scripts for every platform.

### Step 1: Provision & Federate AWS Glue + S3 Iceberg (Secretless OIDC)

First, [`infra/aws-glue/00_bootstrap_aws_s3_glue_iceberg.sh`](../infra/aws-glue/00_bootstrap_aws_s3_glue_iceberg.sh) creates an encrypted S3 bucket in `us-east-1`, creates the `fulfillment` Glue database, and uses serverless **Amazon Athena Engine v3** (no EC2/EMR clusters needed) to create and seed the `fulfillment.shipment_events` Apache Iceberg table.

Next, [`infra/aws-glue/setup_glue_federation.sh`](../infra/aws-glue/setup_glue_federation.sh) connects Google Cloud Lakehouse to AWS Glue using **OpenID Connect (OIDC)**—no static AWS access keys required:

```bash
# 1. Bootstrap S3 bucket + Glue DB + seeded Apache Iceberg table from scratch
export PROJECT_ID="your-gcp-project-id"
export GCP_REGION="us-east4"
export AWS_REGION="us-east-1"
bash infra/aws-glue/00_bootstrap_aws_s3_glue_iceberg.sh

# 2. Create the federated catalog in Google Cloud with Intelligent Caching enabled
# (Tip: --glue-warehouse takes your 12-digit AWS Account ID, NOT an s3:// URI!)
gcloud biglake iceberg catalogs create aws_glue_logistics \
  --project="${PROJECT_ID}" \
  --catalog-type="federated" \
  --federated-catalog-type="glue" \
  --glue-warehouse="${AWS_ACCOUNT_ID}" \
  --glue-aws-region="us-east-1" \
  --glue-aws-role-arn="arn:aws:iam::${AWS_ACCOUNT_ID}:role/lakehouse_glue_federation_role" \
  --cross-cloud-cache="enabled" \
  --primary-location="us-east4"

# 3. Lock the AWS IAM Role trust policy to Google's auto-provisioned Subject ID & enable 5m sync
GLUE_SA_SUB="$(gcloud biglake iceberg catalogs describe aws_glue_logistics \
  --project="${PROJECT_ID}" --format="value(biglake_service_account_id)")"
# (setup_glue_federation.sh patches accounts.google.com:sub = $GLUE_SA_SUB automatically)

gcloud biglake iceberg catalogs update aws_glue_logistics \
  --project="${PROJECT_ID}" \
  --refresh-interval="300s" \
  --cross-cloud-cache="enabled"
```

---

### Step 2: Set Up & Federate Standalone Databricks Portal (Unity Catalog)

In the repo, [`infra/databricks-unity/README.md`](../infra/databricks-unity/README.md) and [`00_prepare_uc_external_storage.sh`](../infra/databricks-unity/00_prepare_uc_external_storage.sh) walk click-by-click through preparing a fresh **Databricks Portal** (*Free / Community Edition* or workspace) from scratch. Keep three essentials in mind:

1. Toggle **External data access** ON in Unity Catalog Metastore Settings.
2. Register a **Unity Catalog External Location** (`Catalog -> External Data`)—Google Cloud Lakehouse does not read from Databricks internal `default storage`.
3. Grant **`EXTERNAL USE SCHEMA`** to your Databricks Service Principal so Unity Catalog vends short-lived storage credentials.

Run [`databricks_uc_setup.sql`](../infra/databricks-unity/databricks_uc_setup.sql) in the Databricks SQL Editor to create `supplier_risk_scores` and `sku_margins` with **Delta UniForm (`iceberg`)** enabled:

```sql
CREATE SCHEMA IF NOT EXISTS omniretail_merch.supply_chain
MANAGED LOCATION '<YOUR_UNITY_CATALOG_EXTERNAL_LOCATION_URI>/supply_chain/';

CREATE OR REPLACE TABLE omniretail_merch.supply_chain.supplier_risk_scores (
  supplier_id STRING NOT NULL,
  supplier_name STRING NOT NULL,
  geopolitical_risk_score DOUBLE,
  lead_time_days INT
) TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.enableIcebergCompatV2' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

GRANT USE CATALOG ON CATALOG omniretail_merch TO `<SERVICE_PRINCIPAL_APP_ID>`;
GRANT USE SCHEMA, SELECT, EXTERNAL USE SCHEMA
  ON SCHEMA omniretail_merch.supply_chain TO `<SERVICE_PRINCIPAL_APP_ID>`;
```

Then run [`setup_unity_federation.sh`](../infra/databricks-unity/setup_unity_federation.sh) to store the Service Principal OAuth secret in Regional Secret Manager (or use secretless OIDC + `--federated-catalog-identity-mode="user-identity-federation"` on multi-user workspaces) and turn on **Intelligent Caching**:

```bash
gcloud biglake iceberg catalogs create dbx_unity_merch \
  --project="${PROJECT_ID}" \
  --catalog-type="federated" \
  --federated-catalog-type="unity" \
  --unity-instance-name="dbc-a1b2c3d4-e5f6.cloud.databricks.com" \
  --unity-catalog-name="omniretail_merch" \
  --secret-name="projects/${PROJECT_ID}/locations/us-east4/secrets/dbx-unity-sp-oauth" \
  --cross-cloud-cache="enabled" \
  --primary-location="us-east4"
```

![End-to-End OIDC User Identity Federation](../docs/images/identity_federation_flow.jpg)

---

### Step 3: Seed Native BigQuery Orders & Run a 3-Platform SQL Join

Seed the native BigQuery table (`bash infra/gcp-bigquery/setup_bigquery_native.sh`), then join **BigQuery + standalone Databricks + AWS Glue** in one standard SQL query using **4-part table identifiers** (`` `project.catalog.namespace.table` ``):

```sql
SELECT
  o.customer_region,
  m.category,
  r.supplier_name,
  r.geopolitical_risk_score,
  s.delay_hours,
  COUNT(DISTINCT o.order_id) AS affected_orders,
  ROUND(SUM(o.quantity * (o.unit_sale_price_usd - m.unit_cost_usd)), 2) AS gross_margin_exposure_usd
FROM `omni-retail-lakehouse-prod.commerce_core.orders` AS o
INNER JOIN `omni-retail-lakehouse-prod.dbx_unity_merch.supply_chain.sku_margins` AS m
  ON o.sku_id = m.sku_id
INNER JOIN `omni-retail-lakehouse-prod.dbx_unity_merch.supply_chain.supplier_risk_scores` AS r
  ON m.supplier_id = r.supplier_id
INNER JOIN `omni-retail-lakehouse-prod.aws_glue_logistics.fulfillment.shipment_events` AS s
  ON o.order_id = s.order_id
WHERE o.order_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND (s.delay_hours > 12 OR r.geopolitical_risk_score >= 0.65)
GROUP BY 1, 2, 3, 4, 5
ORDER BY gross_margin_exposure_usd DESC;
```

Need PySpark over the exact same federated tables? [`spark/cross_cloud_margin_risk_job.py`](../spark/cross_cloud_margin_risk_job.py) mounts `https://biglake.googleapis.com/iceberg/v1/restcatalog/ext` with `header.X-Iceberg-Access-Delegation=vended-credentials`—running on **Managed Service for Apache Spark** without a single cloud secret on the cluster.

---

### Step 4: Verify Intelligent Caching (`objectStorageStats`)

![Cross-Cloud Intelligent Caching Data Flow](../docs/images/intelligent_cache_flow.jpg)

To prove Intelligent Caching is serving Parquet blocks locally (rather than hitting BigQuery’s SQL result cache), run [`scripts/run_cross_cloud_join.sh`](../scripts/run_cross_cloud_join.sh)—which executes the join twice with `--nouse_cache`—and inspect `statistics.query.objectStorageStats` on Run #2:

```bash
bash scripts/run_cross_cloud_join.sh
bq show --format=prettyjson -j <RUN_2_JOB_ID> | jq '.statistics.query.objectStorageStats'
```

```json
[
  {
    "cloudProvider": "AWS",
    "cacheBytesRead": "46200000000",
    "objectStorageBytesRead": "1800000000"
  }
]
```

On the warm run, **`cacheBytesRead` jumps to 96%+**, reading directly from Google Cloud’s regional cache at **\$0 remote storage egress**. *(Practitioner note: If `cacheBytesRead` remains `0`, verify `--cross-cloud-cache="enabled"` and check that org policy `constraints/gcp.restrictNonCmekServices` isn’t disabling Google-managed cache encryption.)*

---

## Adding the Agentic Layer: Google ADK + `gemini-3.8-flash`

Dashboards run predictable queries. **AI agents explore data in loops.** When a supply-chain manager asks *“Why is Q4 electronics margin slipping in the Northeast, and which suppliers are driving the delay?”*, an agent fires **6 to 8 follow-up SQL queries** in seconds.

* **Uncached Cross-Cloud Federation**: 40 investigations/day × 6 SQL calls × 48 GB = **345.6 TB/month** of remote egress (**~\$31,104/month** at \$0.09/GB).
* **With Lakehouse Intelligent Caching**: Query #1 warms the regional block cache; agent queries #2–#6 hit local Google Cloud cache (~92% hit rate), cutting egress by **~92% (~\$2,488/month**, or **~\$553/month** over Cross-Cloud Interconnect).

Using **Google Agent Development Kit (`google-adk` 2.x)** and **`gemini-3.8-flash`**, [`src/lakehouse_agent/agent.py`](../src/lakehouse_agent/agent.py) pairs a read-only SQL analyst sub-agent with a FinOps cache-telemetry sub-agent:

```python
from google.adk.agents import Agent
from google.adk.integrations.bigquery import BigQueryCredentialsConfig, BigQueryToolset
from google.adk.integrations.bigquery.config import BigQueryToolConfig, WriteMode
from google.oauth2.credentials import Credentials

from lakehouse_agent.cache_telemetry import (
    inspect_bigquery_job_cache_stats,
    simulate_agentic_burst_finops,
)

def build_copilot(project_id: str, user_oauth_token: str | None = None) -> Agent:
    bq_toolset = BigQueryToolset(
        credentials_config=(
            BigQueryCredentialsConfig(credentials=Credentials(token=user_oauth_token))
            if user_oauth_token else None
        ),
        bigquery_tool_config=BigQueryToolConfig(
            write_mode=WriteMode.BLOCKED,          # Strict read-only guardrail
            location="us-east4",
            compute_project_id=project_id,
            maximum_bytes_billed=10 * 1024**3,     # 10 GiB per-query FinOps cap
        ),
    )

    analyst = Agent(
        name="cross_cloud_supply_chain_analyst",
        model="gemini-3.8-flash",
        instruction=(
            "Query BigQuery (`commerce_core.orders`), standalone Databricks Unity Catalog "
            "(`dbx_unity_merch.supply_chain.*`), and AWS Glue (`aws_glue_logistics.fulfillment.*`) "
            "using 4-part SQL identifiers. Project only needed columns and return the job_id."
        ),
        tools=[bq_toolset],
    )

    finops_advisor = Agent(
        name="finops_cache_telemetry_advisor",
        model="gemini-3.8-flash",
        instruction="Audit BigQuery `objectStorageStats` cache hit ratios and quantify avoided egress.",
        tools=[inspect_bigquery_job_cache_stats, simulate_agentic_burst_finops],
    )

    return Agent(
        name="lakehouse_practitioner_copilot",
        model="gemini-3.8-flash",
        instruction="Coordinate cross-platform SQL analytics and Intelligent Cache FinOps telemetry.",
        sub_agents=[analyst, finops_advisor],
    )
```

Launch the interactive ADK Web UI locally in two commands:

```bash
uv sync --extra dev
uv run adk web src
```

---

## Clean Up / Uninstall the POC

Because this is a hands-on POC spanning three platforms, always tear down your test resources when you’re done experimenting so you don’t incur ongoing storage or compute charges.

Run the included teardown script ([`infra/teardown_all.sh`](../infra/teardown_all.sh)) to delete the Google Cloud federated catalogs, Secret Manager secret, BigQuery `commerce_core` dataset, external GCS bucket, AWS Glue table/database, AWS OIDC IAM role, and S3 bucket:

```bash
export PROJECT_ID="your-gcp-project-id"
export GCP_REGION="us-east4"
export AWS_REGION="us-east-1"

bash infra/teardown_all.sh
```

Then run these three lines in your **Databricks Portal SQL Editor** (and delete your test Service Principal under *Settings -> Identity and access*):

```sql
DROP CATALOG IF EXISTS omniretail_merch CASCADE;
DROP EXTERNAL LOCATION IF EXISTS lakehouse_poc_ext_loc;
DROP STORAGE CREDENTIAL IF EXISTS lakehouse_poc_cred;
```

---

## Wrapping Up

With **Cross-Cloud Federated Catalogs (GA)** and **Intelligent Cross-Cloud Caching (GA)**, **Google Cloud Lakehouse** lets you unify **BigQuery**, **Managed Spark**, **Databricks Unity Catalog**, and **AWS Glue** on open Apache Iceberg standards—delivering local-lakehouse speed and predictable FinOps for both human analysts and **Gemini 3.8 Flash** agents.

Try out the POC in the GitHub repo, and be sure to review the official Google Cloud, Databricks, and AWS documentation linked below as you move toward production architectures.

---

## Key Links & Official References

* **Companion GitHub POC Repository**: [cmanikandan/gcp-borderless-lakehouse-adk](https://github.com/cmanikandan/gcp-borderless-lakehouse-adk)
* **Google Cloud Lakehouse Release Notes**: [Cross-Cloud Caching (GA) & Federated Catalogs (GA)](https://docs.cloud.google.com/lakehouse/docs/release-notes)
* **Google Cloud Cross-Cloud Overview**: [About Cross-Cloud Data Access & Intelligent Caching](https://docs.cloud.google.com/lakehouse/docs/about-cross-cloud-data-access#intelligent-caching)
* **Querying Federated Iceberg Tables**: [Query Remote Data in BigQuery & Spark](https://docs.cloud.google.com/lakehouse/docs/query-remote-data)
* **Databricks Federation Guide**: [Set Up Cross-Cloud Connection to Databricks Unity Catalog](https://docs.cloud.google.com/lakehouse/docs/set-up-cross-cloud-connection-databricks)
* **AWS Glue Federation Guide**: [Set Up Cross-Cloud Connection to AWS Glue Data Catalog](https://docs.cloud.google.com/lakehouse/docs/set-up-cross-cloud-connection-aws-glue)
* **Snowflake & Workday Federation**: [Snowflake Horizon Catalog](https://docs.cloud.google.com/lakehouse/docs/set-up-cross-cloud-connection-snowflake) | [Workday Data Lake](https://docs.cloud.google.com/lakehouse/docs/set-up-cross-cloud-connection-workday)
* **Databricks & AWS Official Docs**: [Databricks Unity Catalog Docs](https://docs.databricks.com/en/data-governance/unity-catalog/index.html) | [AWS Glue Iceberg Docs](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html)
* **Google Agent Development Kit (ADK)**: [BigQuery Toolset Integration (`google.adk.integrations.bigquery`)](https://adk.dev/integrations/bigquery/)
