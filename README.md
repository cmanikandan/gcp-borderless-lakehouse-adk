# Google Cloud Borderless Lakehouse POC + Google ADK (`gemini-3.8-flash`)

![Google Cloud Borderless Lakehouse Overview](docs/images/hero_borderless_lakehouse.jpg)

[![CI — Borderless Lakehouse ADK Suite](https://github.com/cmanikandan/gcp-borderless-lakehouse-adk/actions/workflows/ci.yml/badge.svg)](https://github.com/cmanikandan/gcp-borderless-lakehouse-adk/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Google ADK](https://img.shields.io/badge/Google_ADK-2.13%2B-4285F4)](https://adk.dev/integrations/bigquery/)
[![Model](https://img.shields.io/badge/Gemini-3.8_Flash-34A853)](https://ai.google.dev/)

> **Proof-of-Concept (POC) Disclaimer**: This repository is an educational **Proof of Concept (POC)** built from a cloud practitioner's perspective to explore and test cross-cloud lakehouse federation, caching, and agentic analytics end to end. It is **not** a drop-in production template. Before deploying to production, always consult the official [**Google Cloud Lakehouse documentation**](https://docs.cloud.google.com/lakehouse/docs/about-cross-cloud-data-access), [**Databricks Unity Catalog documentation**](https://docs.databricks.com/en/data-governance/unity-catalog/index.html), and [**AWS Glue & S3 documentation**](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html) for production security hardening, IAM least-privilege policies, networking, and service limits.

---

## Why Google Cloud Lakehouse?

**Google Cloud Lakehouse** (powered by BigLake and BigQuery) brings an open, cloud-spanning data foundation designed around five core advantages:

1. **Open-Format Interoperability on Apache Iceberg**: Native support for **Apache Iceberg** (including Delta Lake tables with Iceberg UniForm enabled) via a standard Iceberg REST Catalog—so **BigQuery**, **Google Cloud Managed Service for Apache Spark**, and open-source **Apache Spark** query the exact same snapshots without format lock-in or duplicate copies.
2. **Zero-ETL Cross-Cloud Federated Catalogs (GA)** (`--catalog-type="federated"`): Continuously synchronize Iceberg metadata in the background from **Databricks Unity Catalog**, **AWS Glue Data Catalog**, **Snowflake Horizon Catalog**, and **Workday Data Lake**, making external tables queryable in place without brittle ETL pipelines.
3. **Intelligent Cross-Cloud Caching (GA)** (`--cross-cloud-cache="enabled"`): Opt-in regional block-level caching inside Google Cloud keeps active Parquet column blocks close to compute—slashing repetitive cross-cloud egress fees and accelerating subsequent BI and AI queries.
4. **Unified Governance, Credential Vending & Identity Federation**: Automatic short-lived storage credential vending eliminates static cloud keys on worker clusters, optional **Cross-Cloud Interconnect** and **VPC Service Controls** secure the network perimeter, and **User Identity Federation** propagates end-user OIDC identities into **Databricks Unity Catalog** for row/column enforcement and audit logging.
5. **Agent-Ready Data Foundation**: Federated tables behave like first-class tables in BigQuery, enabling **Google Agent Development Kit (`google-adk` 2.x)** agents powered by **`gemini-3.8-flash`** to reason across multi-platform datasets while Intelligent Caching absorbs iterative agent query bursts.

> **Companion Medium Blog Post**: Read the full post in [`blog/medium_blog_post.md`](blog/medium_blog_post.md).

---

## POC Use Case: *OmniRetail Horizon* Supply Chain & Margin Risk

In this POC, we stand up three independent platforms from scratch and join them in place without building a single ETL pipeline:

| Domain Dataset | Platform & Governance | Table Format | Query Path in BigQuery |
| :--- | :--- | :--- | :--- |
| **Live Customer Orders** | **Google Cloud BigQuery** (`us-east4`) | Native BigQuery | `` `<PROJECT_ID>.commerce_core.orders` `` |
| **Supplier Risk & SKU Margins** | **Standalone Databricks Portal** (*Free / Community Edition* or Workspace Unity Catalog) | Delta + Iceberg UniForm | `` `<PROJECT_ID>.dbx_unity_merch.supply_chain.*` `` |
| **Carrier Shipment Telemetry** | **AWS Cloud** (*AWS Glue Data Catalog + Amazon S3*, `us-east-1`) | Apache Iceberg on S3 | `` `<PROJECT_ID>.aws_glue_logistics.fulfillment.shipment_events` `` |

---

## Reference Architecture

![Google Cloud Borderless Lakehouse Reference Architecture](docs/images/gcp_drawio_architecture.jpg)

*(Vector and diagram source files are available in [`docs/diagrams/`](docs/diagrams/).)*

---

## Repository Structure

```text
gcp-borderless-lakehouse-adk/
├── AGENTS.md                                           # AI coding & runtime agent guardrails
├── README.md                                           # End-to-end setup, testing & teardown guide
├── blog/
│   └── medium_blog_post.md                             # Companion Medium blog post
├── docs/
│   ├── diagrams/                                       # Diagram sources (.drawio & .svg)
│   └── images/                                         # Architecture, caching & OIDC visuals
├── infra/
│   ├── aws-glue/
│   │   ├── 00_bootstrap_aws_s3_glue_iceberg.sh         # Creates S3 bucket, Glue DB & Athena Iceberg table
│   │   ├── athena_seed_iceberg.sql                     # Athena Engine v3 Iceberg DDL & seed data
│   │   ├── setup_glue_federation.sh                    # Secretless OIDC IAM role + Lakehouse catalog sync
│   │   ├── permissions_policy.json                     # Read-only Glue + S3 IAM policy template
│   │   └── trust_policy_placeholder.json               # Google OIDC trust policy template
│   ├── databricks-unity/
│   │   ├── README.md                                   # Click-by-click Databricks Portal setup guide
│   │   ├── 00_prepare_uc_external_storage.sh           # Creates external bucket & binds Databricks SA
│   │   ├── databricks_uc_setup.sql                     # Creates UC schema, UniForm tables & grants
│   │   └── setup_unity_federation.sh                   # Connects Databricks Unity Catalog to Lakehouse
│   ├── gcp-bigquery/
│   │   └── setup_bigquery_native.sh                    # Enables GCP APIs & seeds native BigQuery orders
│   ├── networking/
│   │   └── setup_cci_service_directory.sh              # Optional Private Cross-Cloud Interconnect setup
│   └── teardown_all.sh                                 # One-command POC uninstall across GCP, AWS & DBX
├── sql/
│   ├── 01_seed_bigquery_orders.sql                     # Native BigQuery orders table DDL + seed rows
│   ├── 02_cross_cloud_supply_chain_join.sql            # 4-part cross-platform SQL join
│   └── 03_verify_databricks_user_audit.sql             # Verify OIDC identity in Databricks system.access.audit
├── spark/
│   ├── cross_cloud_margin_risk_job.py                  # PySpark job with Iceberg REST credential vending
│   └── submit_managed_spark_batch.sh                   # Submit serverless Managed Spark batch
├── src/lakehouse_agent/
│   ├── config.py                                       # Typed environment configuration
│   ├── cache_telemetry.py                              # Parses statistics.query.objectStorageStats
│   └── agent.py                                        # Google ADK 2.x + gemini-3.8-flash copilot
├── scripts/
│   ├── run_cross_cloud_join.sh                         # Runs Cold vs. Warm SQL join with --nouse_cache
│   └── demo_agent_cache_finops.py                      # Offline & live job cache FinOps analyzer
└── tests/                                              # Pytest suite for ADK guardrails & telemetry
```

---

## Complete From-Scratch Setup Guide (AWS + Databricks + Google Cloud)

You do **not** need pre-existing tables in AWS, Databricks, or BigQuery—follow Phases 1–6 below to provision everything from scratch, test it, and cleanly uninstall it afterward.

### Prerequisites

* **Google Cloud CLI (`gcloud` & `bq`)** authenticated to a Google Cloud project with billing enabled (`gcloud auth login && gcloud config set project <YOUR_PROJECT_ID>`).
* **AWS CLI (`aws`)** authenticated to an AWS account (`aws sts get-caller-identity`) with permissions for S3, Glue, Athena, and IAM.
* **Standalone Databricks Account** accessed via the **Databricks Portal** at [https://login.databricks.com](https://login.databricks.com) (*Databricks Free Edition* or standard workspace).
* **Python 3.11+** and [`uv`](https://docs.astral.sh/uv/) (`curl -LsSf https://astral.sh/uv/install.sh | sh`).

Export your core environment variables once in your shell:

```bash
export PROJECT_ID="your-gcp-project-id"
export GCP_REGION="us-east4"      # Closest Google Cloud region to AWS us-east-1
export AWS_REGION="us-east-1"
```

---

### Phase 1: Provision AWS S3, AWS Glue & Apache Iceberg Data from Scratch

Run [`infra/aws-glue/00_bootstrap_aws_s3_glue_iceberg.sh`](infra/aws-glue/00_bootstrap_aws_s3_glue_iceberg.sh) to create a private encrypted Amazon S3 bucket, create the `fulfillment` database in AWS Glue, and use serverless **Amazon Athena Engine v3** (zero EC2/EMR clusters required) to create and seed the `fulfillment.shipment_events` Apache Iceberg v2 table:

```bash
bash infra/aws-glue/00_bootstrap_aws_s3_glue_iceberg.sh
```

Next, federate AWS Glue into Google Cloud Lakehouse using secretless OpenID Connect (OIDC) and enable **Intelligent Cross-Cloud Caching**:

```bash
bash infra/aws-glue/setup_glue_federation.sh
```

> **Practitioner Note**: `--glue-warehouse` takes your **12-digit AWS Account ID** (automatically detected by the script via `aws sts get-caller-identity`), **not** an `s3://` URI.

---

### Phase 2: Provision Standalone Databricks Portal (Unity Catalog) from Scratch

Google Cloud Lakehouse **does not support Databricks internal `default storage`**—your federated Unity Catalog schema must point to a registered **External Location**, have **External data access** enabled on the metastore, and grant **`EXTERNAL USE SCHEMA`**.

*(Full click-by-click screenshots/navigation notes are in [`infra/databricks-unity/README.md`](infra/databricks-unity/README.md).)*

1. **Create the External Storage Bucket**:
   ```bash
   bash infra/databricks-unity/00_prepare_uc_external_storage.sh
   ```
2. **Configure Your Standalone Databricks Portal (`https://login.databricks.com`)**:
   * **Enable External Data Access**: Go to **Catalog -> Gear Icon (Metastore Settings)** and toggle **External data access** ON.
   * **Create Storage Credential**: Go to **Catalog -> External Data -> Credentials -> Create credential**, select **Google Cloud Storage**, name it `lakehouse_poc_cred`, and copy the generated Databricks Service Account email.
   * **Bind Bucket IAM**:
     ```bash
     export DBX_GCS_SA_EMAIL="<PASTE_DATABRICKS_GENERATED_SA_EMAIL>"
     bash infra/databricks-unity/00_prepare_uc_external_storage.sh
     ```
   * **Create External Location**: Go to **Catalog -> External Data -> External Locations -> Create external location**, name it `lakehouse_poc_ext_loc`, select `lakehouse_poc_cred`, and set URL to `gs://${PROJECT_ID}-dbx-uc-ext/uc-external`.
   * **Create Service Principal + OAuth Secret**: Go to **Settings -> Identity and access -> Service principals -> Add service principal** (`gcp-lakehouse-federation-sp`) -> **Secrets -> Generate secret**. Save the Client ID (`DBX_SP_CLIENT_ID`) and Secret (`DBX_SP_CLIENT_SECRET`).
3. **Seed Delta UniForm (`iceberg`) Tables in Databricks SQL Editor**:
   * Open [`infra/databricks-unity/databricks_uc_setup.sql`](infra/databricks-unity/databricks_uc_setup.sql) in the Databricks SQL Editor, set `<YOUR_UNITY_CATALOG_EXTERNAL_LOCATION_URI>` to `gs://${PROJECT_ID}-dbx-uc-ext/uc-external` and `<SERVICE_PRINCIPAL_APP_ID>` to your `DBX_SP_CLIENT_ID`, and click **Run**.
4. **Federate Databricks Unity Catalog into Google Cloud Lakehouse**:
   ```bash
   export DBX_WORKSPACE_HOST="dbc-a1b2c3d4-e5f6.cloud.databricks.com"  # Your portal host (no https://)
   export DBX_UC_CATALOG="omniretail_merch"
   export DBX_SP_CLIENT_ID="<YOUR_DATABRICKS_SP_UUID>"
   export DBX_SP_CLIENT_SECRET="<YOUR_DATABRICKS_SP_OAUTH_SECRET>"

   bash infra/databricks-unity/setup_unity_federation.sh
   ```

![End-to-End User Identity Federation Flow](docs/images/identity_federation_flow.jpg)

---

### Phase 3: Provision Native Google Cloud BigQuery Dataset & Seed Orders

Enable the required Google Cloud APIs and create/seed `<PROJECT_ID>.commerce_core.orders` in `us-east4`:

```bash
bash infra/gcp-bigquery/setup_bigquery_native.sh
```

---

### Phase 4: Run Zero-ETL Cross-Platform Joins (BigQuery & Managed Spark)

#### 4A. Cold vs. Warm 3-Platform SQL Join in BigQuery + Cache Verification

![Cross-Cloud Intelligent Caching Flow](docs/images/intelligent_cache_flow.jpg)

Run [`scripts/run_cross_cloud_join.sh`](scripts/run_cross_cloud_join.sh), which executes [`sql/02_cross_cloud_supply_chain_join.sql`](sql/02_cross_cloud_supply_chain_join.sql) **twice** with `--nouse_cache` (bypassing BigQuery's SQL result cache so you can observe Lakehouse's storage-level **Intelligent Cross-Cloud Caching** on Run #2):

```bash
bash scripts/run_cross_cloud_join.sh
```

Then inspect `statistics.query.objectStorageStats` (`cacheBytesRead` vs. `objectStorageBytesRead`) on the warm job:

```bash
uv sync --extra dev
LAST_JOB="$(bq ls -j -n 1 --project_id="${PROJECT_ID}" --location="${GCP_REGION}" --format=json | jq -r '.[0].jobReference.jobId')"
uv run python scripts/demo_agent_cache_finops.py --live-job-id "${LAST_JOB}" --location "${GCP_REGION}"
```

*(Want to preview the cache telemetry parser and FinOps benchmark offline right now without cloud credentials? Run `uv run python scripts/demo_agent_cache_finops.py`.)*

#### 4B. Serverless Managed Spark with Credential Vending

Query the exact same federated Iceberg catalogs from **Google Cloud Managed Service for Apache Spark** using `X-Iceberg-Access-Delegation=vended-credentials` (zero AWS keys or Databricks secrets stored in Spark):

```bash
bash spark/submit_managed_spark_batch.sh
```

---

### Phase 5: Launch the Agentic Layer (`google-adk` 2.x + `gemini-3.8-flash`)

```bash
cp .env.example .env
# Edit .env to set PROJECT_ID and GOOGLE_API_KEY (or GOOGLE_GENAI_USE_VERTEXAI=TRUE)

# Run unit tests
uv run pytest -v

# Start the interactive Google ADK Web UI
uv run adk web src
```

Ask `lakehouse_practitioner_copilot` in the browser UI:
* *"Which product categories have the highest gross margin exposure from delayed shipments or high supplier risk over the past 7 days? Also check the BigQuery job's cross-cloud cache hit ratio."*
* *"Simulate our monthly S3 egress savings if 40 analysts run 6-step agent investigations every day over 48 GB of federated Iceberg tables."*

---

### Phase 6: Clean Up / Uninstall Everything (Avoid Ongoing Cloud Charges)

When you finish testing the POC, tear down all federated catalogs, secrets, BigQuery datasets, GCS buckets, AWS Glue tables/databases, IAM roles, and S3 buckets in one command using [`infra/teardown_all.sh`](infra/teardown_all.sh):

```bash
export PROJECT_ID="your-gcp-project-id"
export GCP_REGION="us-east4"
export AWS_REGION="us-east-1"

bash infra/teardown_all.sh
```

Then run the three cleanup lines printed by the script inside your **Databricks Portal SQL Editor**:

```sql
DROP CATALOG IF EXISTS omniretail_merch CASCADE;
DROP EXTERNAL LOCATION IF EXISTS lakehouse_poc_ext_loc;
DROP STORAGE CREDENTIAL IF EXISTS lakehouse_poc_cred;
```

Finally, remove `gcp-lakehouse-federation-sp` under **Settings -> Identity and access -> Service principals** in your Databricks Portal, and optionally remove your local virtual environment (`rm -rf .venv .env`).

---

## Troubleshooting Checklist

* **Catalog sync succeeds, but 0 tables appear from Databricks**: Verify **External data access** is enabled on your Unity Catalog metastore, your schema uses an **External Location** (not Databricks `default storage`), and your service principal has `EXTERNAL USE SCHEMA`.
* **`cacheBytesRead` stays `0` on repeat `--nouse_cache` runs**: Confirm `--cross-cloud-cache="enabled"` on the federated catalog AND verify that organization policy `constraints/gcp.restrictNonCmekServices` is not enforced on the project (the regional cache uses Google-managed encryption keys).
* **AWS Glue catalog creation fails on `--glue-warehouse`**: Pass only the **12-digit AWS Account ID** (e.g., `123456789012`), not an `s3://` path.

---

## Key Official References

* [Google Cloud Lakehouse Release Notes (Cross-Cloud Caching GA & Federated Catalogs GA)](https://docs.cloud.google.com/lakehouse/docs/release-notes)
* [About Cross-Cloud Data Access & Intelligent Caching](https://docs.cloud.google.com/lakehouse/docs/about-cross-cloud-data-access#intelligent-caching)
* [Query Remote Data in Google Cloud Lakehouse](https://docs.cloud.google.com/lakehouse/docs/query-remote-data)
* [Set Up Cross-Cloud Connection to Databricks Unity Catalog](https://docs.cloud.google.com/lakehouse/docs/set-up-cross-cloud-connection-databricks)
* [Set Up Cross-Cloud Connection to AWS Glue Data Catalog](https://docs.cloud.google.com/lakehouse/docs/set-up-cross-cloud-connection-aws-glue)
* [Set Up Cross-Cloud Connection to Snowflake Horizon Catalog](https://docs.cloud.google.com/lakehouse/docs/set-up-cross-cloud-connection-snowflake)
* [Set Up Cross-Cloud Connection to Workday Data Lake](https://docs.cloud.google.com/lakehouse/docs/set-up-cross-cloud-connection-workday)
* [Databricks Unity Catalog Official Documentation](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
* [AWS Glue & Apache Iceberg Official Documentation](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html)
* [Google Agent Development Kit (ADK) — BigQuery Integration](https://adk.dev/integrations/bigquery/)
