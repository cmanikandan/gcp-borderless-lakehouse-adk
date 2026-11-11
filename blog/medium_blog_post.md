# How to query Databricks Unity Catalog and AWS Glue in place with Google Cloud Lakehouse, cross-cloud caching, and Google ADK

![Google Cloud Lakehouse cross-cloud architecture overview](../docs/images/hero_borderless_lakehouse.png)

When an autonomous AI agent investigates a production supply chain anomaly, it rarely runs a single SQL query—it loops through five or six follow-up queries in seconds to isolate root causes across regions, suppliers, and carriers. Across 40 daily investigations over a 48 GB cross-cloud table on Amazon S3, uncached federation transfers **345.6 TB per month**—generating roughly **\$31,104/month** in cloud storage egress fees before a single dashboard refreshes.

With the General Availability (GA) of [cross-cloud federated catalogs and cross-cloud caching in Google Cloud Lakehouse](https://docs.cloud.google.com/lakehouse/docs/release-notes), cloud practitioners can query remote **Apache Iceberg** tables in **Databricks Unity Catalog**, **AWS Glue Data Catalog**, **Snowflake Horizon Catalog**, and **Workday Data Lake** directly from **BigQuery** and **Apache Spark** without building brittle ETL pipelines—while caching active Parquet blocks inside your Google Cloud region at **\$0 additional Google Cloud fee**.

First, we unpack the core architecture of Google Cloud Lakehouse, including background metadata synchronization, short-lived credential vending, OpenID Connect (OIDC) user identity federation, and regional block caching. Second, we walk step by step through provisioning AWS Glue, a standalone Databricks Unity Catalog workspace, BigQuery, Managed Service for Apache Spark, and a tiered multi-agent copilot built with **Google Agent Development Kit (Google ADK)** pairing **Gemini 4 Argon**, **Gemini 4 Carbon**, and **Gemini 3.8 Flash**. Finally, we evaluate a real-world retail supply chain scenario, measure 30-day multi-agent egress savings, explore additional cross-cloud use cases, and clean up every proof-of-concept resource.

> **Proof-of-concept disclaimer:** Everything in this article and the [companion GitHub repository (`cmanikandan/gcp-borderless-lakehouse-adk`)](https://github.com/cmanikandan/gcp-borderless-lakehouse-adk) is an educational proof of concept (POC) built from a cloud practitioner's perspective to validate cross-cloud capabilities hands-on. Before deploying production workloads, always consult the official [Google Cloud Lakehouse documentation](https://docs.cloud.google.com/lakehouse/docs/about-cross-cloud-data-access), [Databricks Unity Catalog documentation](https://docs.databricks.com/en/data-governance/unity-catalog/index.html), and [AWS Glue Apache Iceberg documentation](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html) for production security hardening, IAM least-privilege policies, networking, and service limits.

---

## Understanding core Google Cloud Lakehouse concepts

Enterprise datasets rarely live in a single cloud. Historically, analytics teams faced an expensive trade-off: maintain fragile copy pipelines that duplicate terabytes across clouds, or query remote object storage directly and absorb high latency and unpredictable network egress charges.

**Google Cloud Lakehouse** (built on **BigLake** and **BigQuery**) resolves that trade-off by combining open table standards with cloud-spanning governance and regional block caching.

* **Open-format interoperability on Apache Iceberg:** Google Cloud Lakehouse exposes a unified Apache Iceberg REST Catalog (`https://biglake.googleapis.com/iceberg/v1/restcatalog`) over native Apache Iceberg tables as well as Delta Lake tables configured with Iceberg UniForm. **BigQuery**, **Google Cloud Managed Service for Apache Spark**, and open-source **Apache Spark** read the exact same table snapshots in place with zero duplicate copies.
* **Zero-ETL cross-cloud federated catalogs (GA):** Configuring `--catalog-type="federated"` synchronizes catalog namespaces and Iceberg snapshot pointers in the background from **Databricks Unity Catalog**, **AWS Glue Data Catalog** (including Amazon S3 Table Buckets), **Snowflake Horizon Catalog**, and **Workday Data Lake**.
* **Intelligent cross-cloud caching (GA):** Disabled by default and available at **\$0 additional Google Cloud fee**, opting into cross-cloud caching stores active remote Parquet column blocks in Google-managed regional cache inside your Google Cloud region—checking remote snapshot metadata and ACLs on every query while serving warm BigQuery scans at **\$0 repeat cross-cloud egress**.
* **Secretless credential vending, private routing, and user identity federation:** Lakehouse vends short-lived, table-scoped credentials on demand, routes over TLS 1.3 or **Cross-Cloud Interconnect** inside **VPC Service Controls** perimeters, and propagates end-user OIDC identities into **Databricks Unity Catalog** row/column policies and `system.access.audit` logs.
* **Agent-ready analytics foundation:** Federated tables support predicate pushdown and zero-shot `AI.FORECAST` (TimesFM 2.0) in BigQuery, enabling tiered **Google ADK** agents (**Gemini 4 Argon**, **Gemini 4 Carbon**, and **Gemini 3.8 Flash**) to run iterative cross-cloud SQL investigations economically.

---

## Step-by-step implementation and code walkthrough

Clone [`cmanikandan/gcp-borderless-lakehouse-adk`](https://github.com/cmanikandan/gcp-borderless-lakehouse-adk) to run the local 30-second simulator (`make install && make test && make demo-finops`) or provision all three environments end to end.

![Cross-cloud Google Cloud Lakehouse reference architecture](../docs/images/gcp_lakehouse_architecture.png)

### 1. Federating AWS Glue and Amazon S3 with OpenID Connect

Run [`00_bootstrap_aws_s3_glue_iceberg.sh`](https://github.com/cmanikandan/gcp-borderless-lakehouse-adk/blob/main/infra/aws-glue/00_bootstrap_aws_s3_glue_iceberg.sh) to provision an encrypted Amazon S3 bucket and seed an Apache Iceberg v2 table (`fulfillment.shipment_events`) via serverless Amazon Athena Engine v3, then run [`setup_glue_federation.sh`](https://github.com/cmanikandan/gcp-borderless-lakehouse-adk/blob/main/infra/aws-glue/setup_glue_federation.sh) to bind Google's BigLake service account to AWS IAM via secretless OIDC with `--cross-cloud-cache="enabled"`.

### 2. Connecting standalone Databricks Unity Catalog

Follow [`infra/databricks-unity/README.md`](https://github.com/cmanikandan/gcp-borderless-lakehouse-adk/blob/main/infra/databricks-unity/README.md) *(use a 14-day Databricks Free Trial or standard workspace so Metastore Admin **External data access** is unlocked, or pass `--glue-only` to test BigQuery + AWS Glue standalone)*, seed `supplier_risk_scores` and `sku_margins` with Delta UniForm (`iceberg`) via [`databricks_uc_setup.sql`](https://github.com/cmanikandan/gcp-borderless-lakehouse-adk/blob/main/infra/databricks-unity/databricks_uc_setup.sql), and federate with [`setup_unity_federation.sh`](https://github.com/cmanikandan/gcp-borderless-lakehouse-adk/blob/main/infra/databricks-unity/setup_unity_federation.sh).

![End-to-end OIDC user identity federation to Databricks Unity Catalog](../docs/images/identity_federation_flow.png)

### 3. Joining tables across BigQuery and Managed Service for Apache Spark

Seed the native BigQuery orders table (`bash infra/gcp-bigquery/setup_bigquery_native.sh`), then run [`02_cross_cloud_supply_chain_join.sql`](https://github.com/cmanikandan/gcp-borderless-lakehouse-adk/blob/main/sql/02_cross_cloud_supply_chain_join.sql) across **BigQuery + standalone Databricks Unity Catalog + AWS Glue** using 4-part table identifiers (`` `project.catalog.namespace.table` ``)—or run PySpark feature engineering over the same catalogs in **Managed Service for Apache Spark 2.3** via [`cross_cloud_margin_risk_job.py`](https://github.com/cmanikandan/gcp-borderless-lakehouse-adk/blob/main/spark/cross_cloud_margin_risk_job.py) (`X-Iceberg-Access-Delegation=vended-credentials`).

![Multi-engine access with BigQuery SQL and Managed Service for Apache Spark](../docs/images/iceberg_rest_credential_vending.png)

### 4. Enabling and verifying cross-cloud caching in Google Cloud Lakehouse

![Google Cloud Lakehouse intelligent cross-cloud caching workflow](../docs/images/intelligent_cache_flow.png)

Opt into caching via `--cross-cloud-cache="enabled"` on a federated catalog or `crossCloudCacheOptions.enabled=true` on a BigQuery cross-cloud connection ([`setup_bq_cross_cloud_connection_cache.sh`](https://github.com/cmanikandan/gcp-borderless-lakehouse-adk/blob/main/infra/gcp-bigquery/setup_bq_cross_cloud_connection_cache.sh)), and audit `cacheBytesRead` vs. `objectStorageBytesRead` via [`04_finops_org_cache_audit.sql`](https://github.com/cmanikandan/gcp-borderless-lakehouse-adk/blob/main/sql/04_finops_org_cache_audit.sql).

### 5. Building a tiered multi-agent copilot with Google ADK and Gemini 4

![Multi-agent cross-cloud analytics and cost telemetry with Google ADK](../docs/images/adk_multi_agent_cache_sequence.png)

Rather than forcing every agent onto a single model, [`src/lakehouse_agent/agent.py`](https://github.com/cmanikandan/gcp-borderless-lakehouse-adk/blob/main/src/lakehouse_agent/agent.py) defaults to `MODEL_PROFILE="tiered"`—pairing **`gemini-4-argon`** (`ThinkingLevel.LOW`) on the Coordinator for fast routing, **`gemini-4-carbon`** (`ThinkingLevel.HIGH`) on the cross-cloud SQL & `AI.FORECAST` analyst for deep multi-table reasoning, and **`gemini-3.8-flash`** (`ThinkingLevel.LOW`) on the cache FinOps advisor, with explicit `max_output_tokens=8192` decode caps and dynamic `llm_model` BigQuery job labels.

---

## Real-world application and multi-cloud cost impact

In our *OmniRetail Horizon* supply chain scenario, BigQuery prunes healthy order `ORD-8803` (`2h` delay, `0.14` risk on `NordicFreight`) and identifies **\$39,202.00** in gross margin exposure across three delayed shipments (`PacificAirCargo` \$24,412.00, `OceanicExpress` \$7,740.00, and `GreatLakesRail` \$7,050.00). Across 40 daily 6-query agent investigations over a 48 GB table, regional caching lowers monthly cross-cloud egress from **\$31,104/month** (`345.6 TB`) to **\$2,488/month** (**92% savings**)—or **\$553/month** over Cross-Cloud Interconnect.

### Cleaning up your proof-of-concept resources

Run [`infra/teardown_all.sh`](https://github.com/cmanikandan/gcp-borderless-lakehouse-adk/blob/main/infra/teardown_all.sh) to tear down all Google Cloud and AWS resources in one command, then run `DROP CATALOG IF EXISTS omniretail_merch CASCADE;` along with `DROP EXTERNAL LOCATION` and `DROP STORAGE CREDENTIAL` in your Databricks SQL Editor.

---

## Conclusion and next steps

Beyond retail supply chain analytics, this same **Google Cloud Lakehouse + tiered Gemini 4 ADK** architecture unlocks post-merger financial reconciliation across **Snowflake Horizon Catalog** and **Databricks Unity Catalog**, workforce planning across **Workday Data Lake** and **AWS Glue**, VPC-SC-protected cybersecurity threat hunting over **Amazon S3 Table Buckets** with **Gemini 4 Carbon**, multi-engine PySpark feature engineering + `AI.FORECAST` Customer 360 pipelines, and OIDC-governed clinical cohort federation.

Clone [`cmanikandan/gcp-borderless-lakehouse-adk`](https://github.com/cmanikandan/gcp-borderless-lakehouse-adk), test `--model-profile tiered|argon|carbon|flash` locally with `make inspect-agent`, and consult the official [Google Cloud Lakehouse](https://docs.cloud.google.com/lakehouse/docs/release-notes), [Gemini 4](https://aistudio.google.com/docs/whats-new-gemini-4-carbon), [Databricks Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html), and [AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html) documentation before production rollouts.
