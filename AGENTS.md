# AGENTS.md — AI Coding & Runtime Agent Guide

> **Proof-of-Concept (POC) Notice**: This repository is an educational cloud practitioner Proof of Concept (POC) exploring cross-cloud data federation, intelligent caching, and agentic analytics. Always consult the official **Google Cloud**, **Databricks**, and **AWS** documentation for production security, IAM, networking, and governance configurations.

This repository contains end-to-end POC provisioning scripts, cross-platform SQL/Spark analytics, teardown automation, and a **Google Agent Development Kit (`google-adk` 2.x)** multi-agent system powered by **`gemini-3.8-flash`** on top of **Google Cloud Lakehouse**.

Use this file as the source of truth when modifying agents, SQL/Spark workloads, infrastructure scripts, or tests.

---

## 1. System Topology & Agent Roles

`src/lakehouse_agent/agent.py` exports `root_agent` (lazily initialized via `__getattr__` in `src/lakehouse_agent/__init__.py` so env overrides can be set prior to agent instantiation) and runs out-of-the-box with `adk web src`, `adk run src/lakehouse_agent`, and `scripts/run_agent_cli.py`:

```text
lakehouse_practitioner_copilot (Root Orchestrator — gemini-3.8-flash, ThinkingLevel.LOW)
├── cross_cloud_supply_chain_analyst (Sub-Agent — gemini-3.8-flash, ThinkingLevel.MEDIUM)
│   ├── discover_federated_tables (BigLake Iceberg REST API 4-part schema & snapshot discovery)
│   └── BigQueryToolset (WriteMode.BLOCKED, BIGQUERY_ALLOWED_TOOLS filter, execute_sql + forecast)
└── finops_cache_telemetry_advisor (Sub-Agent — gemini-3.8-flash, ThinkingLevel.MEDIUM)
    ├── inspect_bigquery_job_cache_stats (supports job_id="LATEST" via workload label lookup)
    ├── simulate_agentic_burst_finops (models multi-turn agent cache ROI & avoided egress)
    └── get_federated_catalog_cli_blueprint (emits verified GA gcloud & bq connection commands)
```

---

## 2. Technical Invariants (Do Not Violate)

1. **Google ADK 2.x Integration & `tool_filter` Guardrails**:
   - Always import `BigQueryToolset` and `BigQueryCredentialsConfig` from `google.adk.integrations.bigquery` (and `BigQueryToolConfig`, `WriteMode` from `google.adk.integrations.bigquery.config`). **Never** import from deprecated `google.adk.tools.bigquery`.
   - Keep `tool_filter=list(BIGQUERY_ALLOWED_TOOLS)` on `BigQueryToolset` (`execute_sql`, `forecast`, `list_dataset_ids`, `get_dataset_info`, `list_table_ids`, `get_table_info`, `get_job_info`). Do **not** expose `search_catalog` (requires optional `google-cloud-dataplex`) or `analyze_contribution` / `detect_anomalies` (raise `ValueError` under `WriteMode.BLOCKED` because they execute `CREATE TEMP MODEL`).
   - When Application Default Credentials (ADC) are absent in unit tests or CI, pass `credentials_config=None` to `BigQueryToolset` rather than an empty `BigQueryCredentialsConfig()`.

2. **Federated Schema Discovery & `job_id="LATEST"` Telemetry**:
   - Standard `BigQueryToolset` catalog tools (`list_dataset_ids`) only enumerate 3-part native datasets and cannot list 4-part federated catalogs. Keep `discover_federated_tables` (`src/lakehouse_agent/catalog_discovery.py`) wired on the analyst agent so it can inspect namespaces, column schemas, and Iceberg snapshot IDs via `https://biglake.googleapis.com/iceberg/v1/restcatalog`.
   - Because `BigQueryToolset.execute_sql` returns rows without exposing the BigQuery `job_id`, `BigQueryToolConfig` tags queries with `job_labels={"workload": "borderless-lakehouse-adk", "component": "adk-copilot"}`, and `inspect_bigquery_job_cache_stats(job_id="LATEST")` automatically resolves the latest matching completed query job in `BQ_LOCATION`.

3. **Decoupled Vertex AI vs. BigQuery Locations**:
   - Preview/latest Gemini models on Vertex AI (`GOOGLE_GENAI_USE_VERTEXAI=TRUE`) require `GOOGLE_CLOUD_LOCATION=global`, whereas BigQuery datasets and Lakehouse regional caches live in `BQ_LOCATION` / `GCP_REGION` (`us-east4`). Keep these decoupled in `LakehouseSettings` (`src/lakehouse_agent/config.py`).

4. **Standalone Databricks Platform vs. AWS Cloud**:
   - Treat **Databricks Unity Catalog** (accessed via its own standalone **Databricks Portal — Free / Community Edition** or workspace) and **AWS Glue Data Catalog** (on **AWS S3** or **S3 Table Buckets**) as **two separate, independent external platforms** federated into Google Cloud (`us-east4`). Never model Databricks inside AWS.
   - Google Cloud Lakehouse **does not support Databricks internal `default storage`**. Federated Unity Catalog schemas must use a registered **External Location**, have **External data access** enabled on the metastore, and grant `EXTERNAL USE SCHEMA`.
   - Regional Secret Manager secrets require scoping `CLOUDSDK_API_ENDPOINT_OVERRIDES_SECRETMANAGER="https://secretmanager.${GCP_REGION}.rep.googleapis.com/"`.

5. **4-Part SQL Identifiers & Spark Iceberg REST Properties**:
   - Native BigQuery tables use 3-part names (`` `project.dataset.table` ``); federated Iceberg tables use 4-part names (`` `project.federated_catalog.namespace.table` ``). Always project explicit columns and push down `WHERE` predicates.
   - In Apache Spark (`spark/cross_cloud_margin_risk_job.py`), use `uri = https://biglake.googleapis.com/iceberg/v1/restcatalog` (no `/ext`), `warehouse = bl://projects/<PROJECT>/catalogs/<CATALOG>`, and `rest.auth.type = org.apache.iceberg.gcp.auth.GoogleAuthManager`. Note that Intelligent Cross-Cloud Caching accelerates **BigQuery** queries; Spark executors read directly from remote storage via short-lived vended credentials.

---

## 3. Repository Commands

```bash
# Install dependencies (Python 3.11+ via uv)
make install          # or: uv sync --extra dev

# Run full validation suite (pytest + shell syntax + XML/SVG diagram validation)
make test

# Run environment & cloud readiness diagnostics (exits 0 locally even without cloud auth)
make doctor           # or: uv run python scripts/preflight_doctor.py

# Run offline Cold vs. Warm Intelligent Cache & Agentic FinOps benchmark demo
make demo-finops      # or: uv run python scripts/demo_agent_cache_finops.py

# Inspect multi-agent ADK topology & tool wiring offline (or pass --prompt "..." with API key)
make inspect-agent    # or: uv run python scripts/run_agent_cli.py --inspect-topology

# Launch interactive Google ADK Dev UI
make adk-web          # or: uv run adk web src

# Uninstall / tear down all POC cloud resources when finished testing
make teardown         # or: bash infra/teardown_all.sh
```
