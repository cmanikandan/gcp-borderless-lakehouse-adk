# AGENTS.md — AI Coding & Runtime Agent Guide

> **Proof-of-Concept (POC) Notice**: This repository is an educational cloud practitioner Proof of Concept (POC) exploring cross-cloud data federation, intelligent caching, and agentic analytics. Always consult the official **Google Cloud**, **Databricks**, and **AWS** documentation for production security, IAM, networking, and governance configurations.

This repository contains end-to-end POC provisioning scripts, cross-platform SQL/Spark analytics, teardown automation, and a **Google Agent Development Kit (`google-adk` 2.x)** multi-agent system using **tiered Gemini 4 & Gemini 3.8 routing** (`gemini-4-argon`, `gemini-4-carbon`, and `gemini-3.8-flash`) on top of **Google Cloud Lakehouse**.

Use this file as the source of truth when modifying agents, SQL/Spark workloads, infrastructure scripts, or tests.

---

## 1. System Topology & Multi-Model Agent Roles

`src/lakehouse_agent/agent.py` exports `root_agent` (lazily initialized via `__getattr__` in `src/lakehouse_agent/__init__.py`) and supports `MODEL_PROFILE=tiered|argon|carbon|flash` (plus per-role overrides `COORDINATOR_MODEL`, `ANALYST_MODEL`, `FINOPS_MODEL` and global `GEMINI_MODEL`):

```text
lakehouse_practitioner_copilot (Coordinator — gemini-4-argon, ThinkingLevel.LOW, max_output_tokens=8192)
├── cross_cloud_supply_chain_analyst (Sub-Agent — gemini-4-carbon, ThinkingLevel.HIGH, max_output_tokens=8192)
│   ├── discover_federated_tables (BigLake Iceberg REST API 4-part schema & snapshot discovery)
│   └── BigQueryToolset (WriteMode.BLOCKED, BIGQUERY_ALLOWED_TOOLS filter, execute_sql + forecast)
└── finops_cache_telemetry_advisor (Sub-Agent — gemini-3.8-flash, ThinkingLevel.LOW, max_output_tokens=8192)
    ├── inspect_bigquery_job_cache_stats (supports job_id="LATEST" via workload label lookup)
    ├── simulate_agentic_burst_finops (models multi-turn agent cache ROI & avoided egress)
    └── get_federated_catalog_cli_blueprint (emits verified GA gcloud & bq connection commands)
```

---

## 2. Technical Invariants (Do Not Violate)

1. **Google ADK 2.x Integration, `tool_filter` & Gemini 4 Decode Guardrails**:
   - Always import `BigQueryToolset` and `BigQueryCredentialsConfig` from `google.adk.integrations.bigquery` (and `BigQueryToolConfig`, `WriteMode` from `google.adk.integrations.bigquery.config`). **Never** import from deprecated `google.adk.tools.bigquery`.
   - Keep `tool_filter=list(BIGQUERY_ALLOWED_TOOLS)` on `BigQueryToolset` (`execute_sql`, `forecast`, `list_dataset_ids`, `get_dataset_info`, `list_table_ids`, `get_table_info`, `get_job_info`). Do **not** expose `search_catalog` (requires optional `google-cloud-dataplex`) or `analyze_contribution` / `detect_anomalies` (raise `ValueError` under `WriteMode.BLOCKED` because they execute `CREATE TEMP MODEL`).
   - Because **Gemini 4 Argon** (`gemini-4-argon`) and **Gemini 4 Carbon** (`gemini-4-carbon`) support up to a **1M-token output decode window**, always attach `generate_content_config=types.GenerateContentConfig(max_output_tokens=cfg.max_output_tokens)` to prevent accidental runaway token bills, and sanitize `cfg.resolved_analyst_model` dynamically into `BigQueryToolConfig.job_labels["llm_model"]`.

2. **Federated Schema Discovery & `job_id="LATEST"` Telemetry**:
   - Standard `BigQueryToolset` catalog tools (`list_dataset_ids`) only enumerate 3-part native datasets and cannot list 4-part federated catalogs. Keep `discover_federated_tables` (`src/lakehouse_agent/catalog_discovery.py`) wired on the analyst agent so it can inspect namespaces, column schemas, and Iceberg snapshot IDs via `https://biglake.googleapis.com/iceberg/v1/restcatalog`.
   - `BigQueryToolConfig` tags queries with `job_labels={"workload": "borderless-lakehouse-adk", "llm_model": ..., "model_profile": ...}`, and `inspect_bigquery_job_cache_stats(job_id="LATEST")` automatically resolves the latest matching completed query job in `BQ_LOCATION`.

3. **Decoupled Global Gemini Endpoint vs. Regional BigQuery Cache**:
   - On Vertex AI / Gemini Enterprise Agent Platform (`GOOGLE_GENAI_USE_VERTEXAI=TRUE`), serving `gemini-4-argon`, `gemini-4-carbon`, and `gemini-3.8-flash` via `GOOGLE_CLOUD_LOCATION=global` avoids the 10% regional token premium, whereas BigQuery datasets and Lakehouse regional caches stay co-located in `BQ_LOCATION` / `GCP_REGION` (`us-east4`). Keep these decoupled in `LakehouseSettings`.

4. **Standalone Databricks Platform vs. AWS Cloud**:
   - Treat **Databricks Unity Catalog** (accessed via its own standalone **Databricks Portal**) and **AWS Glue Data Catalog** (on **AWS S3** or **S3 Table Buckets**) as **two separate, independent external platforms** federated into Google Cloud (`us-east4`). Never model Databricks inside AWS.
   - Google Cloud Lakehouse **does not support Databricks internal `default storage`**. Federated Unity Catalog schemas must use a registered **External Location**, have **External data access** enabled on the metastore, and grant `EXTERNAL USE SCHEMA`.

5. **4-Part SQL Identifiers & Spark Iceberg REST Properties**:
   - Native BigQuery tables use 3-part names (`` `project.dataset.table` ``); federated Iceberg tables use 4-part names (`` `project.federated_catalog.namespace.table` ``). Always project explicit columns and push down `WHERE` predicates.
   - In Apache Spark (`spark/cross_cloud_margin_risk_job.py`), use `uri = https://biglake.googleapis.com/iceberg/v1/restcatalog`, `warehouse = bl://projects/<PROJECT>/catalogs/<CATALOG>`, and `rest.auth.type = org.apache.iceberg.gcp.auth.GoogleAuthManager`. Note that Intelligent Cross-Cloud Caching accelerates **BigQuery** queries; Spark executors read directly from remote storage via short-lived vended credentials.

---

## 3. Repository Commands

```bash
make install          # Install Python 3.11+ dependencies via uv
make test             # Run pytest suite + bash syntax + XML/SVG diagram checks
make doctor           # Validate CLIs, region pairing, Gemini 4 model tiering & org policies
make demo-finops      # Simulate Cold vs. Warm cache telemetry & 30-day agentic egress ROI
make inspect-agent    # Inspect tiered Gemini 4 Argon / Carbon / 3.8 Flash ADK topology
make adk-web          # Launch interactive Google ADK Dev UI
make teardown         # Uninstall / tear down all POC cloud resources
```
