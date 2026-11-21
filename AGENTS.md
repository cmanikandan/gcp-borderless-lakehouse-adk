# AGENTS.md — AI Coding & Runtime Agent Guide

> **Proof-of-Concept (POC) Notice**: This repository is an educational cloud practitioner Proof of Concept (POC) exploring cross-cloud data federation, regional block caching, BigQuery AI, and agentic analytics. Always consult the official **Google Cloud**, **Databricks**, and **AWS** documentation for production security, IAM, networking, and governance configurations.

This repository contains end-to-end POC provisioning scripts, cross-platform SQL/Spark analytics, teardown automation, and a **Google Agent Development Kit (`google-adk` 2.15+)** multi-agent system using **tiered Gemini 4 routing** (`gemini-4-neon`, `gemini-4-carbon`, and `gemini-4-hydrogen`) on top of **Google Cloud Lakehouse** (formerly BigLake).

Use this file as the source of truth when modifying agents, SQL/Spark workloads, infrastructure scripts, or tests.

---

## 1. System Topology & Multi-Model Agent Roles

`src/lakehouse_agent/agent.py` exports both `root_agent` and `app` (`google.adk.apps.App`) and supports `MODEL_PROFILE=tiered|argon-tiered|neon|carbon|argon|hydrogen|flash` (plus per-role overrides `COORDINATOR_MODEL`, `ANALYST_MODEL`, `FINOPS_MODEL` and global `GEMINI_MODEL`):

```text
lakehouse_practitioner_copilot (Coordinator — gemini-4-neon, ThinkingLevel.LOW, max_output_tokens=8192)
├── cross_cloud_supply_chain_analyst (Sub-Agent — gemini-4-carbon, ThinkingLevel.HIGH, max_output_tokens=8192)
│   ├── discover_federated_tables (Lakehouse Iceberg REST API 4-part schema & snapshot discovery)
│   ├── SkillToolset (ADK 2.15 built-in `bigquery-ai-ml` & `bigquery-graph` skills)
│   └── BigQueryToolset (WriteMode.PROTECTED default, execute_sql + forecast + detect_anomalies + analyze_contribution)
└── finops_cache_telemetry_advisor (Sub-Agent — gemini-4-hydrogen, ThinkingLevel.LOW, max_output_tokens=8192)
    ├── inspect_bigquery_job_cache_stats (supports job_id="LATEST" via workload label lookup)
    ├── simulate_agentic_burst_finops (models multi-turn agent cache ROI & avoided egress)
    └── get_federated_catalog_cli_blueprint (emits verified GA gcloud & bq connection commands)
```

---

## 2. Technical Invariants (Do Not Violate)

1. **Google ADK 2.15+ Integration, `WriteMode.PROTECTED`, `SkillToolset` & Gemini 4 Decode Guardrails**:
   - Always import `BigQueryToolset`, `BigQueryCredentialsConfig`, and `get_bigquery_skill` from `google.adk.integrations.bigquery` (and `BigQueryToolConfig`, `WriteMode` from `google.adk.integrations.bigquery.config`). **Never** import from deprecated `google.adk.tools.bigquery`.
   - Use `resolve_write_mode_and_tools(settings)` so `BQ_WRITE_MODE=protected` (default) pairs `WriteMode.PROTECTED` with `BIGQUERY_PROTECTED_TOOLS` (dry-run blocks writes to permanent tables while allowing anonymous session temp tables for `detect_anomalies` and `analyze_contribution`), and `BQ_WRITE_MODE=blocked` pairs `WriteMode.BLOCKED` with `BIGQUERY_READONLY_TOOLS`. Do **not** expose `search_catalog` in the default tool filter unless `google-cloud-dataplex` (`--extra observability`) is installed.
   - Because **Gemini 4** models (`gemini-4-neon`, `gemini-4-carbon`, `gemini-4-hydrogen`, `gemini-4-argon`) support up to a **1M-token output decode window**, always attach `generate_content_config=types.GenerateContentConfig(max_output_tokens=cfg.max_output_tokens)` to prevent accidental runaway token bills, and sanitize `cfg.resolved_analyst_model` dynamically into `BigQueryToolConfig.job_labels["llm_model"]`.

2. **Federated Schema Discovery & `job_id="LATEST"` Telemetry**:
   - Standard `BigQueryToolset` catalog tools (`list_dataset_ids`) only enumerate 3-part native datasets and cannot list 4-part federated catalogs. Keep `discover_federated_tables` (`src/lakehouse_agent/catalog_discovery.py`) wired on the analyst agent so it can inspect namespaces, column schemas, and Iceberg snapshot IDs via `https://biglake.googleapis.com/iceberg/v1/restcatalog`.
   - `BigQueryToolConfig` tags queries with `job_labels={"workload": "borderless-lakehouse-adk", "llm_model": ..., "model_profile": ...}`, and `inspect_bigquery_job_cache_stats(job_id="LATEST")` automatically resolves the latest matching completed query job in `BQ_LOCATION`.

3. **Decoupled Global Gemini Endpoint vs. Regional BigQuery Cache**:
   - On Vertex AI / Gemini Enterprise Agent Platform (`GOOGLE_GENAI_USE_VERTEXAI=TRUE`), serving Gemini 4 models via `GOOGLE_CLOUD_LOCATION=global` avoids the 10% regional token premium, whereas BigQuery datasets and Lakehouse regional caches stay co-located in `BQ_LOCATION` / `GCP_REGION` (`us-east4`). Keep these decoupled in `LakehouseSettings`.

4. **Standalone Databricks Platform vs. AWS Cloud**:
   - Treat **Databricks Unity Catalog** (accessed via its own standalone **Databricks Portal**) and **AWS Glue Data Catalog** (on **AWS S3** or **Amazon S3 Table Buckets**) as **two separate, independent external platforms** federated into Google Cloud (`us-east4`). Never model Databricks inside AWS.
   - Google Cloud Lakehouse **does not support Databricks internal `default storage`**. Federated Unity Catalog schemas must use a registered **External Location**, have **External data access** enabled on the metastore, and grant `EXTERNAL USE SCHEMA`.

5. **4-Part SQL Identifiers & Spark Iceberg REST Properties**:
   - Native BigQuery tables use 3-part names (`` `project.dataset.table` ``); federated Iceberg tables use 4-part names (`` `project.federated_catalog.namespace.table` ``). Always project explicit columns and push down `WHERE` predicates.
   - In Apache Spark (`spark/cross_cloud_margin_risk_job.py`), use `uri = https://biglake.googleapis.com/iceberg/v1/restcatalog`, `warehouse = bl://projects/<PROJECT>/catalogs/<CATALOG>`, and `rest.auth.type = org.apache.iceberg.gcp.auth.GoogleAuthManager`. Cross-cloud caching accelerates **BigQuery** queries; Spark executors read directly from remote storage via short-lived vended credentials.

---

## 3. Repository Commands

```bash
make install          # Install Python 3.11+ dependencies via uv
make test             # Run pytest suite + bash syntax + XML/SVG diagram checks
make doctor           # Validate CLIs, region pairing, Gemini 4 model tiering & org policies
make demo-finops      # Simulate Cold vs. Warm cache telemetry & 30-day agentic egress ROI
make inspect-agent    # Inspect tiered Gemini 4 Neon / Carbon / Hydrogen ADK 2.15 topology
make adk-web          # Launch interactive Google ADK Dev UI
make teardown         # Uninstall / tear down all POC cloud resources
```
