# AGENTS.md — AI Coding & Runtime Agent Guide

This repository contains reference infrastructure, cross-platform SQL/Spark analytics, and a production-styled **Google Agent Development Kit (`google-adk` 2.x)** multi-agent system powered by **`gemini-3.8-flash`** on top of **Google Cloud Borderless Lakehouse**.

Use this file as the source of truth when modifying agents, SQL/Spark workloads, infrastructure scripts, or tests.

---

## 1. System Topology & Agent Roles

`src/lakehouse_agent/agent.py` exports `root_agent` so the project runs out-of-the-box with `adk web src` and `adk run src/lakehouse_agent`:

```text
lakehouse_principal_copilot (Root Orchestrator — gemini-3.8-flash)
├── cross_cloud_supply_chain_analyst (Sub-Agent — gemini-3.8-flash)
│   └── BigQueryToolset (WriteMode.BLOCKED, max_bytes_billed cap, 4-part federated SQL)
└── finops_cache_telemetry_advisor (Sub-Agent — gemini-3.8-flash)
    ├── inspect_bigquery_job_cache_stats (parses statistics.query.objectStorageStats)
    ├── simulate_agentic_burst_finops (models multi-turn agent cache ROI)
    └── get_federated_catalog_cli_blueprint (emits verified GA gcloud commands)
```

---

## 2. Architectural Invariants (Do Not Violate)

1. **Google ADK 2.x Integration Path**:
   - Always import `BigQueryToolset` and `BigQueryCredentialsConfig` from `google.adk.integrations.bigquery` (and `BigQueryToolConfig`, `WriteMode` from `google.adk.integrations.bigquery.config`).
   - **Never** import from `google.adk.tools.bigquery` — that module path is deprecated in `google-adk >= 2.x`.
   - When Application Default Credentials (ADC) are absent in unit tests or CI, pass `credentials_config=None` to `BigQueryToolset` rather than instantiating an empty `BigQueryCredentialsConfig()`.

2. **Standalone Databricks Platform vs. AWS Cloud**:
   - Treat **Databricks Unity Catalog** (accessed via its own standalone **Databricks Portal / Free Edition / Community Edition**) and **AWS Glue Data Catalog** (on **AWS S3**) as **two separate, independent external platforms** federated into Google Cloud (`us-east4`). Do **not** assume or hardcode Databricks inside AWS.
   - Google Cloud Lakehouse **does not support Databricks internal `default storage`**. Every Unity Catalog schema/table federated to Lakehouse must use a registered **Unity Catalog External Location**, have **External data access** enabled on the metastore, and grant `EXTERNAL USE SCHEMA` to the service principal.

3. **4-Part Federated Table Identifiers**:
   - Native BigQuery tables use 3-part names: `` `project.dataset.table` ``.
   - Federated Iceberg tables (Databricks Unity Catalog & AWS Glue) **must** use backticked 4-part names: `` `project.federated_catalog.namespace.table` ``.
   - Instruct SQL agents to project explicit columns and push down `WHERE` predicates (never `SELECT *`) so Lakehouse Intelligent Caching stores only required Parquet column blocks.

4. **Read-Only Guardrails & User Identity Propagation**:
   - Keep `WriteMode.BLOCKED` and `maximum_bytes_billed` enforced on `BigQueryToolConfig` (cross-cloud federated catalogs are read-only in GA).
   - Preserve the `user_access_token` path in `build_bigquery_toolset()` so end-user OAuth credentials can flow through BigQuery into Databricks Unity Catalog when `--federated-catalog-identity-mode="user-identity-federation"` is enabled.

5. **Intelligent Cross-Cloud Caching Telemetry**:
   - Cross-cloud caching (`--cross-cloud-cache="enabled"`) is **disabled by default** in GA and must be explicitly enabled.
   - Verify cache hits via BigQuery job statistics `statistics.query.objectStorageStats` (`cacheBytesRead` vs. `objectStorageBytesRead`). If `cacheBytesRead` remains `0` on warm runs, check both `--cross-cloud-cache="enabled"` and org policy `constraints/gcp.restrictNonCmekServices`.

---

## 3. Repository Commands

```bash
# Install dependencies (Python 3.11+ via uv)
uv sync --extra dev

# Run unit test suite (verifies ADK 2.x wiring, zero deprecation warnings, and FinOps parser)
uv run pytest -v

# Run offline Cold vs. Warm Intelligent Cache & Agentic FinOps benchmark demo
uv run python scripts/demo_agent_cache_finops.py

# Inspect a live BigQuery cross-cloud job's cache hit ratio
uv run python scripts/demo_agent_cache_finops.py --live-job-id <BQ_JOB_ID> --location us-east4

# Launch interactive Google ADK Dev UI
uv run adk web src

# Validate shell scripts and Draw.io / SVG XML diagrams
for f in infra/*/*.sh spark/*.sh; do bash -n "$f"; done
uv run python -c "import xml.etree.ElementTree as ET; ET.parse('docs/diagrams/borderless_lakehouse_architecture.drawio'); ET.parse('docs/diagrams/borderless_lakehouse_architecture.svg')"
```
