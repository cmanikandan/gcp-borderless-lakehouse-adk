"""Multi-Agent Google ADK 2.x Architecture for Google Cloud Borderless Lakehouse.

Powered by `gemini-3.8-flash` and `google.adk.integrations.bigquery.BigQueryToolset`.
Unifies zero-ETL SQL analytics across Native BigQuery, standalone Databricks Unity Catalog,
and AWS Glue Data Catalog—paired with real-time Cross-Cloud Intelligent Caching FinOps.
"""

from __future__ import annotations

import google.auth
from google.auth.exceptions import DefaultCredentialsError
from google.oauth2.credentials import Credentials
from google.adk.agents import Agent
from google.adk.integrations.bigquery import (
    BigQueryCredentialsConfig,
    BigQueryToolset,
)
from google.adk.integrations.bigquery.config import (
    BigQueryToolConfig,
    WriteMode,
)

from lakehouse_agent.cache_telemetry import (
    get_federated_catalog_cli_blueprint,
    inspect_bigquery_job_cache_stats,
    simulate_agentic_burst_finops,
)
from lakehouse_agent.config import LakehouseSettings, load_settings


def build_bigquery_toolset(
    settings: LakehouseSettings,
    user_access_token: str | None = None,
) -> BigQueryToolset:
    """Instantiate a read-only BigQueryToolset with FinOps caps & optional user OAuth token.

    When `user_access_token` (or OAuth Client ID/Secret) is supplied, BigQuery executes
    queries as the authenticated end user—allowing Google Cloud Lakehouse to propagate
    end-user OIDC tokens to Databricks Unity Catalog (`user-identity-federation` mode).
    """
    tool_config = BigQueryToolConfig(
        write_mode=WriteMode.BLOCKED,
        location=settings.gcp_region,
        compute_project_id=settings.project_id,
        default_project_id=settings.project_id,
        maximum_bytes_billed=settings.max_bytes_billed,
        max_query_result_rows=200,
        application_name="borderless-lakehouse-adk-copilot",
        job_labels={
            "workload": "borderless-lakehouse-adk",
            "llm_model": "gemini-3-8-flash",
        },
    )

    if user_access_token:
        creds_config = BigQueryCredentialsConfig(
            credentials=Credentials(token=user_access_token)
        )
    elif settings.oauth_client_id and settings.oauth_client_secret:
        creds_config = BigQueryCredentialsConfig(
            client_id=settings.oauth_client_id,
            client_secret=settings.oauth_client_secret,
        )
    else:
        try:
            adc_credentials, _ = google.auth.default()
            creds_config = BigQueryCredentialsConfig(credentials=adc_credentials)
        except DefaultCredentialsError:
            # Safe fallback so unit tests & offline CLI introspection work without ADC
            creds_config = None

    return BigQueryToolset(
        credentials_config=creds_config,
        bigquery_tool_config=tool_config,
    )


def build_lakehouse_agent_system(
    settings: LakehouseSettings | None = None,
    user_access_token: str | None = None,
) -> Agent:
    """Build the Gemini 3.8 Flash multi-agent system for Borderless Lakehouse."""
    cfg = settings or load_settings()
    bq_toolset = build_bigquery_toolset(cfg, user_access_token=user_access_token)

    cross_cloud_analyst = Agent(
        name="cross_cloud_supply_chain_analyst",
        model=cfg.gemini_model,
        description=(
            "Executes read-only cross-platform SQL joins across native BigQuery datasets, "
            "standalone Databricks Unity Catalog Iceberg tables, and AWS Glue S3 Iceberg tables."
        ),
        instruction=f"""You are a Google Cloud Lakehouse Data Analyst sub-agent.
You query distributed enterprise datasets in place without moving data:
1. Native BigQuery dataset (3-part path):
   - `{cfg.project_id}.{cfg.native_bq_dataset}.orders`
2. Standalone Databricks Unity Catalog (4-part federated path):
   - `{cfg.project_id}.{cfg.unity_catalog_id}.supply_chain.supplier_risk_scores`
   - `{cfg.project_id}.{cfg.unity_catalog_id}.supply_chain.sku_margins`
3. AWS Glue Data Catalog on Amazon S3 (4-part federated path):
   - `{cfg.project_id}.{cfg.glue_catalog_id}.fulfillment.shipment_events`

Always:
- Use 4-part backticked table identifiers (`project.catalog.namespace.table`) for federated catalogs.
- Push down filters (`WHERE`) and column projections (`SELECT` specific columns, never `SELECT *`) so Lakehouse Intelligent Caching caches only required Parquet column blocks.
- Return the executed BigQuery `job_id` when available so the FinOps sub-agent can audit cache hit ratios.
""",
        tools=[bq_toolset],
    )

    finops_cache_advisor = Agent(
        name="finops_cache_telemetry_advisor",
        model=cfg.gemini_model,
        description=(
            "Audits Google Cloud Lakehouse Cross-Cloud Intelligent Caching performance "
            "(`cacheBytesRead` vs `objectStorageBytesRead`), calculates avoided egress costs, "
            "and generates GA `gcloud biglake iceberg catalogs` CLI blueprints."
        ),
        instruction="""You are a Principal Cloud FinOps & Lakehouse Architecture sub-agent.
Use your tools to:
- Inspect BigQuery job telemetry (`inspect_bigquery_job_cache_stats`) for `objectStorageStats` (`cacheBytesRead` vs `objectStorageBytesRead`).
- Quantify monthly cross-cloud egress savings (`simulate_agentic_burst_finops`) for multi-step AI agent workloads.
- Generate exact GA `gcloud biglake iceberg catalogs` commands (`get_federated_catalog_cli_blueprint`) for standalone Databricks Unity Catalog and AWS Glue Data Catalog.
Highlight when 0% cache hit indicates `--cross-cloud-cache=disabled` or `constraints/gcp.restrictNonCmekServices`.
""",
        tools=[
            inspect_bigquery_job_cache_stats,
            simulate_agentic_burst_finops,
            get_federated_catalog_cli_blueprint,
        ],
    )

    orchestrator = Agent(
        name="lakehouse_principal_copilot",
        model=cfg.gemini_model,
        description=(
            "Principal Architect Copilot orchestrating zero-ETL cross-platform analytics "
            "(BigQuery + Databricks Unity Catalog + AWS Glue) and Intelligent Caching FinOps."
        ),
        instruction="""You are a Google Cloud Principal Architect Copilot for the Borderless Lakehouse.
Route analytical data questions across BigQuery, standalone Databricks Unity Catalog, and AWS Glue
to `cross_cloud_supply_chain_analyst`.
Route cache hit telemetry audits, egress cost simulations, and catalog setup requests to
`finops_cache_telemetry_advisor`.
Synthesize clear, concise, executive-ready answers backed by SQL metrics and cache FinOps telemetry.
""",
        sub_agents=[cross_cloud_analyst, finops_cache_advisor],
    )
    return orchestrator


# Export `root_agent` for seamless `adk web src` and `adk run src/lakehouse_agent` execution
root_agent = build_lakehouse_agent_system()
