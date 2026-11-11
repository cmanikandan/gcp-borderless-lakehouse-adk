"""Multi-Agent Google ADK 2.x Suite for Google Cloud Borderless Lakehouse POC.

Implements heterogeneous Gemini model routing across roles (`MODEL_PROFILE=tiered`):
- `lakehouse_practitioner_copilot` -> `gemini-4-argon` (`ThinkingLevel.LOW`) for fast
  frontier intent decomposition, routing, and concise synthesis.
- `cross_cloud_supply_chain_analyst` -> `gemini-4-carbon` (`ThinkingLevel.HIGH`) for
  deep multi-table SQL reasoning, schema verification across BigQuery + Databricks Unity
  Catalog + AWS Glue, and zero-shot TimesFM `AI.FORECAST` analysis.
- `finops_cache_telemetry_advisor` -> `gemini-3.8-flash` (`ThinkingLevel.LOW`) for rapid,
  cost-efficient `objectStorageStats` inspection and CLI blueprint generation.

Also supports uniform single-model profiles (`argon`, `carbon`, `flash`) and explicit
per-turn `max_output_tokens` guardrails for Gemini 4's 1M-token decode window.
"""

from __future__ import annotations

from typing import Any

import google.auth
from google.adk.agents import Agent
from google.adk.integrations.bigquery import (
    BigQueryCredentialsConfig,
    BigQueryToolset,
)
from google.adk.integrations.bigquery.config import (
    BigQueryToolConfig,
    WriteMode,
)
from google.adk.planners import BuiltInPlanner
from google.auth.exceptions import DefaultCredentialsError
from google.genai import types
from google.oauth2.credentials import Credentials

from lakehouse_agent.cache_telemetry import (
    get_federated_catalog_cli_blueprint,
    inspect_bigquery_job_cache_stats,
    simulate_agentic_burst_finops,
)
from lakehouse_agent.catalog_discovery import discover_federated_catalog_tables
from lakehouse_agent.config import LakehouseSettings, load_settings, sanitize_bq_label

# Explicit allowlist compatible with WriteMode.BLOCKED & zero extra optional dependencies
# (`execute_sql` + `forecast` work directly on 4-part federated Iceberg tables!)
BIGQUERY_ALLOWED_TOOLS: list[str] = [
    "execute_sql",
    "forecast",
    "list_dataset_ids",
    "get_dataset_info",
    "list_table_ids",
    "get_table_info",
    "get_job_info",
]


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
            "llm_model": sanitize_bq_label(settings.resolved_analyst_model),
            "model_profile": sanitize_bq_label(settings.model_profile),
        },
    )

    if user_access_token:
        creds_config: BigQueryCredentialsConfig | None = BigQueryCredentialsConfig(
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
        tool_filter=BIGQUERY_ALLOWED_TOOLS,
        credentials_config=creds_config,
        bigquery_tool_config=tool_config,
    )


def _analyst_thinking_level(model_id: str) -> types.ThinkingLevel:
    """Select deeper reasoning on Gemini 4 Carbon while keeping Argon/Flash responsive."""
    if "carbon" in model_id.lower():
        return types.ThinkingLevel.HIGH
    return types.ThinkingLevel.MEDIUM


def build_lakehouse_agent_system(
    settings: LakehouseSettings | None = None,
    user_access_token: str | None = None,
) -> Agent:
    """Build the multi-model Google ADK 2.x system for Borderless Lakehouse."""
    cfg = settings or load_settings()
    bq_toolset = build_bigquery_toolset(cfg, user_access_token=user_access_token)

    coord_model = cfg.resolved_coordinator_model
    analyst_model = cfg.resolved_analyst_model
    finops_model = cfg.resolved_finops_model

    def discover_federated_tables(
        catalog_id: str,
        namespace: str = "",
        include_schema: bool = True,
    ) -> dict[str, Any]:
        """Discover namespaces, Iceberg tables, 4-part SQL identifiers, and column schemas in a federated Lakehouse catalog (`dbx_unity_merch` or `aws_glue_logistics`)."""
        return discover_federated_catalog_tables(
            catalog_id=catalog_id,
            namespace=namespace,
            include_schema=include_schema,
            project_id=cfg.project_id,
            user_access_token=user_access_token,
        )

    router_planner = BuiltInPlanner(
        thinking_config=types.ThinkingConfig(
            thinking_level=types.ThinkingLevel.LOW,
            include_thoughts=True,
        )
    )
    analyst_planner = BuiltInPlanner(
        thinking_config=types.ThinkingConfig(
            thinking_level=_analyst_thinking_level(analyst_model),
            include_thoughts=True,
        )
    )
    finops_planner = BuiltInPlanner(
        thinking_config=types.ThinkingConfig(
            thinking_level=types.ThinkingLevel.LOW,
            include_thoughts=True,
        )
    )

    # Explicit max_output_tokens cap prevents runaway spend on Gemini 4's 1M-token decode window
    bounded_gen_config = types.GenerateContentConfig(
        max_output_tokens=cfg.max_output_tokens,
    )

    cross_cloud_analyst = Agent(
        name="cross_cloud_supply_chain_analyst",
        model=analyst_model,
        planner=analyst_planner,
        generate_content_config=bounded_gen_config,
        description=(
            "Discovers federated Iceberg schemas and executes read-only cross-platform SQL joins "
            "and TimesFM forecasts across BigQuery, standalone Databricks Unity Catalog, and AWS Glue."
        ),
        instruction=f"""You are a Google Cloud Lakehouse Data Analyst sub-agent.
You query distributed datasets in place without moving data:
1. Native BigQuery dataset (3-part path):
   - `{cfg.project_id}.{cfg.native_bq_dataset}.orders`
2. Standalone Databricks Unity Catalog (4-part federated path):
   - `{cfg.project_id}.{cfg.unity_catalog_id}.supply_chain.supplier_risk_scores`
   - `{cfg.project_id}.{cfg.unity_catalog_id}.supply_chain.sku_margins`
3. AWS Glue Data Catalog on Amazon S3 (4-part federated path):
   - `{cfg.project_id}.{cfg.glue_catalog_id}.fulfillment.shipment_events`

Always:
- Call `discover_federated_tables` when asked to explore unknown schemas/tables inside `{cfg.unity_catalog_id}` or `{cfg.glue_catalog_id}` (standard `list_dataset_ids` only sees 3-part BigQuery datasets).
- Use 4-part backticked table identifiers (`project.catalog.namespace.table`) in `execute_sql`.
- Push down filters (`WHERE`) and explicit column projections (never `SELECT *`) so Lakehouse Intelligent Caching caches only needed Parquet column blocks.
""",
        tools=[bq_toolset, discover_federated_tables],
    )

    finops_cache_advisor = Agent(
        name="finops_cache_telemetry_advisor",
        model=finops_model,
        planner=finops_planner,
        generate_content_config=bounded_gen_config,
        description=(
            "Audits Google Cloud Lakehouse Cross-Cloud Intelligent Caching performance "
            "(`cacheBytesRead` vs `objectStorageBytesRead`), calculates avoided egress costs, "
            "and generates GA `gcloud biglake` and `bq mk --connection` CLI blueprints."
        ),
        instruction="""You are a Cloud FinOps & Lakehouse Practitioner sub-agent.
Use your tools to:
- Inspect BigQuery job telemetry (`inspect_bigquery_job_cache_stats`, passing `job_id='LATEST'` when auditing the most recent SQL query) for `objectStorageStats` (`cacheBytesRead` vs `objectStorageBytesRead`).
- Quantify monthly cross-cloud egress savings (`simulate_agentic_burst_finops`) for multi-step AI agent workloads.
- Generate exact GA CLI commands (`get_federated_catalog_cli_blueprint`) for standalone Databricks Unity Catalog (`unity`), AWS Glue (`glue`), or BigQuery Cross-Cloud Connections (`bq-connection`).
Highlight when 0% cache hit indicates `--cross-cloud-cache=disabled` or `constraints/gcp.restrictNonCmekServices`.
""",
        tools=[
            inspect_bigquery_job_cache_stats,
            simulate_agentic_burst_finops,
            get_federated_catalog_cli_blueprint,
        ],
    )

    orchestrator = Agent(
        name="lakehouse_practitioner_copilot",
        model=coord_model,
        planner=router_planner,
        generate_content_config=bounded_gen_config,
        description=(
            "Cloud Practitioner POC Copilot orchestrating zero-ETL cross-platform analytics "
            "(BigQuery + Databricks Unity Catalog + AWS Glue) and Intelligent Caching FinOps."
        ),
        instruction="""You are a Cloud Practitioner Copilot for the Google Cloud Borderless Lakehouse POC.
Route schema discovery and analytical SQL / forecasting questions across BigQuery, standalone
Databricks Unity Catalog, and AWS Glue to `cross_cloud_supply_chain_analyst`.
Route cache hit telemetry audits (`job_id='LATEST'`), egress cost simulations, and catalog/connection
CLI requests to `finops_cache_telemetry_advisor`.
Synthesize clear, concise, actionable answers backed by SQL metrics and cache FinOps telemetry.
""",
        sub_agents=[cross_cloud_analyst, finops_cache_advisor],
    )
    return orchestrator


# Export `root_agent` for seamless `adk web src` and `adk run src/lakehouse_agent` execution
root_agent = build_lakehouse_agent_system()
