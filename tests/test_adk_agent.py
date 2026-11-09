"""Unit tests verifying Google ADK 2.x multi-agent topology, guardrails, and Gemini 3.8 Flash config."""

from __future__ import annotations

import warnings

from google.adk.integrations.bigquery.config import WriteMode

from lakehouse_agent.agent import build_bigquery_toolset, build_lakehouse_agent_system
from lakehouse_agent.config import LakehouseSettings


def test_adk_multi_agent_wiring_and_no_deprecation_warnings() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", DeprecationWarning)
        settings = LakehouseSettings(
            project_id="test-lakehouse-proj",
            gcp_region="us-east4",
            gemini_model="gemini-3.8-flash",
        )
        orchestrator = build_lakehouse_agent_system(settings=settings)

    adk_deprecations = [
        w for w in caught if "google.adk" in str(w.message)
    ]
    assert adk_deprecations == [], f"Unexpected ADK deprecations: {adk_deprecations}"

    assert orchestrator.name == "lakehouse_practitioner_copilot"
    assert orchestrator.model == "gemini-3.8-flash"
    assert len(orchestrator.sub_agents) == 2

    sub_names = {sub.name for sub in orchestrator.sub_agents}
    assert sub_names == {
        "cross_cloud_supply_chain_analyst",
        "finops_cache_telemetry_advisor",
    }


def test_bigquery_toolset_blocks_writes_and_supports_end_user_oauth_token() -> None:
    settings = LakehouseSettings(
        project_id="test-lakehouse-proj",
        gcp_region="us-east4",
        max_bytes_billed=5 * 1024**3,
    )
    toolset = build_bigquery_toolset(
        settings=settings,
        user_access_token="ya29.mock-end-user-oauth-token",
    )
    # Verify read-only WriteMode.BLOCKED guardrail and location binding
    tool_cfg = toolset._tool_settings
    assert tool_cfg.write_mode == WriteMode.BLOCKED
    assert tool_cfg.location == "us-east4"
    assert tool_cfg.maximum_bytes_billed == 5 * 1024**3
