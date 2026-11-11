"""Unit tests verifying Google ADK 2.x multi-agent topology, ThinkingLevel planners, and guardrails."""

from __future__ import annotations

import warnings

from google.adk.integrations.bigquery.config import WriteMode
from google.adk.planners import BuiltInPlanner
from google.genai import types

from lakehouse_agent.agent import (
    BIGQUERY_ALLOWED_TOOLS,
    build_bigquery_toolset,
    build_lakehouse_agent_system,
)
from lakehouse_agent.config import LakehouseSettings, load_settings


def test_adk_multi_agent_wiring_planners_and_no_deprecation_warnings() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", DeprecationWarning)
        settings = LakehouseSettings(
            project_id="test-lakehouse-proj",
            gcp_region="us-east4",
            gemini_model="gemini-3.8-flash",
        )
        orchestrator = build_lakehouse_agent_system(settings=settings)

    adk_deprecations = [w for w in caught if "google.adk" in str(w.message)]
    assert adk_deprecations == [], f"Unexpected ADK deprecations: {adk_deprecations}"

    assert orchestrator.name == "lakehouse_practitioner_copilot"
    assert orchestrator.model == "gemini-3.8-flash"
    assert isinstance(orchestrator.planner, BuiltInPlanner)
    assert orchestrator.planner.thinking_config.thinking_level == types.ThinkingLevel.LOW
    assert len(orchestrator.sub_agents) == 2

    sub_by_name = {sub.name: sub for sub in orchestrator.sub_agents}
    assert set(sub_by_name.keys()) == {
        "cross_cloud_supply_chain_analyst",
        "finops_cache_telemetry_advisor",
    }
    assert (
        sub_by_name["cross_cloud_supply_chain_analyst"].planner.thinking_config.thinking_level
        == types.ThinkingLevel.MEDIUM
    )


def test_bigquery_toolset_blocks_writes_filters_tools_and_supports_oauth_token() -> None:
    settings = LakehouseSettings(
        project_id="test-lakehouse-proj",
        gcp_region="us-east4",
        max_bytes_billed=5 * 1024**3,
    )
    toolset = build_bigquery_toolset(
        settings=settings,
        user_access_token="ya29.mock-end-user-oauth-token",
    )
    tool_cfg = toolset._tool_settings
    assert tool_cfg.write_mode == WriteMode.BLOCKED
    assert tool_cfg.location == "us-east4"
    assert tool_cfg.maximum_bytes_billed == 5 * 1024**3
    assert toolset.tool_filter == BIGQUERY_ALLOWED_TOOLS


def test_dynamic_load_settings_decouples_vertex_and_bigquery_locations(monkeypatch) -> None:
    monkeypatch.setenv("PROJECT_ID", "dynamic-poc-proj")
    monkeypatch.delenv("GOOGLE_CLOUD_PROJECT", raising=False)
    monkeypatch.setenv("GOOGLE_CLOUD_LOCATION", "global")
    monkeypatch.setenv("BQ_LOCATION", "us-east4")

    cfg = load_settings(load_env_file=False)
    assert cfg.project_id == "dynamic-poc-proj"
    assert cfg.vertex_location == "global"
    assert cfg.gcp_region == "us-east4"
