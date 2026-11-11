"""Unit tests verifying Google ADK 2.x multi-agent topology, Gemini 4 tiering, and guardrails."""

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


def test_default_tiered_gemini_4_argon_carbon_and_flash_wiring() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", DeprecationWarning)
        settings = LakehouseSettings(
            project_id="test-lakehouse-proj",
            gcp_region="us-east4",
            model_profile="tiered",
            max_output_tokens=8192,
        )
        orchestrator = build_lakehouse_agent_system(settings=settings)

    adk_deprecations = [w for w in caught if "google.adk" in str(w.message)]
    assert adk_deprecations == [], f"Unexpected ADK deprecations: {adk_deprecations}"

    assert orchestrator.name == "lakehouse_practitioner_copilot"
    assert orchestrator.model == "gemini-4-argon"
    assert isinstance(orchestrator.planner, BuiltInPlanner)
    assert orchestrator.planner.thinking_config.thinking_level == types.ThinkingLevel.LOW
    assert orchestrator.generate_content_config.max_output_tokens == 8192
    assert len(orchestrator.sub_agents) == 2

    sub_by_name = {sub.name: sub for sub in orchestrator.sub_agents}
    analyst = sub_by_name["cross_cloud_supply_chain_analyst"]
    finops = sub_by_name["finops_cache_telemetry_advisor"]

    assert analyst.model == "gemini-4-carbon"
    assert analyst.planner.thinking_config.thinking_level == types.ThinkingLevel.HIGH
    assert analyst.generate_content_config.max_output_tokens == 8192

    assert finops.model == "gemini-3.8-flash"
    assert finops.planner.thinking_config.thinking_level == types.ThinkingLevel.LOW


def test_single_model_override_and_dynamic_bq_job_labels() -> None:
    settings = LakehouseSettings(
        project_id="test-lakehouse-proj",
        gcp_region="us-east4",
        gemini_model="gemini-4-argon",
        max_bytes_billed=5 * 1024**3,
    )
    orchestrator = build_lakehouse_agent_system(settings=settings)
    assert orchestrator.model == "gemini-4-argon"
    for sub in orchestrator.sub_agents:
        assert sub.model == "gemini-4-argon"

    toolset = build_bigquery_toolset(
        settings=settings,
        user_access_token="ya29.mock-end-user-oauth-token",
    )
    tool_cfg = toolset._tool_settings
    assert tool_cfg.write_mode == WriteMode.BLOCKED
    assert tool_cfg.location == "us-east4"
    assert tool_cfg.maximum_bytes_billed == 5 * 1024**3
    assert tool_cfg.job_labels["llm_model"] == "gemini-4-argon"
    assert toolset.tool_filter == BIGQUERY_ALLOWED_TOOLS


def test_dynamic_load_settings_profiles_and_endpoint_separation(monkeypatch) -> None:
    monkeypatch.setenv("PROJECT_ID", "dynamic-poc-proj")
    monkeypatch.delenv("GOOGLE_CLOUD_PROJECT", raising=False)
    monkeypatch.delenv("GEMINI_MODEL", raising=False)
    monkeypatch.setenv("MODEL_PROFILE", "carbon")
    monkeypatch.setenv("FINOPS_MODEL", "gemini-3.8-flash")
    monkeypatch.setenv("GOOGLE_CLOUD_LOCATION", "global")
    monkeypatch.setenv("BQ_LOCATION", "us-east4")
    monkeypatch.setenv("MAX_OUTPUT_TOKENS", "12288")

    cfg = load_settings(load_env_file=False)
    assert cfg.project_id == "dynamic-poc-proj"
    assert cfg.vertex_location == "global"
    assert cfg.gcp_region == "us-east4"
    assert cfg.resolved_coordinator_model == "gemini-4-carbon"
    assert cfg.resolved_analyst_model == "gemini-4-carbon"
    assert cfg.resolved_finops_model == "gemini-3.8-flash"
    assert cfg.max_output_tokens == 12288
