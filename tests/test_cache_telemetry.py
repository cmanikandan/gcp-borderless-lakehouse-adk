"""Unit tests for Google Cloud Lakehouse Cross-Cloud Intelligent Caching FinOps Telemetry."""

from __future__ import annotations

import pytest

from lakehouse_agent.cache_telemetry import (
    evaluate_job_finops,
    get_federated_catalog_cli_blueprint,
    parse_object_storage_stats,
    simulate_agentic_burst_finops,
)


def test_parse_camel_case_rest_object_storage_stats() -> None:
    raw = {
        "statistics": {
            "query": {
                "objectStorageStats": [
                    {
                        "cloudProvider": "AWS",
                        "cacheBytesRead": "8000000000",
                        "objectStorageBytesRead": "2000000000",
                    }
                ]
            }
        }
    }
    stats = parse_object_storage_stats(raw)
    assert len(stats) == 1
    assert stats[0].cloud_provider == "AWS"
    assert stats[0].cache_bytes_read == 8_000_000_000
    assert stats[0].object_storage_bytes_read == 2_000_000_000
    assert stats[0].cache_hit_ratio_pct == 80.0


def test_parse_snake_case_proto_object_storage_stats() -> None:
    raw = {
        "statistics": {
            "query": {
                "object_storage_stats": [
                    {
                        "cloud_provider": "azure",
                        "cache_bytes_read": 9_500_000_000,
                        "object_storage_bytes_read": 500_000_000,
                    }
                ]
            }
        }
    }
    report = evaluate_job_finops("job-proto-1", "us-east4", raw)
    assert report.overall_cache_hit_ratio_pct == 95.0
    assert report.avoided_public_egress_usd == pytest.approx(0.855, rel=1e-3)
    assert "Optimal cache efficiency" in report.architectural_recommendation


def test_cold_read_flags_cache_disabled_or_cmek_org_policy() -> None:
    raw = {
        "statistics": {
            "query": {
                "objectStorageStats": [
                    {
                        "cloudProvider": "AWS",
                        "cacheBytesRead": "0",
                        "objectStorageBytesRead": "15000000000",
                    }
                ]
            }
        }
    }
    report = evaluate_job_finops("job-cold-1", "us-east4", raw)
    assert report.overall_cache_hit_ratio_pct == 0.0
    assert "--cross-cloud-cache=enabled" in report.architectural_recommendation
    assert "restrictNonCmekServices" in report.architectural_recommendation


def test_simulate_agentic_burst_finops() -> None:
    sim = simulate_agentic_burst_finops(
        dataset_scanned_gb_per_query=50.0,
        queries_per_agent_investigation=5,
        daily_investigations=20,
    )
    assert sim["monthly_agent_sql_queries"] == 3000
    assert sim["uncached_remote_s3_egress_gb"] == 150_000.0
    assert sim["monthly_savings_with_cache_usd"] > 10_000.0


def test_cli_blueprints_for_unity_and_glue() -> None:
    unity = get_federated_catalog_cli_blueprint("unity", "dbx_unity_merch")
    assert '--federated-catalog-type="unity"' in unity["gcloud_command"]
    assert '--cross-cloud-cache="enabled"' in unity["gcloud_command"]
    assert '--federated-catalog-identity-mode="user-identity-federation"' in unity["gcloud_command"]

    glue = get_federated_catalog_cli_blueprint("glue", "aws_glue_logistics")
    assert '--federated-catalog-type="glue"' in glue["gcloud_command"]
    assert "--glue-warehouse=" in glue["gcloud_command"]
    assert '--cross-cloud-cache="enabled"' in glue["gcloud_command"]
