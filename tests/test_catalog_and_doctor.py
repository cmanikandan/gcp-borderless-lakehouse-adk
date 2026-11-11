"""Unit tests for Federated Iceberg Catalog REST Discovery, job_id='LATEST' resolution, and Preflight Doctor."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from lakehouse_agent.cache_telemetry import (
    get_federated_catalog_cli_blueprint,
    inspect_bigquery_job_cache_stats,
)
from lakehouse_agent.catalog_discovery import discover_federated_catalog_tables


def test_discover_federated_catalog_tables_parses_iceberg_rest_responses(monkeypatch) -> None:
    def fake_get(url: str, headers: dict, timeout: int):
        resp = MagicMock()
        resp.ok = True
        resp.raise_for_status = lambda: None
        if url.endswith("/namespaces"):
            resp.json.return_value = {"namespaces": [["supply_chain"]]}
        elif url.endswith("/namespaces/supply_chain/tables"):
            resp.json.return_value = {
                "identifiers": [{"namespace": ["supply_chain"], "name": "sku_margins"}]
            }
        elif url.endswith("/namespaces/supply_chain/tables/sku_margins"):
            resp.json.return_value = {
                "metadata": {
                    "location": "gs://demo-uc-ext/supply_chain/sku_margins",
                    "current-snapshot-id": 88123456789,
                    "current-schema-id": 0,
                    "schemas": [
                        {
                            "schema-id": 0,
                            "fields": [
                                {"name": "sku_id", "type": "string", "required": True},
                                {"name": "unit_cost_usd", "type": "double", "required": False},
                            ],
                        }
                    ],
                }
            }
        return resp

    fake_session = MagicMock()
    fake_session.get.side_effect = fake_get
    monkeypatch.setattr(
        "lakehouse_agent.catalog_discovery.AuthorizedSession",
        lambda creds: fake_session,
    )

    result = discover_federated_catalog_tables(
        catalog_id="dbx_unity_merch",
        project_id="test-proj",
        user_access_token="ya29.test-token",
    )
    assert result["status"] == "SUCCESS"
    assert result["table_count"] == 1
    tbl = result["tables"][0]
    assert tbl["bigquery_sql_identifier"] == "`test-proj.dbx_unity_merch.supply_chain.sku_margins`"
    assert tbl["current_snapshot_id"] == 88123456789
    assert [c["name"] for c in tbl["columns"]] == ["sku_id", "unit_cost_usd"]


def test_inspect_bigquery_job_cache_stats_resolves_latest_adk_job(monkeypatch) -> None:
    listed_adk_job = SimpleNamespace(
        job_id="bquxjob_latest_adk_99",
        job_type="query",
        location="us-east4",
        labels={"workload": "borderless-lakehouse-adk"},
    )
    detailed_job = SimpleNamespace(
        job_id="bquxjob_latest_adk_99",
        _properties={
            "statistics": {
                "query": {
                    "objectStorageStats": [
                        {
                            "cloudProvider": "AWS",
                            "cacheBytesRead": "46200000000",
                            "objectStorageBytesRead": "1800000000",
                        }
                    ]
                }
            }
        },
    )

    fake_bq_client = MagicMock()
    fake_bq_client.list_jobs.return_value = [listed_adk_job]
    fake_bq_client.get_job.return_value = detailed_job

    monkeypatch.setattr(
        "lakehouse_agent.cache_telemetry.bigquery.Client",
        lambda project, location: fake_bq_client,
    )

    report = inspect_bigquery_job_cache_stats(job_id="LATEST", location="us-east4")
    assert report["job_id"] == "bquxjob_latest_adk_99"
    assert report["overall_cache_hit_ratio_pct"] == 96.25


def test_bq_cross_cloud_connection_cli_blueprint() -> None:
    bp = get_federated_catalog_cli_blueprint("bq-connection", "aws_s3_cached_conn")
    assert "bq mk --connection" in bp["gcloud_command"]
    assert '"crossCloudCacheOptions":{"enabled":true}' in bp["gcloud_command"]
