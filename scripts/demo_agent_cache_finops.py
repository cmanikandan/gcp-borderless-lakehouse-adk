#!/usr/bin/env python3
"""Interactive CLI Demo: Cold vs. Warm Intelligent Cache Telemetry & Agentic FinOps.

Demonstrates how Google Cloud Borderless Lakehouse's GA Intelligent Caching
(`statistics.query.objectStorageStats`) transforms cross-platform economics when
running multi-step Google ADK agent investigations (`gemini-4-argon`, `gemini-4-carbon`,
and `gemini-3.8-flash`) across AWS Glue (S3) and standalone Databricks Unity Catalog.

Usage:
  uv run python scripts/demo_agent_cache_finops.py
  uv run python scripts/demo_agent_cache_finops.py --live-job-id bquxjob_1234 --location us-east4
"""

from __future__ import annotations

import argparse
import json

from lakehouse_agent.agent import build_lakehouse_agent_system
from lakehouse_agent.cache_telemetry import (
    evaluate_job_finops,
    get_federated_catalog_cli_blueprint,
    inspect_bigquery_job_cache_stats,
    simulate_agentic_burst_finops,
)


def run_offline_benchmark_showcase() -> None:
    print("=" * 84)
    print(" Google Cloud Borderless Lakehouse + Tiered Gemini 4 ADK — FinOps Showcase")
    print("=" * 84)

    agent = build_lakehouse_agent_system()
    print(f"\n[1] Verified ADK Coordinator : {agent.name} (model={agent.model})")
    for sub in agent.sub_agents:
        print(f"    └── Specialist Sub-Agent : {sub.name} (model={sub.model})")

    # Simulated BigQuery Job 1: Initial Cold Cross-Platform Join (0% Cache Hit)
    cold_job_payload = {
        "statistics": {
            "query": {
                "objectStorageStats": [
                    {
                        "cloudProvider": "AWS",
                        "cacheBytesRead": "0",
                        "objectStorageBytesRead": str(48_000_000_000),  # 48 GB remote read
                    }
                ]
            }
        }
    }

    # Simulated BigQuery Job 2: Warm Agentic Follow-up Query (96.25% Cache Hit in us-east4!)
    warm_job_payload = {
        "statistics": {
            "query": {
                "objectStorageStats": [
                    {
                        "cloudProvider": "AWS",
                        "cacheBytesRead": str(46_200_000_000),         # 46.2 GB served from local cache!
                        "objectStorageBytesRead": str(1_800_000_000),  # Only 1.8 GB delta snapshot read
                    }
                ]
            }
        }
    }

    cold_report = evaluate_job_finops("bquxjob_cold_001", "us-east4", cold_job_payload)
    warm_report = evaluate_job_finops("bquxjob_warm_002", "us-east4", warm_job_payload)

    print("\n[2] BigQuery `statistics.query.objectStorageStats` Telemetry Comparison:")
    print("-" * 84)
    print(f"{'Metric':<34} | {'Run #1 (Cold Scan)':<21} | {'Run #2 (Warm Agent Scan)':<22}")
    print("-" * 84)
    print(
        f"{'Remote Object Bytes Read':<34} | "
        f"{cold_report.total_remote_bytes_read / 1e9:>18.2f} GB | "
        f"{warm_report.total_remote_bytes_read / 1e9:>19.2f} GB"
    )
    print(
        f"{'Local Google Cloud Cache Read':<34} | "
        f"{cold_report.total_cache_bytes_read / 1e9:>18.2f} GB | "
        f"{warm_report.total_cache_bytes_read / 1e9:>19.2f} GB"
    )
    print(
        f"{'Cache Hit Ratio (%)':<34} | "
        f"{cold_report.overall_cache_hit_ratio_pct:>19.2f}% | "
        f"{warm_report.overall_cache_hit_ratio_pct:>20.2f}%"
    )
    print(
        f"{'Avoided Public Egress ($0.09/GB)':<34} | "
        f"${cold_report.avoided_public_egress_usd:>19.2f} | "
        f"${warm_report.avoided_public_egress_usd:>20.2f}"
    )
    print("-" * 84)
    print(f"Warm Run Guidance: {warm_report.architectural_recommendation}")

    print("\n[3] Monthly Agentic Workload Simulation (40 investigations/day × 6 SQL calls):")
    sim = simulate_agentic_burst_finops(
        dataset_scanned_gb_per_query=48.0,
        queries_per_agent_investigation=6,
        daily_investigations=40,
    )
    print(json.dumps(sim, indent=2))

    print("\n[4] Generated Standalone Databricks Unity Catalog Federation Command:")
    blueprint = get_federated_catalog_cli_blueprint(
        provider="unity",
        catalog_name="dbx_unity_merch",
        gcp_region="us-east4",
        enable_cache=True,
        enable_user_identity=True,
    )
    print(blueprint["gcloud_command"])
    print("=" * 84)


def main() -> None:
    parser = argparse.ArgumentParser(description="Borderless Lakehouse FinOps & ADK Demo")
    parser.add_argument("--live-job-id", help="Inspect a live BigQuery job ID in your project")
    parser.add_argument("--location", default="us-east4", help="BigQuery job location")
    args = parser.parse_args()

    if args.live_job_id:
        report = inspect_bigquery_job_cache_stats(args.live_job_id, location=args.location)
        print(json.dumps(report, indent=2))
    else:
        run_offline_benchmark_showcase()


if __name__ == "__main__":
    main()
