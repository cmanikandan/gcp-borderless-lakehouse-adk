#!/usr/bin/env python3
"""Zero-Mutation Cloud Readiness & Guardrail Validator (`preflight_doctor.py`).

Checks local developer CLIs, environment alignment (including BigQuery `us-east4` vs.
AWS `us-east-1` co-location and Vertex AI `global` endpoint separation), and—when
`--check-cloud` is passed—live Google Cloud org policy guardrails such as
`constraints/gcp.restrictNonCmekServices` (which silently disables Intelligent Caching).

Usage:
  uv run python scripts/preflight_doctor.py
  uv run python scripts/preflight_doctor.py --check-cloud
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass

from lakehouse_agent.config import load_settings

LOW_LATENCY_PAIRINGS: dict[str, set[str]] = {
    "us-east-1": {"us-east4", "us-east1", "us-east5", "us-central1"},
    "us-east-2": {"us-east5", "us-central1"},
    "us-west-2": {"us-west1"},
    "eu-central-1": {"europe-west3"},
    "eu-west-2": {"europe-west2"},
}


@dataclass(frozen=True)
class CheckResult:
    name: str
    status: str  # "PASS", "WARN", "FAIL"
    detail: str


def check_local_environment() -> list[CheckResult]:
    results: list[CheckResult] = []
    cfg = load_settings()

    for cli in ("gcloud", "bq", "aws", "uv", "python3"):
        path = shutil.which(cli)
        if path:
            results.append(CheckResult(f"CLI '{cli}'", "PASS", f"Found at {path}"))
        else:
            status = "WARN" if cli in ("gcloud", "bq", "aws") else "FAIL"
            results.append(
                CheckResult(
                    f"CLI '{cli}'",
                    status,
                    f"'{cli}' not found on PATH (needed for live cloud provisioning).",
                )
            )

    aws_region = os.environ.get("AWS_REGION", "us-east-1")
    recommended_gcp = LOW_LATENCY_PAIRINGS.get(aws_region, set())
    if recommended_gcp and cfg.gcp_region not in recommended_gcp:
        results.append(
            CheckResult(
                "Cross-Cloud Region Co-location",
                "WARN",
                f"BQ_LOCATION='{cfg.gcp_region}' with AWS_REGION='{aws_region}'. "
                f"Recommended low-latency Google Cloud pairings: {sorted(recommended_gcp)}",
            )
        )
    else:
        results.append(
            CheckResult(
                "Cross-Cloud Region Co-location",
                "PASS",
                f"Google Cloud '{cfg.gcp_region}' paired cleanly with AWS '{aws_region}'.",
            )
        )

    if cfg.vertex_location == cfg.gcp_region and os.environ.get("GOOGLE_GENAI_USE_VERTEXAI", "").upper() == "TRUE":
        results.append(
            CheckResult(
                "Vertex AI vs. BigQuery Endpoint Separation",
                "WARN",
                f"GOOGLE_CLOUD_LOCATION='{cfg.vertex_location}' matches BQ_LOCATION. "
                "Set GOOGLE_CLOUD_LOCATION=global for Vertex AI Gemini 3.8 Flash while keeping BQ_LOCATION=us-east4.",
            )
        )
    else:
        results.append(
            CheckResult(
                "Vertex AI vs. BigQuery Endpoint Separation",
                "PASS",
                f"Vertex AI='{cfg.vertex_location}' | BigQuery/Cache='{cfg.gcp_region}' | Model='{cfg.gemini_model}'.",
            )
        )

    return results


def check_live_cloud_guardrails(project_id: str) -> list[CheckResult]:
    results: list[CheckResult] = []
    try:
        proc = subprocess.run(
            [
                "gcloud",
                "resource-manager",
                "org-policies",
                "describe",
                "gcp.restrictNonCmekServices",
                f"--project={project_id}",
                "--effective",
                "--format=json",
            ],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        if proc.returncode == 0 and "biglake.googleapis.com" in proc.stdout:
            results.append(
                CheckResult(
                    "Org Policy Non-CMEK Check",
                    "WARN",
                    "`constraints/gcp.restrictNonCmekServices` restricts non-CMEK services! "
                    "Cross-Cloud Intelligent Caching uses Google-managed keys and will auto-disable.",
                )
            )
        else:
            results.append(
                CheckResult(
                    "Org Policy Non-CMEK Check",
                    "PASS",
                    "No `restrictNonCmekServices` block detected for Intelligent Caching.",
                )
            )
    except Exception as exc:  # pragma: no cover - defensive CLI check
        results.append(
            CheckResult(
                "Org Policy Non-CMEK Check",
                "WARN",
                f"Skipped live org-policy inspection ({exc}).",
            )
        )
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Borderless Lakehouse POC Preflight Doctor")
    parser.add_argument(
        "--check-cloud",
        action="store_true",
        help="Also query live Google Cloud org policies via gcloud",
    )
    args = parser.parse_args()

    cfg = load_settings()
    checks = check_local_environment()
    if args.check_cloud:
        checks.extend(check_live_cloud_guardrails(cfg.project_id))

    print("=" * 84)
    print(" Google Cloud Borderless Lakehouse POC — Preflight Doctor")
    print("=" * 84)
    has_fail = False
    for item in checks:
        badge = {"PASS": "[PASS]", "WARN": "[WARN]", "FAIL": "[FAIL]"}[item.status]
        print(f" {badge:<7} {item.name:<42} {item.detail}")
        if item.status == "FAIL":
            has_fail = True
    print("=" * 84)
    return 1 if has_fail else 0


if __name__ == "__main__":
    sys.exit(main())
