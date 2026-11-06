"""Settings and environment configuration for the Borderless Lakehouse ADK Agent."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class LakehouseSettings:
    """Immutable runtime settings for Lakehouse cross-cloud federation and ADK agents."""

    project_id: str = os.environ.get("GOOGLE_CLOUD_PROJECT", "omni-retail-lakehouse-prod")
    gcp_region: str = os.environ.get("GOOGLE_CLOUD_LOCATION", "us-east4")
    gemini_model: str = os.environ.get("GEMINI_MODEL", "gemini-3.8-flash")
    unity_catalog_id: str = os.environ.get("UNITY_FED_CATALOG_ID", "dbx_unity_merch")
    glue_catalog_id: str = os.environ.get("GLUE_CATALOG_ID", "aws_glue_logistics")
    native_bq_dataset: str = os.environ.get("NATIVE_BQ_DATASET", "commerce_core")
    max_bytes_billed: int = int(
        os.environ.get("MAX_BYTES_BILLED_PER_QUERY", str(10 * 1024**3))  # 10 GiB default cap
    )
    aws_public_egress_usd_per_gb: float = float(
        os.environ.get("AWS_PUBLIC_EGRESS_USD_PER_GB", "0.09")
    )
    aws_cci_egress_usd_per_gb: float = float(
        os.environ.get("AWS_CCI_EGRESS_USD_PER_GB", "0.02")
    )
    oauth_client_id: str | None = os.environ.get("OAUTH_CLIENT_ID") or None
    oauth_client_secret: str | None = os.environ.get("OAUTH_CLIENT_SECRET") or None


def load_settings() -> LakehouseSettings:
    """Load settings from environment variables."""
    return LakehouseSettings()
