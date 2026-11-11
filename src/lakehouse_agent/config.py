"""Settings and dynamic environment configuration for the Borderless Lakehouse ADK Agent."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class LakehouseSettings:
    """Immutable runtime settings for Lakehouse cross-cloud federation and ADK agents."""

    project_id: str = "omni-retail-lakehouse-prod"
    gcp_region: str = "us-east4"
    vertex_location: str = "global"
    gemini_model: str = "gemini-3.8-flash"
    unity_catalog_id: str = "dbx_unity_merch"
    glue_catalog_id: str = "aws_glue_logistics"
    native_bq_dataset: str = "commerce_core"
    max_bytes_billed: int = 10 * 1024**3  # 10 GiB default cap
    aws_public_egress_usd_per_gb: float = 0.09
    aws_cci_egress_usd_per_gb: float = 0.02
    oauth_client_id: str | None = None
    oauth_client_secret: str | None = None


def load_settings(load_env_file: bool = True) -> LakehouseSettings:
    """Load settings dynamically from environment variables (and `.env` if present)."""
    if load_env_file:
        load_dotenv(override=False)

    project_id = (
        os.environ.get("GOOGLE_CLOUD_PROJECT")
        or os.environ.get("PROJECT_ID")
        or "omni-retail-lakehouse-prod"
    )
    # Keep BigQuery / Lakehouse cache region (`us-east4`) distinct from Vertex AI Gemini `global`
    gcp_region = (
        os.environ.get("BQ_LOCATION")
        or os.environ.get("GCP_REGION")
        or "us-east4"
    )
    vertex_location = os.environ.get("GOOGLE_CLOUD_LOCATION", "global")

    return LakehouseSettings(
        project_id=project_id,
        gcp_region=gcp_region,
        vertex_location=vertex_location,
        gemini_model=os.environ.get("GEMINI_MODEL", "gemini-3.8-flash"),
        unity_catalog_id=os.environ.get("UNITY_FED_CATALOG_ID", "dbx_unity_merch"),
        glue_catalog_id=os.environ.get("GLUE_CATALOG_ID", "aws_glue_logistics"),
        native_bq_dataset=os.environ.get("NATIVE_BQ_DATASET", "commerce_core"),
        max_bytes_billed=int(
            os.environ.get("MAX_BYTES_BILLED_PER_QUERY", str(10 * 1024**3))
        ),
        aws_public_egress_usd_per_gb=float(
            os.environ.get("AWS_PUBLIC_EGRESS_USD_PER_GB", "0.09")
        ),
        aws_cci_egress_usd_per_gb=float(
            os.environ.get("AWS_CCI_EGRESS_USD_PER_GB", "0.02")
        ),
        oauth_client_id=os.environ.get("OAUTH_CLIENT_ID") or None,
        oauth_client_secret=os.environ.get("OAUTH_CLIENT_SECRET") or None,
    )
