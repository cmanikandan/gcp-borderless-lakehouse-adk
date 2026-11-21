"""Settings and dynamic environment configuration for the Borderless Lakehouse ADK Agent."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass

from dotenv import load_dotenv

MODEL_PROFILES: dict[str, tuple[str, str, str]] = {
    # (coordinator_model, analyst_model, finops_model)
    # Default Nov 2026 Gemini 4 triad: Neon (fast frontier synthesis) + Carbon (deep SQL/AI) + Hydrogen (Flash-tier telemetry)
    "tiered": ("gemini-4-neon", "gemini-4-carbon", "gemini-4-hydrogen"),
    "argon-tiered": ("gemini-4-argon", "gemini-4-carbon", "gemini-3.8-flash"),
    "neon": ("gemini-4-neon", "gemini-4-neon", "gemini-4-neon"),
    "carbon": ("gemini-4-carbon", "gemini-4-carbon", "gemini-4-carbon"),
    "argon": ("gemini-4-argon", "gemini-4-argon", "gemini-4-argon"),
    "hydrogen": ("gemini-4-hydrogen", "gemini-4-hydrogen", "gemini-4-hydrogen"),
    "flash": ("gemini-3.8-flash", "gemini-3.8-flash", "gemini-3.8-flash"),
}

SUPPORTED_GEMINI_MODELS: frozenset[str] = frozenset(
    {
        "gemini-4-neon",
        "gemini-4-carbon",
        "gemini-4-hydrogen",
        "gemini-4-argon",
        "gemini-3.8-flash",
        "gemini-3.8-live-extended-thinking",
    }
)


def sanitize_bq_label(value: str) -> str:
    """Convert a model or profile string into a valid BigQuery job label value."""
    cleaned = re.sub(r"[^a-z0-9_-]", "-", value.strip().lower())
    return cleaned[:63].strip("-") or "unknown"


@dataclass(frozen=True)
class LakehouseSettings:
    """Immutable runtime settings for Google Cloud Lakehouse federation and ADK 2.15+ agents.

    Supports heterogeneous per-role Gemini 4 model routing (`model_profile="tiered"` uses
    `gemini-4-neon` for fast frontier orchestration, `gemini-4-carbon` for deep cross-cloud
    SQL & BigQuery AI synthesis, and `gemini-4-hydrogen` for high-throughput FinOps telemetry)
    as well as uniform single-model overrides via `gemini_model` or `MODEL_PROFILE`.
    """

    project_id: str = "omni-retail-lakehouse-prod"
    gcp_region: str = "us-east4"
    vertex_location: str = "global"
    model_profile: str = "tiered"
    gemini_model: str | None = None
    coordinator_model: str | None = None
    analyst_model: str | None = None
    finops_model: str | None = None
    max_output_tokens: int = 8192  # Caps Gemini 4's 1M-token decode window per turn
    bq_write_mode: str = "protected"  # "protected" (session temp tables only) or "blocked"
    enable_bq_agent_analytics: bool = False  # Streams ADK events to BigQuery when True
    bq_agent_analytics_dataset: str = "commerce_core"
    unity_catalog_id: str = "dbx_unity_merch"
    glue_catalog_id: str = "aws_glue_logistics"
    native_bq_dataset: str = "commerce_core"
    max_bytes_billed: int = 10 * 1024**3  # 10 GiB default cap
    aws_public_egress_usd_per_gb: float = 0.09
    aws_cci_egress_usd_per_gb: float = 0.02
    oauth_client_id: str | None = None
    oauth_client_secret: str | None = None

    def _profile_models(self) -> tuple[str, str, str]:
        return MODEL_PROFILES.get(self.model_profile.lower(), MODEL_PROFILES["tiered"])

    @property
    def resolved_coordinator_model(self) -> str:
        if self.coordinator_model:
            return self.coordinator_model
        if self.gemini_model:
            return self.gemini_model
        return self._profile_models()[0]

    @property
    def resolved_analyst_model(self) -> str:
        if self.analyst_model:
            return self.analyst_model
        if self.gemini_model:
            return self.gemini_model
        return self._profile_models()[1]

    @property
    def resolved_finops_model(self) -> str:
        if self.finops_model:
            return self.finops_model
        if self.gemini_model:
            return self.gemini_model
        return self._profile_models()[2]


def load_settings(
    load_env_file: bool = True,
    model_profile_override: str | None = None,
) -> LakehouseSettings:
    """Load settings dynamically from environment variables (and `.env` if present)."""
    if load_env_file:
        load_dotenv(override=False)

    project_id = (
        os.environ.get("GOOGLE_CLOUD_PROJECT")
        or os.environ.get("PROJECT_ID")
        or "omni-retail-lakehouse-prod"
    )
    # Keep BigQuery / Lakehouse cache region (`us-east4`) distinct from Vertex AI / Agent Platform `global`
    gcp_region = (
        os.environ.get("BQ_LOCATION")
        or os.environ.get("GCP_REGION")
        or "us-east4"
    )
    vertex_location = os.environ.get("GOOGLE_CLOUD_LOCATION", "global")
    profile = (
        model_profile_override
        or os.environ.get("MODEL_PROFILE")
        or "tiered"
    ).strip().lower()
    write_mode = os.environ.get("BQ_WRITE_MODE", "protected").strip().lower()
    enable_analytics = os.environ.get("ENABLE_BQ_AGENT_ANALYTICS", "false").strip().lower() in {
        "1",
        "true",
        "yes",
    }

    return LakehouseSettings(
        project_id=project_id,
        gcp_region=gcp_region,
        vertex_location=vertex_location,
        model_profile=profile,
        gemini_model=os.environ.get("GEMINI_MODEL") or None,
        coordinator_model=os.environ.get("COORDINATOR_MODEL") or None,
        analyst_model=os.environ.get("ANALYST_MODEL") or None,
        finops_model=os.environ.get("FINOPS_MODEL") or None,
        max_output_tokens=int(os.environ.get("MAX_OUTPUT_TOKENS", "8192")),
        bq_write_mode=write_mode,
        enable_bq_agent_analytics=enable_analytics,
        bq_agent_analytics_dataset=os.environ.get("BQ_AGENT_ANALYTICS_DATASET", "commerce_core"),
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
