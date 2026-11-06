"""Cross-Cloud Intelligent Caching Telemetry & FinOps Egress Analyzer.

Parses BigQuery job execution metadata (`statistics.query.objectStorageStats` /
`object_storage_stats`) introduced with Google Cloud Lakehouse Cross-Cloud Caching (GA)
to quantify local cache hit ratios and avoided AWS/Azure egress fees.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from google.cloud import bigquery

from lakehouse_agent.config import load_settings

BYTES_PER_GB = 1_000_000_000  # Decimal GB used by cloud network egress billing SKUs


@dataclass(frozen=True)
class CloudProviderCacheStat:
    """Per-provider cross-cloud object storage & local cache read telemetry."""

    cloud_provider: str
    cache_bytes_read: int
    object_storage_bytes_read: int

    @property
    def total_cross_cloud_bytes(self) -> int:
        return self.cache_bytes_read + self.object_storage_bytes_read

    @property
    def cache_hit_ratio_pct(self) -> float:
        total = self.total_cross_cloud_bytes
        if total == 0:
            return 0.0
        return round((self.cache_bytes_read / total) * 100.0, 2)


@dataclass(frozen=True)
class JobCacheFinOpsReport:
    """Structured FinOps summary for a cross-cloud BigQuery / Lakehouse query job."""

    job_id: str
    location: str
    providers: list[CloudProviderCacheStat]
    total_cache_bytes_read: int
    total_remote_bytes_read: int
    overall_cache_hit_ratio_pct: float
    avoided_public_egress_usd: float
    avoided_cci_egress_usd: float
    architectural_recommendation: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["providers"] = [
            {
                **asdict(p),
                "total_cross_cloud_bytes": p.total_cross_cloud_bytes,
                "cache_hit_ratio_pct": p.cache_hit_ratio_pct,
            }
            for p in self.providers
        ]
        return payload


def parse_object_storage_stats(raw_job_properties: dict[str, Any]) -> list[CloudProviderCacheStat]:
    """Extract per-cloud cache telemetry from BigQuery REST or Proto job properties.

    Supports both camelCase REST JSON (`statistics.query.objectStorageStats`)
    and snake_case Proto dicts (`statistics.query.object_storage_stats`).
    """
    stats_root = raw_job_properties.get("statistics", {})
    query_stats = stats_root.get("query", {})
    entries = (
        query_stats.get("objectStorageStats")
        or query_stats.get("object_storage_stats")
        or []
    )

    parsed: list[CloudProviderCacheStat] = []
    for item in entries:
        provider = str(
            item.get("cloudProvider") or item.get("cloud_provider") or "UNKNOWN"
        ).upper()
        cache_bytes = int(item.get("cacheBytesRead") or item.get("cache_bytes_read") or 0)
        remote_bytes = int(
            item.get("objectStorageBytesRead")
            or item.get("object_storage_bytes_read")
            or 0
        )
        parsed.append(
            CloudProviderCacheStat(
                cloud_provider=provider,
                cache_bytes_read=cache_bytes,
                object_storage_bytes_read=remote_bytes,
            )
        )
    return parsed


def evaluate_job_finops(
    job_id: str,
    location: str,
    raw_job_properties: dict[str, Any],
    public_egress_rate_per_gb: float = 0.09,
    cci_egress_rate_per_gb: float = 0.02,
) -> JobCacheFinOpsReport:
    """Compute cache hit ratio, avoided egress cost, and architectural guidance."""
    providers = parse_object_storage_stats(raw_job_properties)
    total_cache = sum(p.cache_bytes_read for p in providers)
    total_remote = sum(p.object_storage_bytes_read for p in providers)
    grand_total = total_cache + total_remote

    hit_ratio = round((total_cache / grand_total) * 100.0, 2) if grand_total > 0 else 0.0
    avoided_gb = total_cache / BYTES_PER_GB
    saved_public = round(avoided_gb * public_egress_rate_per_gb, 4)
    saved_cci = round(avoided_gb * cci_egress_rate_per_gb, 4)

    if grand_total == 0:
        recommendation = (
            "Query read only native Google Cloud tables (0 cross-cloud object bytes scanned)."
        )
    elif total_cache == 0 and total_remote > 0:
        recommendation = (
            "0% cache hit detected (cold read or cache disabled). If repeat queries also show "
            "cacheBytesRead=0, verify `--cross-cloud-cache=enabled` on the federated catalog AND "
            "confirm org policy `constraints/gcp.restrictNonCmekServices` is not disabling cache."
        )
    elif hit_ratio < 70.0:
        recommendation = (
            f"Partial cache warm-up ({hit_ratio}% local read). Subsequent agentic or BI queries "
            "over these Iceberg snapshots will serve from the regional cache at $0 remote egress."
        )
    else:
        recommendation = (
            f"Optimal cache efficiency ({hit_ratio}% served from Google Cloud regional cache). "
            "Cross-cloud egress minimized."
        )

    return JobCacheFinOpsReport(
        job_id=job_id,
        location=location,
        providers=providers,
        total_cache_bytes_read=total_cache,
        total_remote_bytes_read=total_remote,
        overall_cache_hit_ratio_pct=hit_ratio,
        avoided_public_egress_usd=saved_public,
        avoided_cci_egress_usd=saved_cci,
        architectural_recommendation=recommendation,
    )


def inspect_bigquery_job_cache_stats(job_id: str, location: str = "us-east4") -> dict[str, Any]:
    """ADK Tool: Fetch a BigQuery job by ID and analyze cross-cloud cache & egress savings.

    Args:
        job_id: The BigQuery Job ID to inspect (e.g. 'bquxjob_1234abcd_192f0').
        location: The Google Cloud region where the Lakehouse query ran (default 'us-east4').

    Returns:
        Dictionary containing per-cloud cacheBytesRead, objectStorageBytesRead,
        cache hit percentage, avoided AWS/Azure egress fees, and tuning guidance.
    """
    settings = load_settings()
    client = bigquery.Client(project=settings.project_id, location=location)
    job = client.get_job(job_id=job_id, location=location)
    # `_properties` contains the full BigQuery REST job representation including objectStorageStats
    raw_props: dict[str, Any] = getattr(job, "_properties", {}) or {}
    report = evaluate_job_finops(
        job_id=job_id,
        location=location,
        raw_job_properties=raw_props,
        public_egress_rate_per_gb=settings.aws_public_egress_usd_per_gb,
        cci_egress_rate_per_gb=settings.aws_cci_egress_usd_per_gb,
    )
    return report.to_dict()


def simulate_agentic_burst_finops(
    dataset_scanned_gb_per_query: float = 45.0,
    queries_per_agent_investigation: int = 6,
    daily_investigations: int = 40,
) -> dict[str, Any]:
    """ADK Tool: Quantify monthly cross-cloud egress savings for chatty AI agent workloads.

    Models why Intelligent Cross-Cloud Caching is critical for AI agents that issue
    multiple iterative follow-up SQL queries against federated S3 / Unity Catalog tables.

    Args:
        dataset_scanned_gb_per_query: Average remote Iceberg Parquet GB scanned per query.
        queries_per_agent_investigation: Number of iterative SQL tool calls per user prompt.
        daily_investigations: Number of agent investigations per day across the team.

    Returns:
        Monthly egress comparison (Uncached Public Internet vs. Intelligent Regional Cache).
    """
    settings = load_settings()
    monthly_queries = queries_per_agent_investigation * daily_investigations * 30
    uncached_total_gb = monthly_queries * dataset_scanned_gb_per_query
    uncached_public_cost_usd = round(
        uncached_total_gb * settings.aws_public_egress_usd_per_gb, 2
    )

    # With Lakehouse Intelligent Caching, the initial cold scan populates the regional block
    # cache; subsequent agent queries within the snapshot window read locally (assume ~92% hit rate).
    cached_remote_gb = round(uncached_total_gb * 0.08, 2)
    cached_local_gb = round(uncached_total_gb * 0.92, 2)
    cached_public_cost_usd = round(
        cached_remote_gb * settings.aws_public_egress_usd_per_gb, 2
    )
    cached_cci_cost_usd = round(
        cached_remote_gb * settings.aws_cci_egress_usd_per_gb, 2
    )

    return {
        "monthly_agent_sql_queries": monthly_queries,
        "uncached_remote_s3_egress_gb": round(uncached_total_gb, 2),
        "uncached_monthly_aws_egress_usd": uncached_public_cost_usd,
        "intelligent_cache_local_read_gb": cached_local_gb,
        "intelligent_cache_remote_read_gb": cached_remote_gb,
        "cached_public_internet_egress_usd": cached_public_cost_usd,
        "cached_plus_cci_egress_usd": cached_cci_cost_usd,
        "monthly_savings_with_cache_usd": round(
            uncached_public_cost_usd - cached_public_cost_usd, 2
        ),
        "monthly_savings_pct": 92.0,
    }


def get_federated_catalog_cli_blueprint(
    provider: str,
    catalog_name: str,
    gcp_region: str = "us-east4",
    enable_cache: bool = True,
    enable_user_identity: bool = True,
) -> dict[str, str]:
    """ADK Tool: Generate verified GA `gcloud biglake iceberg catalogs` CLI commands.

    Args:
        provider: Either 'unity' (Databricks Unity Catalog) or 'glue' (AWS Glue Data Catalog).
        catalog_name: Desired Lakehouse catalog name in Google Cloud.
        gcp_region: Target Google Cloud region closest to remote storage (e.g. 'us-east4').
        enable_cache: Whether to opt in to `--cross-cloud-cache=enabled`.
        enable_user_identity: Propagate end-user OIDC identities (supported for 'unity').

    Returns:
        Dictionary with exact GA CLI command and architectural notes.
    """
    settings = load_settings()
    cache_flag = "enabled" if enable_cache else "disabled"
    norm = provider.strip().lower()

    if norm in ("unity", "databricks"):
        identity_mode = (
            "user-identity-federation" if enable_user_identity else "service-identity"
        )
        cmd = (
            f"gcloud biglake iceberg catalogs create {catalog_name} \\\n"
            f'  --project="{settings.project_id}" \\\n'
            f'  --catalog-type="federated" \\\n'
            f'  --federated-catalog-type="unity" \\\n'
            f'  --unity-instance-name="<WORKSPACE_HOST>.cloud.databricks.com" \\\n'
            f'  --unity-catalog-name="<REMOTE_UC_CATALOG>" \\\n'
            f'  --unity-service-principal-application-id="<SP_APP_UUID>" \\\n'
            f'  --federated-catalog-identity-mode="{identity_mode}" \\\n'
            f'  --cross-cloud-cache="{cache_flag}" \\\n'
            f'  --primary-location="{gcp_region}" \\\n'
            f'  --refresh-interval="300s"'
        )
        note = (
            "Ensure External Data Access is enabled on the Unity Catalog metastore, grant "
            "`EXTERNAL USE SCHEMA` to the service principal, and configure the Databricks "
            "account-wide OIDC federation policy for user-identity-federation."
        )
    elif norm in ("glue", "aws-glue", "aws_glue"):
        cmd = (
            f"gcloud biglake iceberg catalogs create {catalog_name} \\\n"
            f'  --project="{settings.project_id}" \\\n'
            f'  --catalog-type="federated" \\\n'
            f'  --federated-catalog-type="glue" \\\n'
            f'  --glue-warehouse="<12_DIGIT_AWS_ACCOUNT_ID>" \\\n'
            f'  --glue-aws-region="us-east-1" \\\n'
            f'  --glue-aws-role-arn="arn:aws:iam::<12_DIGIT_AWS_ACCOUNT_ID>:role/lakehouse_glue_federation_role" \\\n'
            f'  --cross-cloud-cache="{cache_flag}" \\\n'
            f'  --primary-location="{gcp_region}"'
        )
        note = (
            "User identity federation is currently supported only for Databricks Unity Catalog; "
            "AWS Glue uses OIDC service-account federation (`biglake-service-account-id`). "
            "After updating the AWS IAM role trust policy, run `gcloud biglake iceberg catalogs update` "
            "with `--refresh-interval=300s`."
        )
    else:
        raise ValueError("Unsupported provider. Choose 'unity' or 'glue'.")

    return {"provider": norm, "gcloud_command": cmd, "architect_note": note}
