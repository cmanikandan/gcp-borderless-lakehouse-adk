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
    """Compute cache hit ratio, avoided egress cost, and tuning guidance."""
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


def _resolve_latest_query_job(
    client: bigquery.Client,
    project_id: str,
    location: str,
) -> Any:
    """Find the most recent completed BigQuery QUERY job (preferring ADK-labeled jobs)."""
    candidates: list[Any] = []
    for j in client.list_jobs(project=project_id, state_filter="DONE", max_results=20):
        if getattr(j, "job_type", None) != "query":
            continue
        if getattr(j, "location", location) != location:
            continue
        labels = getattr(j, "labels", {}) or {}
        if labels.get("workload") == "borderless-lakehouse-adk":
            return client.get_job(job_id=j.job_id, location=location)
        candidates.append(j)

    if candidates:
        return client.get_job(job_id=candidates[0].job_id, location=location)
    raise ValueError(f"No completed BigQuery query jobs found in {project_id} ({location}).")


def inspect_bigquery_job_cache_stats(
    job_id: str = "LATEST",
    location: str = "us-east4",
) -> dict[str, Any]:
    """ADK Tool: Inspect a BigQuery job (or 'LATEST' ADK query) for cross-cloud cache savings.

    Note: Because `BigQueryToolset.execute_sql` returns query rows without the `job_id`,
    passing `job_id='LATEST'` automatically resolves the most recent completed ADK query job!

    Args:
        job_id: Specific BigQuery Job ID, or 'LATEST' to auto-select the most recent query job.
        location: Google Cloud region where the Lakehouse query ran (default 'us-east4').

    Returns:
        Dictionary containing per-cloud cacheBytesRead, objectStorageBytesRead,
        cache hit percentage, avoided AWS/Azure egress fees, and tuning guidance.
    """
    settings = load_settings()
    target_loc = location or settings.gcp_region
    client = bigquery.Client(project=settings.project_id, location=target_loc)

    normalized_id = (job_id or "").strip()
    if not normalized_id or normalized_id.upper() == "LATEST":
        job = _resolve_latest_query_job(client, settings.project_id, target_loc)
        resolved_job_id = str(job.job_id)
    else:
        job = client.get_job(job_id=normalized_id, location=target_loc)
        resolved_job_id = normalized_id

    raw_props: dict[str, Any] = getattr(job, "_properties", {}) or {}
    report = evaluate_job_finops(
        job_id=resolved_job_id,
        location=target_loc,
        raw_job_properties=raw_props,
        public_egress_rate_per_gb=settings.aws_public_egress_usd_per_gb,
        cci_egress_rate_per_gb=settings.aws_cci_egress_usd_per_gb,
    )
    return report.to_dict()


def simulate_agentic_burst_finops(
    dataset_scanned_gb_per_query: float = 48.0,
    queries_per_agent_investigation: int = 6,
    daily_investigations: int = 40,
) -> dict[str, Any]:
    """ADK Tool: Quantify monthly cross-cloud egress savings for chatty AI agent workloads.

    Models why Intelligent Cross-Cloud Caching is critical for AI agents that issue
    multiple iterative follow-up SQL queries against federated S3 / Unity Catalog tables.

    Args:
        dataset_scanned_gb_per_query: Average remote Iceberg Parquet GB scanned per query (default 48 GB).
        queries_per_agent_investigation: Number of iterative SQL tool calls per user prompt (default 6).
        daily_investigations: Number of agent investigations per day across the team (default 40).

    Returns:
        Monthly egress comparison (Uncached Public Internet vs. Intelligent Regional Cache).
    """
    settings = load_settings()
    monthly_queries = queries_per_agent_investigation * daily_investigations * 30
    uncached_total_gb = monthly_queries * dataset_scanned_gb_per_query
    uncached_public_cost_usd = round(
        uncached_total_gb * settings.aws_public_egress_usd_per_gb, 2
    )

    # With Lakehouse Intelligent Caching ($0 extra Google Cloud fee), query #1 warms the regional
    # block cache; subsequent agent queries within the snapshot window read locally (~92% hit rate).
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
    """ADK Tool: Generate verified GA CLI commands for Cross-Cloud Federation & Caching.

    Args:
        provider: 'unity' (Databricks Unity Catalog), 'glue' (AWS Glue), or 'bq-connection' (BigQuery AWS Connection).
        catalog_name: Desired Lakehouse catalog ID or BigQuery connection ID.
        gcp_region: Target Google Cloud region closest to remote storage (e.g. 'us-east4').
        enable_cache: Whether to opt in to Cross-Cloud Intelligent Caching.
        enable_user_identity: Propagate end-user OIDC identities (supported for 'unity').

    Returns:
        Dictionary with exact GA CLI command and practitioner setup notes.
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
            f'  --primary-location="{gcp_region}"'
        )
        note = (
            "Ensure External Data Access is enabled on the Unity Catalog metastore, use a "
            "registered External Location (not Databricks default storage), grant "
            "`EXTERNAL USE SCHEMA` to the service principal, configure OIDC federation "
            "policies, and then run `gcloud biglake iceberg catalogs update` with `--refresh-interval=300s`."
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
            "For Amazon S3 Tables, pass `--glue-warehouse=\"<ACCOUNT_ID>:s3tablescatalog/<TABLE_BUCKET>\"`. "
            "Extract `biglake-service-account-id` from `catalogs describe`, bind `accounts.google.com:aud` "
            "and `accounts.google.com:sub` in your AWS IAM trust policy, then run `catalogs update --refresh-interval=300s`."
        )
    elif norm in ("bq-connection", "bigquery-connection", "aws-connection"):
        cache_bool = "true" if enable_cache else "false"
        cmd = (
            f"bq mk --connection \\\n"
            f"  --connection_type='AWS' \\\n"
            f'  --location="{gcp_region}" \\\n'
            f'  --project_id="{settings.project_id}" \\\n'
            f'  --properties=\'{{"accessRole":{{"iamRoleId":"arn:aws:iam::<12_DIGIT_AWS_ACCOUNT_ID>:role/lakehouse_glue_federation_role"}},"crossCloudCacheOptions":{{"enabled":{cache_bool}}}}}\' \\\n'
            f"  {catalog_name}"
        )
        note = (
            "Alternative GA surface for standalone BigLake S3/ADLS external tables in standard "
            "BigQuery regions (no BigQuery Omni `aws-*`/`azure-*` reservation required; $0 extra cache fee)."
        )
    else:
        raise ValueError("Unsupported provider. Choose 'unity', 'glue', or 'bq-connection'.")

    return {"provider": norm, "gcloud_command": cmd, "practitioner_note": note}
