-- =============================================================================
-- Fleet-Wide Cross-Cloud Caching & AI Agent FinOps Audit (BigQuery)
--
-- Queries `INFORMATION_SCHEMA.JOBS_BY_PROJECT` using the GA `object_storage_stats`
-- repeated record (`ARRAY<STRUCT<cloud_provider, object_storage_bytes_read, cache_bytes_read>>`)
-- paired with our Google ADK `workload` and `llm_model` job labels.
--
-- Production Tip (Nov 2026 GA): Pair Cross-Cloud Caching (which bounds cloud
-- storage egress) with BigQuery Project Caps (`max_slots` and concurrency caps
-- per project inside Enterprise / Enterprise Plus reservations) so exploratory
-- AI agents never starve executive BI dashboards.
-- =============================================================================
SELECT
  DATE(j.creation_time) AS query_date,
  oss.cloud_provider,
  COALESCE(
    (SELECT l.value FROM UNNEST(j.labels) AS l WHERE l.key = 'workload'),
    'ad-hoc-or-bi'
  ) AS workload_type,
  COALESCE(
    (SELECT l.value FROM UNNEST(j.labels) AS l WHERE l.key = 'llm_model'),
    'human-sql'
  ) AS agent_model,
  COUNT(DISTINCT j.job_id) AS cross_cloud_query_jobs,
  ROUND(SUM(oss.cache_bytes_read) / 1e9, 3) AS local_cache_gb_read,
  ROUND(SUM(oss.object_storage_bytes_read) / 1e9, 3) AS remote_storage_gb_read,
  ROUND(
    SAFE_DIVIDE(
      SUM(oss.cache_bytes_read),
      SUM(oss.cache_bytes_read + oss.object_storage_bytes_read)
    ) * 100,
    2
  ) AS cache_hit_ratio_pct,
  ROUND((SUM(oss.cache_bytes_read) / 1e9) * 0.09, 2) AS avoided_public_egress_usd,
  ROUND((SUM(oss.cache_bytes_read) / 1e9) * 0.02, 2) AS avoided_cci_egress_usd
FROM
  `region-us-east4`.INFORMATION_SCHEMA.JOBS_BY_PROJECT AS j,
  UNNEST(j.object_storage_stats) AS oss
WHERE
  j.creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND j.job_type = 'QUERY'
  AND j.statement_type != 'SCRIPT'
  AND j.error_result IS NULL
GROUP BY
  1, 2, 3, 4
ORDER BY
  query_date DESC,
  avoided_public_egress_usd DESC;
