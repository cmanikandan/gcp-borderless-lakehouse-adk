#!/usr/bin/env bash
# ==============================================================================
# Execute Cold vs. Warm Cross-Platform Join in BigQuery (`--nouse_cache`)
# Substitutes your PROJECT_ID dynamically and prints job IDs for cache auditing.
# ==============================================================================
set -euo pipefail

export PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || echo 'omni-retail-lakehouse-prod')}"
export GCP_REGION="${GCP_REGION:-us-east4}"
export UNITY_FED_CATALOG_ID="${UNITY_FED_CATALOG_ID:-dbx_unity_merch}"
export GLUE_CATALOG_ID="${GLUE_CATALOG_ID:-aws_glue_logistics}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

RENDERED_SQL="$(sed \
  -e "s/omni-retail-lakehouse-prod/${PROJECT_ID}/g" \
  -e "s/dbx_unity_merch/${UNITY_FED_CATALOG_ID}/g" \
  -e "s/aws_glue_logistics/${GLUE_CATALOG_ID}/g" \
  "${REPO_ROOT}/sql/02_cross_cloud_supply_chain_join.sql")"

echo "==> [Run #1 — Cold Scan] Populating Google Cloud regional block cache in ${GCP_REGION}..."
echo "${RENDERED_SQL}" | bq query \
  --project_id="${PROJECT_ID}" \
  --location="${GCP_REGION}" \
  --use_legacy_sql=false \
  --nouse_cache

echo ""
echo "==> [Run #2 — Warm Scan] Reading cached Parquet blocks locally (--nouse_cache bypasses SQL result cache)..."
echo "${RENDERED_SQL}" | bq query \
  --project_id="${PROJECT_ID}" \
  --location="${GCP_REGION}" \
  --use_legacy_sql=false \
  --nouse_cache

echo ""
echo "Tip: Inspect the latest job's objectStorageStats cache hit ratio with:"
echo "  LAST_JOB=\$(bq ls -j -n 1 --project_id=${PROJECT_ID} --location=${GCP_REGION} --format=json | jq -r '.[0].jobReference.jobId')"
echo "  uv run python scripts/demo_agent_cache_finops.py --live-job-id \"\${LAST_JOB}\" --location ${GCP_REGION}"
