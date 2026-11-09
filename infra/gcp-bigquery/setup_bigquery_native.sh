#!/usr/bin/env bash
# ==============================================================================
# Google Cloud BigQuery Native Setup (POC)
# Enables required Google Cloud APIs and seeds `commerce_core.orders` in BigQuery
# ==============================================================================
set -euo pipefail

export PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || echo 'omni-retail-lakehouse-prod')}"
export GCP_REGION="${GCP_REGION:-us-east4}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

echo "==> [1/2] Enabling Google Cloud BigQuery, BigLake, Secret Manager & Dataproc APIs on '${PROJECT_ID}'..."
gcloud services enable \
  bigquery.googleapis.com \
  biglake.googleapis.com \
  secretmanager.googleapis.com \
  dataproc.googleapis.com \
  --project="${PROJECT_ID}"

echo "==> [2/2] Creating native BigQuery dataset 'commerce_core' (${GCP_REGION}) and seeding 'orders'..."
sed -e "s/omni-retail-lakehouse-prod/${PROJECT_ID}/g" \
    -e "s/us-east4/${GCP_REGION}/g" \
    "${REPO_ROOT}/sql/01_seed_bigquery_orders.sql" \
  | bq query --project_id="${PROJECT_ID}" --location="${GCP_REGION}" --use_legacy_sql=false

echo "SUCCESS: Native BigQuery table '${PROJECT_ID}.commerce_core.orders' is populated in ${GCP_REGION}!"
