#!/usr/bin/env bash
# ==============================================================================
# Alternative GA Surface: BigQuery Cross-Cloud Connection with Intelligent Cache
#
# Per Google Cloud Lakehouse Release Notes, you can opt in to Cross-Cloud
# Intelligent Caching on EITHER:
#   1. A Lakehouse Federated Catalog (`gcloud biglake iceberg catalogs ... --cross-cloud-cache="enabled"`)
#   2. A BigQuery Cross-Cloud Connection (`bq mk --connection` with `crossCloudCacheOptions`)
#      created in a standard Google Cloud BigQuery region (e.g., `us-east4`) — NO
#      BigQuery Omni `aws-*`/`azure-*` reservation required, and $0 extra cache fee!
# ==============================================================================
set -euo pipefail

export PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || echo 'omni-retail-lakehouse-prod')}"
export GCP_REGION="${GCP_REGION:-us-east4}"
if [[ -z "${AWS_ACCOUNT_ID:-}" ]]; then
  AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo '123456789012')"
  export AWS_ACCOUNT_ID
fi
export AWS_ROLE_NAME="${AWS_ROLE_NAME:-lakehouse_glue_federation_role}"
export BQ_CC_CONN_ID="${BQ_CC_CONN_ID:-aws_s3_cached_conn}"

AWS_ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${AWS_ROLE_NAME}"
CONN_PROPS="{\"accessRole\":{\"iamRoleId\":\"${AWS_ROLE_ARN}\"},\"crossCloudCacheOptions\":{\"enabled\":true}}"

echo "==> [1/2] Creating/Updating BigQuery Cross-Cloud Connection (${BQ_CC_CONN_ID}) in ${GCP_REGION} with Intelligent Caching..."
if ! bq show --connection --project_id="${PROJECT_ID}" --location="${GCP_REGION}" "${BQ_CC_CONN_ID}" >/dev/null 2>&1; then
  bq mk --connection \
    --connection_type='AWS' \
    --location="${GCP_REGION}" \
    --project_id="${PROJECT_ID}" \
    --properties="${CONN_PROPS}" \
    "${BQ_CC_CONN_ID}"
else
  bq update --connection \
    --connection_type='AWS' \
    --location="${GCP_REGION}" \
    --project_id="${PROJECT_ID}" \
    --properties="${CONN_PROPS}" \
    "${BQ_CC_CONN_ID}"
fi

echo "==> [2/2] Inspecting provisioned AWS Identity & crossCloudCacheOptions..."
bq show --format=prettyjson --connection \
  --project_id="${PROJECT_ID}" \
  --location="${GCP_REGION}" \
  "${BQ_CC_CONN_ID}"
