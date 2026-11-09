#!/usr/bin/env bash
# ==============================================================================
# Google Cloud Borderless Lakehouse -> AWS Glue Data Catalog Federation + Cache
# Cloud Practitioner Proof-of-Concept (POC) Script
# ==============================================================================
set -euo pipefail

# Required environment variables (override or export via .env)
export PROJECT_ID="${PROJECT_ID:-omni-retail-lakehouse-prod}"
export GCP_REGION="${GCP_REGION:-us-east4}"          # Paired with AWS us-east-1 for minimal latency
if [[ -z "${AWS_ACCOUNT_ID:-}" ]]; then
  AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo '123456789012')"
  export AWS_ACCOUNT_ID
fi
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_ROLE_NAME="${AWS_ROLE_NAME:-lakehouse_glue_federation_role}"
export GLUE_DATABASE="${GLUE_DATABASE:-fulfillment}"
export S3_ICEBERG_BUCKET="${S3_ICEBERG_BUCKET:-omniretail-logistics-iceberg-${AWS_ACCOUNT_ID}}"
export GLUE_CATALOG_ID="${GLUE_CATALOG_ID:-aws_glue_logistics}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

echo "==> [1/6] Enabling required Google Cloud APIs..."
gcloud services enable biglake.googleapis.com bigquery.googleapis.com --project="${PROJECT_ID}"

echo "==> [2/6] Creating AWS IAM Role (${AWS_ROLE_NAME}) with placeholder OIDC trust policy..."
# 12-hour max session duration (43200s) prevents token expiry on heavy analytical scans
aws iam create-role \
  --role-name "${AWS_ROLE_NAME}" \
  --assume-role-policy-document "file://${SCRIPT_DIR}/trust_policy_placeholder.json" \
  --max-session-duration 43200 \
  --description "OIDC federated role for Google Cloud Borderless Lakehouse" || true

envsubst < "${SCRIPT_DIR}/permissions_policy.json" > "${WORK_DIR}/permissions_policy_rendered.json"

aws iam put-role-policy \
  --role-name "${AWS_ROLE_NAME}" \
  --policy-name "LakehouseGlueIcebergReadAccess" \
  --policy-document "file://${WORK_DIR}/permissions_policy_rendered.json"

AWS_ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${AWS_ROLE_NAME}"

echo "==> [3/6] Creating Lakehouse Federated Catalog (${GLUE_CATALOG_ID}) with Intelligent Caching..."
# Architectural Tip: Omit --refresh-interval on initial creation so background sync
# does not fail before AWS IAM OIDC trust policy propagation completes.
# Note: --glue-warehouse takes your 12-digit AWS Account ID (or S3 table bucket catalog), NOT an s3:// URI!
gcloud biglake iceberg catalogs create "${GLUE_CATALOG_ID}" \
  --project="${PROJECT_ID}" \
  --catalog-type="federated" \
  --federated-catalog-type="glue" \
  --glue-warehouse="${AWS_ACCOUNT_ID}" \
  --glue-aws-region="${AWS_REGION}" \
  --glue-aws-role-arn="${AWS_ROLE_ARN}" \
  --primary-location="${GCP_REGION}" \
  --cross-cloud-cache="enabled"

echo "==> [4/6] Extracting auto-provisioned Google BigLake Service Account Subject ID..."
BIGLAKE_SA_ID="$(gcloud biglake iceberg catalogs describe "${GLUE_CATALOG_ID}" \
  --project="${PROJECT_ID}" \
  --format="value(biglake_service_account_id)")"

if [[ -z "${BIGLAKE_SA_ID}" ]]; then
  echo "ERROR: Could not extract biglake_service_account_id. Check gcloud output:" >&2
  gcloud biglake iceberg catalogs describe "${GLUE_CATALOG_ID}" --project="${PROJECT_ID}"
  exit 1
fi
echo "    Provisioned BigLake OIDC Subject ID: ${BIGLAKE_SA_ID}"

echo "==> [5/6] Updating AWS IAM Trust Policy with verified Google OIDC Subject ID..."
sed "s/PLACEHOLDER_BIGLAKE_SA_ID/${BIGLAKE_SA_ID}/g" \
  "${SCRIPT_DIR}/trust_policy_placeholder.json" > "${WORK_DIR}/trust_policy_final.json"

aws iam update-assume-role-policy \
  --role-name "${AWS_ROLE_NAME}" \
  --policy-document "file://${WORK_DIR}/trust_policy_final.json"

echo "    Waiting 15 seconds for AWS IAM trust propagation..."
sleep 15

echo "==> [6/6] Activating 5-minute background metadata refresh & verifying sync..."
gcloud biglake iceberg catalogs update "${GLUE_CATALOG_ID}" \
  --project="${PROJECT_ID}" \
  --refresh-interval="300s" \
  --cross-cloud-cache="enabled"

gcloud biglake iceberg catalogs describe "${GLUE_CATALOG_ID}" --project="${PROJECT_ID}"
gcloud biglake iceberg namespaces list --catalog="${GLUE_CATALOG_ID}" --project="${PROJECT_ID}"
echo "SUCCESS: AWS Glue federated catalog '${GLUE_CATALOG_ID}' is live with Cross-Cloud Intelligent Caching!"
