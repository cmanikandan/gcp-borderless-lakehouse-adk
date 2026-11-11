#!/usr/bin/env bash
# ==============================================================================
# Google Cloud Borderless Lakehouse -> AWS Glue Data Catalog Federation + Cache
# Cloud Practitioner Proof-of-Concept (POC) Script
# Supports both Standard AWS Glue Data Catalog and Amazon S3 Tables (`s3tablescatalog`)
# ==============================================================================
set -euo pipefail

export PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || echo 'omni-retail-lakehouse-prod')}"
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
# Optional: set S3_TABLE_BUCKET="my-s3-table-bucket" to federate an Amazon S3 Tables catalog
export S3_TABLE_BUCKET="${S3_TABLE_BUCKET:-}"

if [[ -n "${S3_TABLE_BUCKET}" ]]; then
  GLUE_WAREHOUSE="${AWS_ACCOUNT_ID}:s3tablescatalog/${S3_TABLE_BUCKET}"
else
  GLUE_WAREHOUSE="${AWS_ACCOUNT_ID}"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

echo "==> [1/6] Enabling required Google Cloud APIs..."
gcloud services enable biglake.googleapis.com bigquery.googleapis.com --project="${PROJECT_ID}"

echo "==> [2/6] Creating AWS IAM Role (${AWS_ROLE_NAME}) with placeholder OIDC trust policy..."
# 12-hour max session duration (43200s) prevents token expiry on long analytical scans
aws iam create-role \
  --role-name "${AWS_ROLE_NAME}" \
  --assume-role-policy-document "file://${SCRIPT_DIR}/trust_policy_placeholder.json" \
  --max-session-duration 43200 \
  --description "OIDC federated role for Google Cloud Borderless Lakehouse POC" 2>/dev/null || true

# Portable template substitution (works on both macOS and Linux without gettext/envsubst)
python3 - "${SCRIPT_DIR}/permissions_policy.json" "${WORK_DIR}/permissions_policy_rendered.json" <<'PY'
import os, sys
src, dst = sys.argv[1], sys.argv[2]
text = open(src, encoding="utf-8").read()
for k in ("AWS_REGION", "AWS_ACCOUNT_ID", "GLUE_DATABASE", "S3_ICEBERG_BUCKET"):
    text = text.replace(f"${{{k}}}", os.environ[k])
open(dst, "w", encoding="utf-8").write(text)
PY

aws iam put-role-policy \
  --role-name "${AWS_ROLE_NAME}" \
  --policy-name "LakehouseGlueIcebergReadAccess" \
  --policy-document "file://${WORK_DIR}/permissions_policy_rendered.json"

AWS_ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${AWS_ROLE_NAME}"

echo "==> [3/6] Ensuring Lakehouse Federated Catalog (${GLUE_CATALOG_ID}) exists with Intelligent Caching..."
# Practitioner Tip: Omit --refresh-interval on initial creation so background sync
# does not fail before AWS IAM OIDC trust policy propagation completes.
if ! gcloud biglake iceberg catalogs describe "${GLUE_CATALOG_ID}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud biglake iceberg catalogs create "${GLUE_CATALOG_ID}" \
    --project="${PROJECT_ID}" \
    --catalog-type="federated" \
    --federated-catalog-type="glue" \
    --glue-warehouse="${GLUE_WAREHOUSE}" \
    --glue-aws-region="${AWS_REGION}" \
    --glue-aws-role-arn="${AWS_ROLE_ARN}" \
    --primary-location="${GCP_REGION}" \
    --cross-cloud-cache="enabled"
else
  echo "    Catalog '${GLUE_CATALOG_ID}' already exists; proceeding to OIDC trust verification."
fi

echo "==> [4/6] Extracting auto-provisioned Google BigLake Service Account Subject ID..."
# Official REST extension property is kebab-case `biglake-service-account-id`
BIGLAKE_SA_ID="$(gcloud biglake iceberg catalogs describe "${GLUE_CATALOG_ID}" \
  --project="${PROJECT_ID}" \
  --format=json | python3 -c '
import json, sys
d = json.load(sys.stdin)
print(d.get("biglake-service-account-id") or d.get("biglake_service_account_id") or d.get("biglakeServiceAccountId") or "")
')"

if [[ -z "${BIGLAKE_SA_ID}" ]]; then
  echo "ERROR: Could not extract biglake-service-account-id. Check gcloud output:" >&2
  gcloud biglake iceberg catalogs describe "${GLUE_CATALOG_ID}" --project="${PROJECT_ID}"
  exit 1
fi
echo "    Provisioned BigLake OIDC Subject/Audience ID: ${BIGLAKE_SA_ID}"

echo "==> [5/6] Updating AWS IAM Trust Policy (accounts.google.com:aud & accounts.google.com:sub)..."
sed "s/PLACEHOLDER_BIGLAKE_SA_ID/${BIGLAKE_SA_ID}/g" \
  "${SCRIPT_DIR}/trust_policy_placeholder.json" > "${WORK_DIR}/trust_policy_final.json"

aws iam update-assume-role-policy \
  --role-name "${AWS_ROLE_NAME}" \
  --policy-document "file://${WORK_DIR}/trust_policy_final.json"

echo "    Waiting 15 seconds for AWS IAM trust propagation..."
sleep 15

echo "==> [6/6] Activating 5-minute background metadata refresh & verifying Iceberg table sync..."
gcloud biglake iceberg catalogs update "${GLUE_CATALOG_ID}" \
  --project="${PROJECT_ID}" \
  --refresh-interval="300s" \
  --cross-cloud-cache="enabled"

gcloud biglake iceberg catalogs describe "${GLUE_CATALOG_ID}" --project="${PROJECT_ID}"
gcloud biglake iceberg namespaces list --catalog="${GLUE_CATALOG_ID}" --project="${PROJECT_ID}"
gcloud biglake iceberg tables list \
  --catalog="${GLUE_CATALOG_ID}" \
  --namespace="${GLUE_DATABASE}" \
  --project="${PROJECT_ID}" || true

echo "SUCCESS: AWS Glue federated catalog '${GLUE_CATALOG_ID}' is live with Cross-Cloud Intelligent Caching!"
