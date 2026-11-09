#!/usr/bin/env bash
# ==============================================================================
# Complete POC Teardown / Uninstall Script (`infra/teardown_all.sh`)
# Cleanly removes Google Cloud, AWS, and local POC resources to prevent ongoing charges.
#
# Usage:
#   export PROJECT_ID="your-gcp-project-id"
#   export GCP_REGION="us-east4"
#   export AWS_REGION="us-east-1"
#   bash infra/teardown_all.sh
# ==============================================================================
set -euo pipefail

export PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || echo 'omni-retail-lakehouse-prod')}"
export GCP_REGION="${GCP_REGION:-us-east4}"
export UNITY_FED_CATALOG_ID="${UNITY_FED_CATALOG_ID:-dbx_unity_merch}"
export GLUE_CATALOG_ID="${GLUE_CATALOG_ID:-aws_glue_logistics}"
export DBX_SECRET_NAME="${DBX_SECRET_NAME:-dbx-unity-sp-oauth}"
export UC_EXTERNAL_GCS_BUCKET="${UC_EXTERNAL_GCS_BUCKET:-${PROJECT_ID}-dbx-uc-ext}"
export SD_NAMESPACE="${SD_NAMESPACE:-lakehouse-cci-ns}"

export AWS_REGION="${AWS_REGION:-us-east-1}"
if [[ -z "${AWS_ACCOUNT_ID:-}" ]]; then
  AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo '')"
  export AWS_ACCOUNT_ID
fi
export AWS_ROLE_NAME="${AWS_ROLE_NAME:-lakehouse_glue_federation_role}"
export GLUE_DATABASE="${GLUE_DATABASE:-fulfillment}"
export S3_ICEBERG_BUCKET="${S3_ICEBERG_BUCKET:-omniretail-logistics-iceberg-${AWS_ACCOUNT_ID}}"

echo "=============================================================================="
echo " Starting Borderless Lakehouse POC Teardown"
echo "   Google Cloud Project : ${PROJECT_ID} (${GCP_REGION})"
echo "   AWS Account / Region : ${AWS_ACCOUNT_ID:-<skip-if-unauthenticated>} (${AWS_REGION})"
echo "=============================================================================="

# ------------------------------------------------------------------------------
# 1. Google Cloud Lakehouse Federated Catalogs, Secrets & BigQuery Dataset
# ------------------------------------------------------------------------------
echo "==> [1/4] Removing Google Cloud Lakehouse Federated Catalogs, Secrets & BigQuery POC Dataset..."
for CAT in "${UNITY_FED_CATALOG_ID}" "${GLUE_CATALOG_ID}"; do
  gcloud biglake iceberg catalogs delete "${CAT}" \
    --project="${PROJECT_ID}" \
    --quiet 2>/dev/null && echo "    Deleted federated catalog: ${CAT}" || echo "    Catalog '${CAT}' not found (skipped)."
done

gcloud secrets delete "${DBX_SECRET_NAME}" \
  --location="${GCP_REGION}" \
  --project="${PROJECT_ID}" \
  --quiet 2>/dev/null && echo "    Deleted regional secret: ${DBX_SECRET_NAME}" || echo "    Secret '${DBX_SECRET_NAME}' not found (skipped)."

# Optional Service Directory CCI endpoints if created
if gcloud service-directory namespaces describe "${SD_NAMESPACE}" --location="${GCP_REGION}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  for SVC in "aws-s3-vpce-privatelink" "aws-glue-vpce-privatelink"; do
    gcloud service-directory services delete "${SVC}" \
      --namespace="${SD_NAMESPACE}" \
      --location="${GCP_REGION}" \
      --project="${PROJECT_ID}" \
      --quiet 2>/dev/null || true
  done
  gcloud service-directory namespaces delete "${SD_NAMESPACE}" \
    --location="${GCP_REGION}" \
    --project="${PROJECT_ID}" \
    --quiet 2>/dev/null || true
  echo "    Deleted Service Directory namespace: ${SD_NAMESPACE}"
fi

# Delete native BigQuery POC dataset (`commerce_core`)
bq rm -r -f -d "${PROJECT_ID}:commerce_core" 2>/dev/null \
  && echo "    Deleted BigQuery dataset: ${PROJECT_ID}:commerce_core" \
  || echo "    BigQuery dataset '${PROJECT_ID}:commerce_core' not found (skipped)."

# Delete external GCS bucket used for Databricks Unity Catalog POC (if present)
if gcloud storage buckets describe "gs://${UC_EXTERNAL_GCS_BUCKET}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud storage rm --recursive "gs://${UC_EXTERNAL_GCS_BUCKET}" --project="${PROJECT_ID}" --quiet || true
  echo "    Deleted GCS bucket: gs://${UC_EXTERNAL_GCS_BUCKET}"
fi

# ------------------------------------------------------------------------------
# 2. AWS Glue Table/Database, IAM Role & S3 Bucket
# ------------------------------------------------------------------------------
echo "==> [2/4] Removing AWS Glue Data Catalog, OIDC IAM Role & S3 Iceberg Bucket..."
if [[ -n "${AWS_ACCOUNT_ID}" ]]; then
  aws glue delete-table \
    --region "${AWS_REGION}" \
    --database-name "${GLUE_DATABASE}" \
    --name "shipment_events" 2>/dev/null && echo "    Deleted Glue table: ${GLUE_DATABASE}.shipment_events" || true

  aws glue delete-database \
    --region "${AWS_REGION}" \
    --name "${GLUE_DATABASE}" 2>/dev/null && echo "    Deleted Glue database: ${GLUE_DATABASE}" || true

  aws iam delete-role-policy \
    --role-name "${AWS_ROLE_NAME}" \
    --policy-name "LakehouseGlueIcebergReadAccess" 2>/dev/null || true

  aws iam delete-role \
    --role-name "${AWS_ROLE_NAME}" 2>/dev/null && echo "    Deleted AWS IAM role: ${AWS_ROLE_NAME}" || true

  if aws s3api head-bucket --bucket "${S3_ICEBERG_BUCKET}" 2>/dev/null; then
    aws s3 rb "s3://${S3_ICEBERG_BUCKET}" --force
    echo "    Deleted S3 bucket: s3://${S3_ICEBERG_BUCKET}"
  fi
else
  echo "    AWS CLI not authenticated; skipping AWS cleanup."
fi

# ------------------------------------------------------------------------------
# 3. Standalone Databricks Portal Cleanup Instructions
# ------------------------------------------------------------------------------
echo "==> [3/4] Standalone Databricks Portal Cleanup (Run in Databricks SQL Editor):"
cat <<'SQL'
    ------------------------------------------------------------------------
    DROP CATALOG IF EXISTS omniretail_merch CASCADE;
    DROP EXTERNAL LOCATION IF EXISTS lakehouse_poc_ext_loc;
    DROP STORAGE CREDENTIAL IF EXISTS lakehouse_poc_cred;
    -- Then remove 'gcp-lakehouse-federation-sp' under Settings -> Identity and access -> Service principals
    ------------------------------------------------------------------------
SQL

# ------------------------------------------------------------------------------
# 4. Local Python Virtualenv Cleanup (Optional)
# ------------------------------------------------------------------------------
echo "==> [4/4] Local environment note:"
echo "    To remove local Python dependencies & .env, run: rm -rf .venv .env"
echo "=============================================================================="
echo " Teardown finished!"
echo "=============================================================================="
