#!/usr/bin/env bash
# ==============================================================================
# Step 0 (Databricks Prerequisite): Prepare External Storage Bucket for Unity Catalog
# Proof-of-Concept (POC) helper for Standalone Databricks Portal (Free Edition)
#
# Why is this needed?
# Google Cloud Lakehouse requires Unity Catalog schemas/tables to reside in a
# registered Unity Catalog External Location (NOT Databricks internal default storage).
#
# Usage:
#   # 1. Create the external GCS bucket in your Google Cloud project:
#   export PROJECT_ID="your-gcp-project-id"
#   bash infra/databricks-unity/00_prepare_uc_external_storage.sh
#
#   # 2. After creating a Storage Credential in Databricks Portal -> Catalog ->
#   #    External Data -> Credentials (which gives you a Databricks GCS Service Account
#   #    email), re-run with DBX_GCS_SA_EMAIL set to grant bucket access:
#   export DBX_GCS_SA_EMAIL="dbx-uc-sa@....iam.gserviceaccount.com"
#   bash infra/databricks-unity/00_prepare_uc_external_storage.sh
# ==============================================================================
set -euo pipefail

export PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || echo 'omni-retail-lakehouse-prod')}"
export GCP_REGION="${GCP_REGION:-us-east4}"
export UC_EXTERNAL_GCS_BUCKET="${UC_EXTERNAL_GCS_BUCKET:-${PROJECT_ID}-dbx-uc-ext}"

echo "==> [1/2] Ensuring Google Cloud Storage bucket 'gs://${UC_EXTERNAL_GCS_BUCKET}' exists in ${GCP_REGION}..."
if ! gcloud storage buckets describe "gs://${UC_EXTERNAL_GCS_BUCKET}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud storage buckets create "gs://${UC_EXTERNAL_GCS_BUCKET}" \
    --project="${PROJECT_ID}" \
    --location="${GCP_REGION}" \
    --uniform-bucket-level-access
  echo "    Created bucket: gs://${UC_EXTERNAL_GCS_BUCKET}"
else
  echo "    Bucket already exists: gs://${UC_EXTERNAL_GCS_BUCKET}"
fi

if [[ -n "${DBX_GCS_SA_EMAIL:-}" ]]; then
  echo "==> [2/2] Granting Databricks Unity Catalog Storage Credential SA (${DBX_GCS_SA_EMAIL}) access..."
  gcloud storage buckets add-iam-policy-binding "gs://${UC_EXTERNAL_GCS_BUCKET}" \
    --member="serviceAccount:${DBX_GCS_SA_EMAIL}" \
    --role="roles/storage.legacyBucketReader"

  gcloud storage buckets add-iam-policy-binding "gs://${UC_EXTERNAL_GCS_BUCKET}" \
    --member="serviceAccount:${DBX_GCS_SA_EMAIL}" \
    --role="roles/storage.objectAdmin"

  echo ""
  echo "SUCCESS! Now in your Databricks Portal:"
  echo "  1. Go to Catalog -> External Data -> External Locations -> Create external location"
  echo "  2. Set URL to: gs://${UC_EXTERNAL_GCS_BUCKET}/uc-external"
  echo "  3. Select your Storage Credential and click Create."
  echo "  4. Use 'gs://${UC_EXTERNAL_GCS_BUCKET}/uc-external/supply_chain/' as MANAGED LOCATION in databricks_uc_setup.sql!"
else
  echo ""
  echo "==> [2/2] Next Step in your Databricks Portal:"
  echo "  1. Open Catalog -> External Data -> Credentials -> Create credential"
  echo "  2. Select 'Google Cloud Storage' (or AWS IAM Role if using an S3 bucket), name it 'lakehouse_poc_cred', and click Create."
  echo "  3. Copy the generated Service Account email and run:"
  echo "       export DBX_GCS_SA_EMAIL='<PASTE_DATABRICKS_GENERATED_SA_EMAIL>'"
  echo "       bash infra/databricks-unity/00_prepare_uc_external_storage.sh"
fi
