#!/usr/bin/env bash
# ==============================================================================
# Google Cloud Borderless Lakehouse -> Standalone Databricks Unity Catalog
# Works directly with your Databricks Portal (Free / Community Edition or Enterprise)
# Supports:
#   - Mode A (Default for Portal Quickstart): Regional Secret Manager OAuth Secret
#   - Mode B (Enterprise Secretless): OIDC Service Principal + User Identity Federation
# ==============================================================================
set -euo pipefail

export PROJECT_ID="${PROJECT_ID:-omni-retail-lakehouse-prod}"
export GCP_REGION="${GCP_REGION:-us-east4}"
# Your standalone Databricks workspace hostname from your browser portal (no https://)
export DBX_WORKSPACE_HOST="${DBX_WORKSPACE_HOST:-dbc-a1b2c3d4-e5f6.cloud.databricks.com}"
export DBX_UC_CATALOG="${DBX_UC_CATALOG:-omniretail_merch}"
export UNITY_FED_CATALOG_ID="${UNITY_FED_CATALOG_ID:-dbx_unity_merch}"

# Choose AUTH_MODE="secret" (easiest in Databricks Free/Community Portal) or "oidc"
export AUTH_MODE="${AUTH_MODE:-secret}"
export DBX_SP_CLIENT_ID="${DBX_SP_CLIENT_ID:-00000000-1111-2222-3333-444444444444}"
export DBX_SP_CLIENT_SECRET="${DBX_SP_CLIENT_SECRET:-replace-with-sp-oauth-secret}"
export DBX_SECRET_NAME="${DBX_SECRET_NAME:-dbx-unity-sp-oauth}"

# Optional User Identity Federation ("service-identity" default, or "user-identity-federation")
export IDENTITY_MODE="${IDENTITY_MODE:-service-identity}"

echo "==> [1/4] Enabling BigLake & Secret Manager APIs..."
gcloud services enable biglake.googleapis.com secretmanager.googleapis.com --project="${PROJECT_ID}"

if [[ "${AUTH_MODE}" == "secret" ]]; then
  echo "==> [2/4] Storing Databricks Portal Service Principal OAuth Secret in Regional Secret Manager..."
  WORK_DIR="$(mktemp -d)"
  trap 'rm -rf "${WORK_DIR}"' EXIT
  cat > "${WORK_DIR}/credentials.json" <<JSON
{
  "client_id": "${DBX_SP_CLIENT_ID}",
  "client_secret": "${DBX_SP_CLIENT_SECRET}"
}
JSON

  gcloud secrets create "${DBX_SECRET_NAME}" \
    --location="${GCP_REGION}" \
    --project="${PROJECT_ID}" \
    --data-file="${WORK_DIR}/credentials.json" || \
  gcloud secrets versions add "${DBX_SECRET_NAME}" \
    --location="${GCP_REGION}" \
    --project="${PROJECT_ID}" \
    --data-file="${WORK_DIR}/credentials.json"

  SECRET_RESOURCE="projects/${PROJECT_ID}/locations/${GCP_REGION}/secrets/${DBX_SECRET_NAME}"

  echo "==> [3/4] Creating Lakehouse Federated Catalog (${UNITY_FED_CATALOG_ID}) with Intelligent Caching..."
  gcloud biglake iceberg catalogs create "${UNITY_FED_CATALOG_ID}" \
    --project="${PROJECT_ID}" \
    --catalog-type="federated" \
    --federated-catalog-type="unity" \
    --unity-instance-name="${DBX_WORKSPACE_HOST}" \
    --unity-catalog-name="${DBX_UC_CATALOG}" \
    --secret-name="${SECRET_RESOURCE}" \
    --federated-catalog-identity-mode="${IDENTITY_MODE}" \
    --cross-cloud-cache="enabled" \
    --primary-location="${GCP_REGION}"

  CATALOG_SA_EMAIL="$(gcloud biglake iceberg catalogs describe "${UNITY_FED_CATALOG_ID}" \
    --project="${PROJECT_ID}" \
    --format="value(biglake_service_account)")"

  echo "    Granting Secret Accessor to Lakehouse Service Agent (${CATALOG_SA_EMAIL})..."
  gcloud secrets add-iam-policy-binding "${DBX_SECRET_NAME}" \
    --location="${GCP_REGION}" \
    --project="${PROJECT_ID}" \
    --member="serviceAccount:${CATALOG_SA_EMAIL}" \
    --role="roles/secretmanager.secretAccessor"

  gcloud biglake iceberg catalogs update "${UNITY_FED_CATALOG_ID}" \
    --project="${PROJECT_ID}" \
    --refresh-interval="300s" \
    --cross-cloud-cache="enabled"
else
  echo "==> [2/4] Creating Lakehouse Federated Catalog using Secretless OIDC..."
  gcloud biglake iceberg catalogs create "${UNITY_FED_CATALOG_ID}" \
    --project="${PROJECT_ID}" \
    --catalog-type="federated" \
    --federated-catalog-type="unity" \
    --unity-instance-name="${DBX_WORKSPACE_HOST}" \
    --unity-catalog-name="${DBX_UC_CATALOG}" \
    --unity-service-principal-application-id="${DBX_SP_CLIENT_ID}" \
    --federated-catalog-identity-mode="${IDENTITY_MODE}" \
    --cross-cloud-cache="enabled" \
    --primary-location="${GCP_REGION}" \
    --refresh-interval="300s"
fi

echo "==> [4/4] Verifying synchronized Databricks Unity Catalog namespaces..."
gcloud biglake iceberg catalogs describe "${UNITY_FED_CATALOG_ID}" --project="${PROJECT_ID}"
gcloud biglake iceberg namespaces list \
  --project="${PROJECT_ID}" \
  --catalog="${UNITY_FED_CATALOG_ID}"
