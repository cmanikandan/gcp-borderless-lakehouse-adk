#!/usr/bin/env bash
# ==============================================================================
# Google Cloud Borderless Lakehouse -> Standalone Databricks Unity Catalog (POC)
# Works directly with your Databricks Portal (Free / Community Edition or Workspace)
# Supports:
#   - Mode A (AUTH_MODE="secret", Default): Regional Secret Manager OAuth Secret
#   - Mode B (AUTH_MODE="oidc"): Secretless OIDC + Optional User Identity Federation
# ==============================================================================
set -euo pipefail

export PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || echo 'omni-retail-lakehouse-prod')}"
export GCP_REGION="${GCP_REGION:-us-east4}"
# Your standalone Databricks workspace hostname from your browser portal (no https://)
export DBX_WORKSPACE_HOST="${DBX_WORKSPACE_HOST:-dbc-a1b2c3d4-e5f6.cloud.databricks.com}"
export DBX_UC_CATALOG="${DBX_UC_CATALOG:-omniretail_merch}"
export DBX_UC_SCHEMA="${DBX_UC_SCHEMA:-supply_chain}"
export UNITY_FED_CATALOG_ID="${UNITY_FED_CATALOG_ID:-dbx_unity_merch}"

# Choose AUTH_MODE="secret" (fastest in Databricks Free/Community Portal) or "oidc"
export AUTH_MODE="${AUTH_MODE:-secret}"
export DBX_SP_CLIENT_ID="${DBX_SP_CLIENT_ID:-00000000-1111-2222-3333-444444444444}"
export DBX_SP_CLIENT_SECRET="${DBX_SP_CLIENT_SECRET:-replace-with-sp-oauth-secret}"
export DBX_SECRET_NAME="${DBX_SECRET_NAME:-dbx-unity-sp-oauth}"

# Optional User Identity Federation ("service-identity" default, or "user-identity-federation")
export IDENTITY_MODE="${IDENTITY_MODE:-service-identity}"

# Regional Secret Manager requires the regional API endpoint override (`rep.googleapis.com`)
REGIONAL_SM_ENDPOINT="https://secretmanager.${GCP_REGION}.rep.googleapis.com/"

echo "==> [1/5] Enabling BigLake, BigQuery & Secret Manager APIs..."
gcloud services enable \
  biglake.googleapis.com \
  bigquery.googleapis.com \
  secretmanager.googleapis.com \
  --project="${PROJECT_ID}"

if [[ "${AUTH_MODE}" == "secret" ]]; then
  echo "==> [2/5] Storing Databricks Portal Service Principal OAuth Secret in Regional Secret Manager (${GCP_REGION})..."
  WORK_DIR="$(mktemp -d)"
  trap 'rm -rf "${WORK_DIR}"' EXIT
  cat > "${WORK_DIR}/credentials.json" <<JSON
{
  "client_id": "${DBX_SP_CLIENT_ID}",
  "client_secret": "${DBX_SP_CLIENT_SECRET}"
}
JSON

  CLOUDSDK_API_ENDPOINT_OVERRIDES_SECRETMANAGER="${REGIONAL_SM_ENDPOINT}" \
    gcloud secrets create "${DBX_SECRET_NAME}" \
      --location="${GCP_REGION}" \
      --project="${PROJECT_ID}" \
      --data-file="${WORK_DIR}/credentials.json" 2>/dev/null || \
  CLOUDSDK_API_ENDPOINT_OVERRIDES_SECRETMANAGER="${REGIONAL_SM_ENDPOINT}" \
    gcloud secrets versions add "${DBX_SECRET_NAME}" \
      --location="${GCP_REGION}" \
      --project="${PROJECT_ID}" \
      --data-file="${WORK_DIR}/credentials.json"

  SECRET_RESOURCE="projects/${PROJECT_ID}/locations/${GCP_REGION}/secrets/${DBX_SECRET_NAME}"

  echo "==> [3/5] Ensuring Lakehouse Federated Catalog (${UNITY_FED_CATALOG_ID}) exists with Intelligent Caching..."
  if ! gcloud biglake iceberg catalogs describe "${UNITY_FED_CATALOG_ID}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
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
  fi

  CATALOG_SA_EMAIL="$(gcloud biglake iceberg catalogs describe "${UNITY_FED_CATALOG_ID}" \
    --project="${PROJECT_ID}" \
    --format=json | python3 -c '
import json, sys
d = json.load(sys.stdin)
print(d.get("biglake-service-account") or d.get("biglake_service_account") or d.get("biglakeServiceAccount") or "")
')"

  echo "==> [4/5] Granting Regional Secret Accessor to Lakehouse Service Agent (${CATALOG_SA_EMAIL})..."
  CLOUDSDK_API_ENDPOINT_OVERRIDES_SECRETMANAGER="${REGIONAL_SM_ENDPOINT}" \
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
  echo "==> [2/5] Creating Lakehouse Federated Catalog (${UNITY_FED_CATALOG_ID}) for Secretless OIDC..."
  if ! gcloud biglake iceberg catalogs describe "${UNITY_FED_CATALOG_ID}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    gcloud biglake iceberg catalogs create "${UNITY_FED_CATALOG_ID}" \
      --project="${PROJECT_ID}" \
      --catalog-type="federated" \
      --federated-catalog-type="unity" \
      --unity-instance-name="${DBX_WORKSPACE_HOST}" \
      --unity-catalog-name="${DBX_UC_CATALOG}" \
      --unity-service-principal-application-id="${DBX_SP_CLIENT_ID}" \
      --federated-catalog-identity-mode="${IDENTITY_MODE}" \
      --cross-cloud-cache="enabled" \
      --primary-location="${GCP_REGION}"
  fi

  BIGLAKE_SA_ID="$(gcloud biglake iceberg catalogs describe "${UNITY_FED_CATALOG_ID}" \
    --project="${PROJECT_ID}" \
    --format=json | python3 -c '
import json, sys
d = json.load(sys.stdin)
print(d.get("biglake-service-account-id") or d.get("biglake_service_account_id") or "")
')"

  echo "==> [3/5] Configure Base Databricks Service Principal OIDC Federation Policy:"
  cat <<EOF
    Run via Databricks CLI (Account Admin) before or right after activating refresh:
      databricks account service-principal-federation-policy create <SP_NUMERIC_ID> --json '{
        "oidc_policy": {
          "issuer": "https://accounts.google.com",
          "audiences": ["https://accounts.cloud.databricks.com"],
          "subject": "${BIGLAKE_SA_ID}"
        }
      }'
EOF

  echo "==> [4/5] Activating 5-minute background metadata refresh..."
  gcloud biglake iceberg catalogs update "${UNITY_FED_CATALOG_ID}" \
    --project="${PROJECT_ID}" \
    --refresh-interval="300s" \
    --cross-cloud-cache="enabled"
fi

if [[ "${IDENTITY_MODE}" == "user-identity-federation" ]]; then
  echo "    Extracting OIDC Issuer & Audience for Databricks User Identity Federation..."
  read -r ISSUER_URL AUDIENCE_ID < <(gcloud biglake iceberg catalogs describe "${UNITY_FED_CATALOG_ID}" \
    --project="${PROJECT_ID}" \
    --format=json | python3 -c '
import json, sys
d = json.load(sys.stdin)
print(d.get("issuer-url") or d.get("issuer_url") or "<PENDING_ORG_ISSUER>", d.get("audience") or "<POPULATED_AFTER_FIRST_SYNC>")
')
  cat <<EOF
    [User Identity Federation] Register Account-Wide OIDC Policy in Databricks:
      databricks account federation-policy create --json '{
        "oidc_policy": {
          "issuer": "${ISSUER_URL}",
          "audiences": ["${AUDIENCE_ID}"]
        }
      }'
EOF
fi

echo "==> [5/5] Verifying synchronized Databricks Unity Catalog namespaces & tables..."
gcloud biglake iceberg catalogs describe "${UNITY_FED_CATALOG_ID}" --project="${PROJECT_ID}"
gcloud biglake iceberg namespaces list \
  --project="${PROJECT_ID}" \
  --catalog="${UNITY_FED_CATALOG_ID}" || true
gcloud biglake iceberg tables list \
  --project="${PROJECT_ID}" \
  --catalog="${UNITY_FED_CATALOG_ID}" \
  --namespace="${DBX_UC_SCHEMA}" || true
