#!/usr/bin/env bash
# ==============================================================================
# Optional Enterprise Hardening: Private Routing via Cross-Cloud Interconnect
# Routes Metadata Refresh & Remote Object Reads privately via Service Directory
#
# Practitioner Notes:
# - Standalone Databricks Portal (Free/Community Edition) connects over TLS 1.3
#   without needing CCI; use this script for AWS Glue/S3 PrivateLink over CCI.
# - Service Directory `--network` requires numeric PROJECT_NUMBER (not PROJECT_ID).
# - For multi-zone HA or Partner CCI, front your remote VPCE IPs with a regional
#   INTERNAL_MANAGED TCP Proxy Load Balancer + NON_GCP_PRIVATE_IP_PORT Hybrid NEG
#   and pass the ILB IP to `--address`.
# - VPC Service Controls:
#     * S3/ADLS Egress Rule: Allow `blirc-...` catalog SA + query users to egress
#       to `s3://<BUCKET>` with `biglake.googleapis.com` (`*`) and
#       `bigquery.googleapis.com` (`externalResource.read`).
#     * GCS-backed Unity Catalog External Location: Add an Ingress rule allowing
#       Databricks' provisioned GCS service account even inside the same project.
# ==============================================================================
set -euo pipefail

export PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || echo 'omni-retail-lakehouse-prod')}"
export GCP_REGION="${GCP_REGION:-us-east4}"
PROJECT_NUMBER="${PROJECT_NUMBER:-$(gcloud projects describe "${PROJECT_ID}" --format='value(projectNumber)')}"
export VPC_NETWORK="${VPC_NETWORK:-projects/${PROJECT_NUMBER}/global/networks/enterprise-multicloud-vpc}"
export SD_NAMESPACE="${SD_NAMESPACE:-lakehouse-cci-ns}"
export SD_STORAGE_SVC="${SD_STORAGE_SVC:-aws-s3-vpce-privatelink}"
export SD_METADATA_SVC="${SD_METADATA_SVC:-aws-glue-vpce-privatelink}"
export S3_VPCE_PRIVATE_IP="${S3_VPCE_PRIVATE_IP:-10.42.10.25}"
export GLUE_VPCE_PRIVATE_IP="${GLUE_VPCE_PRIVATE_IP:-10.42.10.50}"
export GLUE_FED_CATALOG_ID="${GLUE_FED_CATALOG_ID:-aws_glue_logistics}"

echo "==> [1/4] Creating Service Directory Namespace and Private CCI Endpoints (Network=${VPC_NETWORK})..."
gcloud service-directory namespaces create "${SD_NAMESPACE}" \
  --location="${GCP_REGION}" \
  --project="${PROJECT_ID}" 2>/dev/null || true

# Data-plane endpoint -> AWS S3 Interface VPC Endpoint over Cross-Cloud Interconnect
gcloud service-directory services create "${SD_STORAGE_SVC}" \
  --namespace="${SD_NAMESPACE}" \
  --location="${GCP_REGION}" \
  --project="${PROJECT_ID}" 2>/dev/null || true

gcloud service-directory endpoints create "s3-vpce-ep-1" \
  --service="${SD_STORAGE_SVC}" \
  --namespace="${SD_NAMESPACE}" \
  --location="${GCP_REGION}" \
  --address="${S3_VPCE_PRIVATE_IP}" \
  --port=443 \
  --network="${VPC_NETWORK}" \
  --project="${PROJECT_ID}" 2>/dev/null || true

# Control-plane endpoint -> AWS Glue Interface VPC Endpoint over Cross-Cloud Interconnect
gcloud service-directory services create "${SD_METADATA_SVC}" \
  --namespace="${SD_NAMESPACE}" \
  --location="${GCP_REGION}" \
  --project="${PROJECT_ID}" 2>/dev/null || true

gcloud service-directory endpoints create "glue-vpce-ep-1" \
  --service="${SD_METADATA_SVC}" \
  --namespace="${SD_NAMESPACE}" \
  --location="${GCP_REGION}" \
  --address="${GLUE_VPCE_PRIVATE_IP}" \
  --port=443 \
  --network="${VPC_NETWORK}" \
  --project="${PROJECT_ID}" 2>/dev/null || true

echo "==> [2/4] Retrieving Lakehouse Catalog Service Agent Email..."
CATALOG_SA_EMAIL="$(gcloud biglake iceberg catalogs describe "${GLUE_FED_CATALOG_ID}" \
  --project="${PROJECT_ID}" \
  --format=json | python3 -c '
import json, sys
d = json.load(sys.stdin)
print(d.get("biglake-service-account") or d.get("biglake_service_account") or "")
')"

echo "==> [3/4] Granting Service Directory Viewer & PSC Authorized Service roles..."
for ROLE in "roles/servicedirectory.viewer" "roles/servicedirectory.pscAuthorizedService"; do
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${CATALOG_SA_EMAIL}" \
    --role="${ROLE}"
done

echo "==> [4/4] Updating Federated Catalog to route over Private Cross-Cloud Interconnect..."
STORAGE_SD_PATH="projects/${PROJECT_ID}/locations/${GCP_REGION}/namespaces/${SD_NAMESPACE}/services/${SD_STORAGE_SVC}"
METADATA_SD_PATH="projects/${PROJECT_ID}/locations/${GCP_REGION}/namespaces/${SD_NAMESPACE}/services/${SD_METADATA_SVC}"

gcloud biglake iceberg catalogs update "${GLUE_FED_CATALOG_ID}" \
  --project="${PROJECT_ID}" \
  --service-directory-name="${STORAGE_SD_PATH}" \
  --metadata-service-directory-name="${METADATA_SD_PATH}"

echo "SUCCESS: Both metadata sync and S3 block reads now route privately over CCI!"
