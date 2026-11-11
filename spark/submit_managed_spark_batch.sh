#!/usr/bin/env bash
# ==============================================================================
# Submit Serverless Managed Spark Batch Job Against Federated Iceberg Catalogs
# Uses Google Cloud Managed Service for Apache Spark Runtime 2.3 (bundles
# Iceberg + GoogleAuthManager + S3FileIO / GCSFileIO natively).
#
# Practitioner Networking Note:
# Managed Spark serverless batches do not assign public IPs to workers by default.
# To reach Amazon S3 over the internet (when not using Cross-Cloud Interconnect),
# ensure Cloud Router + Cloud NAT are configured on your Spark subnet/VPC
# (set ENSURE_CLOUD_NAT=true below to auto-create them on your VPC).
# ==============================================================================
set -euo pipefail

export PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || echo 'omni-retail-lakehouse-prod')}"
export GCP_REGION="${GCP_REGION:-us-east4}"
export STAGING_BUCKET="${STAGING_BUCKET:-${PROJECT_ID}-spark-staging}"
export UNITY_FED_CATALOG_ID="${UNITY_FED_CATALOG_ID:-dbx_unity_merch}"
export GLUE_CATALOG_ID="${GLUE_CATALOG_ID:-aws_glue_logistics}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export UNITY_STORAGE_PROVIDER="${UNITY_STORAGE_PROVIDER:-gcs}"
export SPARK_VPC_SUBNET="${SPARK_VPC_SUBNET:-default}"
export ENSURE_CLOUD_NAT="${ENSURE_CLOUD_NAT:-false}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BATCH_ID="lakehouse-cross-cloud-risk-$(date +%s)"

if [[ "${ENSURE_CLOUD_NAT}" == "true" ]]; then
  echo "==> Configuring Cloud Router + Cloud NAT for outbound Managed Spark S3 access..."
  gcloud compute routers create "lakehouse-spark-nat-router" \
    --project="${PROJECT_ID}" \
    --region="${GCP_REGION}" \
    --network="default" 2>/dev/null || true

  gcloud compute routers nats create "lakehouse-spark-nat-gw" \
    --project="${PROJECT_ID}" \
    --region="${GCP_REGION}" \
    --router="lakehouse-spark-nat-router" \
    --auto-allocate-nat-external-ips \
    --nat-all-subnet-ip-ranges 2>/dev/null || true
fi

echo "==> [1/2] Ensuring Spark staging bucket 'gs://${STAGING_BUCKET}' exists & uploading PySpark script..."
if ! gcloud storage buckets describe "gs://${STAGING_BUCKET}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud storage buckets create "gs://${STAGING_BUCKET}" \
    --project="${PROJECT_ID}" \
    --location="${GCP_REGION}" \
    --uniform-bucket-level-access
fi
gcloud storage cp "${SCRIPT_DIR}/cross_cloud_margin_risk_job.py" "gs://${STAGING_BUCKET}/jobs/cross_cloud_margin_risk_job.py"

echo "==> [2/2] Submitting Serverless Managed Spark 2.3 batch (${BATCH_ID})..."
SPARK_PROPS="spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
SPARK_PROPS+=",spark.dataproc.driverEnv.PROJECT_ID=${PROJECT_ID}"
SPARK_PROPS+=",spark.dataproc.driverEnv.UNITY_FED_CATALOG_ID=${UNITY_FED_CATALOG_ID}"
SPARK_PROPS+=",spark.dataproc.driverEnv.GLUE_CATALOG_ID=${GLUE_CATALOG_ID}"
SPARK_PROPS+=",spark.dataproc.driverEnv.AWS_REGION=${AWS_REGION}"
SPARK_PROPS+=",spark.dataproc.driverEnv.UNITY_STORAGE_PROVIDER=${UNITY_STORAGE_PROVIDER}"
SPARK_PROPS+=",spark.executorEnv.PROJECT_ID=${PROJECT_ID}"

gcloud dataproc batches submit pyspark \
  "gs://${STAGING_BUCKET}/jobs/cross_cloud_margin_risk_job.py" \
  --project="${PROJECT_ID}" \
  --region="${GCP_REGION}" \
  --batch="${BATCH_ID}" \
  --version="2.3" \
  --subnet="${SPARK_VPC_SUBNET}" \
  --deps-bucket="gs://${STAGING_BUCKET}" \
  --properties="${SPARK_PROPS}"
