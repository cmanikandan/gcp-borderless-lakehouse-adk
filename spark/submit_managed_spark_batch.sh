#!/usr/bin/env bash
# ==============================================================================
# Submit Serverless PySpark Job to Managed Service for Apache Spark
# Prerequisite: Ensure Cloud Router + Cloud NAT (or CCI) is active on the subnet
# so Spark executors can reach S3 with Lakehouse vended credentials.
# ==============================================================================
set -euo pipefail

export PROJECT_ID="${PROJECT_ID:-omni-retail-lakehouse-prod}"
export GCP_REGION="${GCP_REGION:-us-east4}"
export SUBNET="${SUBNET:-default}"
export STAGING_BUCKET="${STAGING_BUCKET:-omniretail-spark-staging-use4}"

gsutil cp "$(dirname "$0")/cross_cloud_margin_risk_job.py" "gs://${STAGING_BUCKET}/jobs/cross_cloud_margin_risk_job.py"

gcloud dataproc batches submit pyspark \
  "gs://${STAGING_BUCKET}/jobs/cross_cloud_margin_risk_job.py" \
  --project="${PROJECT_ID}" \
  --region="${GCP_REGION}" \
  --version="2.3" \
  --subnet="${SUBNET}" \
  --properties="spark.executorEnv.PROJECT_ID=${PROJECT_ID}"
