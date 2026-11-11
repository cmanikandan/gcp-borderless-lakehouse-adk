#!/usr/bin/env bash
# ==============================================================================
# Step 0 (AWS Prerequisite): Bootstrap S3 Bucket, AWS Glue DB & Iceberg Table
# Proof-of-Concept (POC) helper using AWS CLI + serverless Amazon Athena v3.
# Zero EC2 or EMR clusters required!
# ==============================================================================
set -euo pipefail

export AWS_REGION="${AWS_REGION:-us-east-1}"
if [[ -z "${AWS_ACCOUNT_ID:-}" ]]; then
  AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
  export AWS_ACCOUNT_ID
fi
export S3_ICEBERG_BUCKET="${S3_ICEBERG_BUCKET:-omniretail-logistics-iceberg-${AWS_ACCOUNT_ID}}"
export GLUE_DATABASE="${GLUE_DATABASE:-fulfillment}"
export ATHENA_WORKGROUP="${ATHENA_WORKGROUP:-primary}"
ATHENA_OUTPUT_URI="s3://${S3_ICEBERG_BUCKET}/athena-query-results/"

echo "==> [1/4] Creating Amazon S3 Bucket (${S3_ICEBERG_BUCKET}) in ${AWS_REGION}..."
if ! aws s3api head-bucket --bucket "${S3_ICEBERG_BUCKET}" 2>/dev/null; then
  if [[ "${AWS_REGION}" == "us-east-1" ]]; then
    aws s3api create-bucket --bucket "${S3_ICEBERG_BUCKET}" --region "${AWS_REGION}"
  else
    aws s3api create-bucket \
      --bucket "${S3_ICEBERG_BUCKET}" \
      --region "${AWS_REGION}" \
      --create-bucket-configuration LocationConstraint="${AWS_REGION}"
  fi
fi

# Enforce S3 public access block & default server-side encryption
aws s3api put-public-access-block \
  --bucket "${S3_ICEBERG_BUCKET}" \
  --public-access-block-configuration \
  "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

aws s3api put-bucket-encryption \
  --bucket "${S3_ICEBERG_BUCKET}" \
  --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'

echo "==> [2/4] Creating AWS Glue Database (${GLUE_DATABASE})..."
aws glue create-database \
  --region "${AWS_REGION}" \
  --database-input "{\"Name\":\"${GLUE_DATABASE}\",\"Description\":\"POC Apache Iceberg logistics database for Google Cloud Lakehouse federation\"}" \
  2>/dev/null || echo "    Glue database '${GLUE_DATABASE}' already exists."

run_athena_sql() {
  local sql_stmt="$1"
  local label="$2"
  echo "    Running Athena query (${label})..."
  local qid
  qid="$(aws athena start-query-execution \
    --region "${AWS_REGION}" \
    --work-group "${ATHENA_WORKGROUP}" \
    --query-execution-context "Database=${GLUE_DATABASE}" \
    --result-configuration "OutputLocation=${ATHENA_OUTPUT_URI}" \
    --query-string "${sql_stmt}" \
    --query "QueryExecutionId" \
    --output text)"

  while true; do
    local state
    state="$(aws athena get-query-execution \
      --region "${AWS_REGION}" \
      --query-execution-id "${qid}" \
      --query "QueryExecution.Status.State" \
      --output text)"
    if [[ "${state}" == "SUCCEEDED" ]]; then
      echo "    [${label}] SUCCEEDED (QueryExecutionId=${qid})"
      break
    elif [[ "${state}" == "FAILED" || "${state}" == "CANCELLED" ]]; then
      local reason
      reason="$(aws athena get-query-execution \
        --region "${AWS_REGION}" \
        --query-execution-id "${qid}" \
        --query "QueryExecution.Status.StateChangeReason" \
        --output text)"
      echo "ERROR: Athena query '${label}' ${state}: ${reason}" >&2
      exit 1
    fi
    sleep 2
  done
}

echo "==> [3/4] Creating Apache Iceberg v2 table '${GLUE_DATABASE}.shipment_events' on S3..."
CREATE_TBL_SQL="CREATE TABLE IF NOT EXISTS ${GLUE_DATABASE}.shipment_events (
  event_id string,
  order_id string,
  sku_id string,
  carrier string,
  origin_port string,
  shipment_status string,
  delay_hours int,
  event_ts timestamp
)
LOCATION 's3://${S3_ICEBERG_BUCKET}/warehouse/${GLUE_DATABASE}/shipment_events/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet',
  'write_compression' = 'zstd'
)"
run_athena_sql "${CREATE_TBL_SQL}" "CREATE_ICEBERG_TABLE"

echo "==> [4/4] Upserting sample shipment delay events into '${GLUE_DATABASE}.shipment_events' (idempotent MERGE)..."
MERGE_SQL="MERGE INTO ${GLUE_DATABASE}.shipment_events AS target
USING (
  VALUES
    ('SHP-5001', 'ORD-8801', 'SKU-9001', 'OceanicExpress', 'KAOHSIUNG', 'PORT_CONGESTION_HOLD', 28, current_timestamp),
    ('SHP-5002', 'ORD-8802', 'SKU-9003', 'PacificAirCargo', 'HO_CHI_MINH', 'CUSTOMS_INSPECTION_DELAY', 19, current_timestamp),
    ('SHP-5003', 'ORD-8803', 'SKU-9002', 'NordicFreight', 'GOTHENBURG', 'IN_TRANSIT_ON_TIME', 2, current_timestamp),
    ('SHP-5004', 'ORD-8804', 'SKU-9004', 'GreatLakesRail', 'CHICAGO', 'WEATHER_REROUTE_DELAY', 36, current_timestamp)
) AS source (event_id, order_id, sku_id, carrier, origin_port, shipment_status, delay_hours, event_ts)
ON target.event_id = source.event_id
WHEN MATCHED THEN UPDATE SET
  shipment_status = source.shipment_status,
  delay_hours = source.delay_hours,
  event_ts = source.event_ts
WHEN NOT MATCHED THEN INSERT (
  event_id, order_id, sku_id, carrier, origin_port, shipment_status, delay_hours, event_ts
) VALUES (
  source.event_id, source.order_id, source.sku_id, source.carrier, source.origin_port,
  source.shipment_status, source.delay_hours, source.event_ts
)"
run_athena_sql "${MERGE_SQL}" "SEED_SHIPMENT_EVENTS"

echo ""
echo "SUCCESS: AWS S3 bucket '${S3_ICEBERG_BUCKET}' and Glue Iceberg table '${GLUE_DATABASE}.shipment_events' are ready!"
echo "Next step: Run 'bash infra/aws-glue/setup_glue_federation.sh' to federate into Google Cloud Lakehouse."
