# ==============================================================================
# Google Cloud Borderless Lakehouse POC — Practitioner Makefile
# Run `make help` to view all end-to-end bootstrap, testing, agent & teardown commands.
# ==============================================================================
.PHONY: help install doctor test demo-finops agent-topology inspect-agent agent-web adk-web agent-cli \
        bootstrap-aws federate-glue prepare-dbx-bucket federate-unity \
        seed-bq bq-cc-connection query-benchmark spark-submit teardown

help: ## Show all available POC commands
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

install: ## Install Python 3.11+ dependencies & Google ADK 2.x via uv
	uv sync --extra dev

doctor: ## Validate local CLIs, region co-location & environment setup
	uv run python scripts/preflight_doctor.py

test: ## Run pytest suite, shell script syntax checks & XML/SVG diagram validation
	uv run pytest -v
	@for shf in infra/*.sh infra/*/*.sh spark/*.sh scripts/*.sh; do bash -n "$$shf" && echo "OK: $$shf"; done
	uv run python -c "import glob, xml.etree.ElementTree as ET; [ET.parse(p) for p in ['docs/diagrams/borderless_lakehouse_architecture.drawio', *sorted(glob.glob('docs/diagrams/*.svg'))]]; print('OK: all XML/SVG diagrams')"

demo-finops: ## Run offline Cold vs. Warm Intelligent Cache & Agentic FinOps benchmark
	uv run python scripts/demo_agent_cache_finops.py

inspect-agent: ## Inspect the Google ADK 2.x (gemini-3.8-flash) multi-agent & tool topology
	uv run python scripts/run_agent_cli.py --inspect-topology

agent-topology: inspect-agent

adk-web: ## Launch interactive Google ADK Dev Web UI (`adk web src`)
	uv run adk web src

agent-web: adk-web

agent-cli: ## Run a live prompt through the ADK copilot in your terminal
	uv run python scripts/run_agent_cli.py

bootstrap-aws: ## Phase 1A: Create S3 bucket, Glue DB & seed Athena Iceberg table from scratch
	bash infra/aws-glue/00_bootstrap_aws_s3_glue_iceberg.sh

federate-glue: ## Phase 1B: Federate AWS Glue Data Catalog into Lakehouse with Intelligent Cache
	bash infra/aws-glue/setup_glue_federation.sh

prepare-dbx-bucket: ## Phase 2A: Create external GCS bucket for Databricks Unity Catalog POC
	bash infra/databricks-unity/00_prepare_uc_external_storage.sh

federate-unity: ## Phase 2B: Federate Standalone Databricks Unity Catalog into Lakehouse
	bash infra/databricks-unity/setup_unity_federation.sh

seed-bq: ## Phase 3: Enable GCP APIs & seed native BigQuery `commerce_core.orders` table
	bash infra/gcp-bigquery/setup_bigquery_native.sh

bq-cc-connection: ## Optional: Create BigQuery AWS Cross-Cloud Connection with Intelligent Cache
	bash infra/gcp-bigquery/setup_bq_cross_cloud_connection_cache.sh

query-benchmark: ## Phase 4A: Run Cold vs. Warm 3-platform BigQuery SQL join (`--nouse_cache`)
	bash scripts/run_cross_cloud_join.sh

spark-submit: ## Phase 4B: Submit Managed Spark 2.3 batch with Iceberg REST vended credentials
	bash spark/submit_managed_spark_batch.sh

teardown: ## Phase 6: Uninstall all Google Cloud, AWS & local POC resources cleanly
	bash infra/teardown_all.sh
