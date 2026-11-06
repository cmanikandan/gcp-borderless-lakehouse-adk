"""Serverless PySpark job querying cross-cloud federated Iceberg catalogs via Lakehouse REST Catalog.

Runs on Google Cloud Managed Service for Apache Spark using short-lived vended S3 credentials
issued by the Lakehouse Runtime Catalog (`X-Iceberg-Access-Delegation=vended-credentials`).
Zero AWS IAM keys or static S3 secrets are stored in Spark!
"""

from __future__ import annotations

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark_session(project_id: str, unity_catalog_id: str, glue_catalog_id: str) -> SparkSession:
    rest_uri = "https://biglake.googleapis.com/iceberg/v1/restcatalog/ext"
    builder = (
        SparkSession.builder.appName("borderless-lakehouse-cross-cloud-risk-scoring")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
    )

    # Register both federated catalogs via Lakehouse Iceberg REST Catalog + Credential Vending
    for cat in (unity_catalog_id, glue_catalog_id):
        builder = (
            builder.config(f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{cat}.type", "rest")
            .config(f"spark.sql.catalog.{cat}.uri", rest_uri)
            .config(f"spark.sql.catalog.{cat}.warehouse", f"bl://{cat}")
            .config(f"spark.sql.catalog.{cat}.header.x-goog-user-project", project_id)
            .config(
                f"spark.sql.catalog.{cat}.header.X-Iceberg-Access-Delegation",
                "vended-credentials",
            )
            .config(f"spark.sql.catalog.{cat}.rest.auth.type", "google")
        )

    return builder.getOrCreate()


def main() -> None:
    project_id = os.environ.get("PROJECT_ID", "omni-retail-lakehouse-prod")
    unity_cat = os.environ.get("UNITY_FED_CATALOG_ID", "dbx_unity_merch")
    glue_cat = os.environ.get("GLUE_CATALOG_ID", "aws_glue_logistics")

    spark = build_spark_session(project_id, unity_cat, glue_cat)

    suppliers_df = spark.table(f"{unity_cat}.supply_chain.supplier_risk_scores")
    margins_df = spark.table(f"{unity_cat}.supply_chain.sku_margins")
    shipments_df = spark.table(f"{glue_cat}.fulfillment.shipment_events")

    enriched_df = (
        shipments_df.join(margins_df, on="sku_id", how="inner")
        .join(suppliers_df, on="supplier_id", how="inner")
        .withColumn(
            "composite_disruption_index",
            F.round(
                (F.col("delay_hours") / 24.0) * 0.55 + F.col("geopolitical_risk_score") * 0.45,
                4,
            ),
        )
        .filter(F.col("composite_disruption_index") >= 0.50)
    )

    enriched_df.select(
        "order_id",
        "sku_id",
        "supplier_name",
        "shipment_status",
        "delay_hours",
        "composite_disruption_index",
    ).orderBy(F.desc("composite_disruption_index")).show(20, truncate=False)


if __name__ == "__main__":
    main()
