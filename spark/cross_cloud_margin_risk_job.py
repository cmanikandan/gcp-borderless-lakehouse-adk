"""Serverless PySpark job querying cross-cloud federated Iceberg catalogs via Lakehouse REST Catalog.

Runs on Google Cloud Managed Service for Apache Spark (Runtime 2.3+) using short-lived vended
storage credentials issued by the Google Cloud Lakehouse Iceberg REST Catalog:
  - Endpoint : https://biglake.googleapis.com/iceberg/v1/restcatalog
  - Warehouse: bl://projects/<PROJECT_ID>/catalogs/<FEDERATED_CATALOG_ID>
  - Header   : X-Iceberg-Access-Delegation=vended-credentials
  - Auth     : org.apache.iceberg.gcp.auth.GoogleAuthManager

Zero AWS IAM keys or Databricks tokens are stored on Spark driver or executor nodes.
(Note: Cross-Cloud Intelligent Caching applies to BigQuery queries; Spark reads directly from
remote storage via S3FileIO / GCSFileIO / ADLSFileIO using short-lived vended credentials.)
"""

from __future__ import annotations

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark_session(
    project_id: str,
    unity_catalog_id: str,
    glue_catalog_id: str,
    aws_region: str = "us-east-1",
    unity_storage: str = "gcs",
) -> SparkSession:
    """Configure SparkSession with Lakehouse Iceberg REST Catalog & credential vending."""
    rest_uri = "https://biglake.googleapis.com/iceberg/v1/restcatalog"
    builder = (
        SparkSession.builder.appName("borderless-lakehouse-cross-cloud-risk-scoring")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
    )

    for cat in (unity_catalog_id, glue_catalog_id):
        builder = (
            builder.config(f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{cat}.type", "rest")
            .config(f"spark.sql.catalog.{cat}.uri", rest_uri)
            .config(
                f"spark.sql.catalog.{cat}.warehouse",
                f"bl://projects/{project_id}/catalogs/{cat}",
            )
            .config(f"spark.sql.catalog.{cat}.header.x-goog-user-project", project_id)
            .config(
                f"spark.sql.catalog.{cat}.header.X-Iceberg-Access-Delegation",
                "vended-credentials",
            )
            .config(
                f"spark.sql.catalog.{cat}.rest.auth.type",
                "org.apache.iceberg.gcp.auth.GoogleAuthManager",
            )
        )

    # AWS Glue catalog reads Apache Iceberg Parquet files on Amazon S3 via vended STS credentials
    builder = (
        builder.config(
            f"spark.sql.catalog.{glue_catalog_id}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config(f"spark.sql.catalog.{glue_catalog_id}.s3.region", aws_region)
    )

    # Databricks Unity Catalog External Location FileIO ("gcs" by default in this POC, or "s3"/"adls")
    norm_storage = unity_storage.strip().lower()
    if norm_storage == "s3":
        builder = (
            builder.config(
                f"spark.sql.catalog.{unity_catalog_id}.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO",
            )
            .config(f"spark.sql.catalog.{unity_catalog_id}.s3.region", aws_region)
        )
    elif norm_storage == "adls":
        builder = builder.config(
            f"spark.sql.catalog.{unity_catalog_id}.io-impl",
            "org.apache.iceberg.azure.adlsv2.ADLSFileIO",
        )
    else:
        builder = (
            builder.config(
                f"spark.sql.catalog.{unity_catalog_id}.io-impl",
                "org.apache.iceberg.gcp.gcs.GCSFileIO",
            )
            .config(
                f"spark.sql.catalog.{unity_catalog_id}.gcs.oauth2.refresh-credentials-endpoint",
                "https://oauth2.googleapis.com/token",
            )
        )

    return builder.getOrCreate()


def main() -> None:
    project_id = os.environ.get("PROJECT_ID", "omni-retail-lakehouse-prod")
    unity_cat = os.environ.get("UNITY_FED_CATALOG_ID", "dbx_unity_merch")
    glue_cat = os.environ.get("GLUE_CATALOG_ID", "aws_glue_logistics")
    aws_region = os.environ.get("AWS_REGION", "us-east-1")
    unity_storage = os.environ.get("UNITY_STORAGE_PROVIDER", "gcs")

    spark = build_spark_session(
        project_id=project_id,
        unity_catalog_id=unity_cat,
        glue_catalog_id=glue_cat,
        aws_region=aws_region,
        unity_storage=unity_storage,
    )

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
