"""Federated Apache Iceberg Catalog Discovery via Google Cloud BigLake REST API.

Standard BigQuery metadata APIs (`datasets.list` / `tables.get` used by ADK's built-in
`list_dataset_ids` and `get_table_info`) only enumerate 3-part native BigQuery datasets
and cannot see 4-part federated catalogs (`project.catalog.namespace.table`).

This module queries Google Cloud Lakehouse's Iceberg REST Catalog API directly so ADK
agents can dynamically discover synchronized Databricks Unity Catalog and AWS Glue
namespaces, tables, active snapshot IDs, and Iceberg column schemas at runtime.
"""

from __future__ import annotations

from typing import Any

import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.oauth2.credentials import Credentials

from lakehouse_agent.config import load_settings

BIGLAKE_ICEBERG_REST_V1 = "https://biglake.googleapis.com/iceberg/v1/restcatalog/v1"


def discover_federated_catalog_tables(
    catalog_id: str,
    namespace: str = "",
    include_schema: bool = True,
    project_id: str = "",
    user_access_token: str | None = None,
) -> dict[str, Any]:
    """ADK Tool: Discover namespaces, Iceberg tables, 4-part SQL paths, and column schemas.

    Args:
        catalog_id: Federated catalog ID (e.g. 'dbx_unity_merch' or 'aws_glue_logistics').
        namespace: Optional namespace/schema filter (e.g. 'supply_chain' or 'fulfillment').
        include_schema: If True, fetches Iceberg column names, types, and snapshot metadata.
        project_id: Optional Google Cloud project override (defaults to configured PROJECT_ID).
        user_access_token: Optional end-user OAuth access token for Unity Catalog user federation.

    Returns:
        Dictionary containing discovered namespaces, 4-part BigQuery SQL table identifiers,
        remote storage locations, current snapshot IDs, and Iceberg column definitions.
    """
    cfg = load_settings()
    target_project = project_id.strip() or cfg.project_id

    if user_access_token:
        creds = Credentials(token=user_access_token)
    else:
        creds, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )

    session = AuthorizedSession(creds)
    headers = {"x-goog-user-project": target_project}
    catalog_root = (
        f"{BIGLAKE_ICEBERG_REST_V1}/projects/{target_project}/catalogs/{catalog_id}"
    )

    if namespace.strip():
        namespaces = [namespace.strip()]
    else:
        ns_resp = session.get(f"{catalog_root}/namespaces", headers=headers, timeout=15)
        ns_resp.raise_for_status()
        namespaces = [
            ".".join(parts) for parts in ns_resp.json().get("namespaces", [])
        ]

    discovered_tables: list[dict[str, Any]] = []
    for ns in namespaces:
        # Per Apache Iceberg REST spec, multi-part namespaces encode '.' as %1F
        ns_encoded = ns.replace(".", "%1F")
        tbl_resp = session.get(
            f"{catalog_root}/namespaces/{ns_encoded}/tables",
            headers=headers,
            timeout=15,
        )
        tbl_resp.raise_for_status()

        for ident in tbl_resp.json().get("identifiers", []):
            tbl_name = ident.get("name")
            if not tbl_name:
                continue
            entry: dict[str, Any] = {
                "namespace": ns,
                "table": tbl_name,
                "bigquery_sql_identifier": f"`{target_project}.{catalog_id}.{ns}.{tbl_name}`",
            }
            if include_schema:
                meta_resp = session.get(
                    f"{catalog_root}/namespaces/{ns_encoded}/tables/{tbl_name}",
                    headers=headers,
                    timeout=15,
                )
                if meta_resp.ok:
                    metadata = meta_resp.json().get("metadata", {})
                    current_schema_id = metadata.get("current-schema-id")
                    schemas = metadata.get("schemas", [])
                    active_schema = next(
                        (s for s in schemas if s.get("schema-id") == current_schema_id),
                        schemas[0] if schemas else {},
                    )
                    entry["location"] = metadata.get("location")
                    entry["current_snapshot_id"] = metadata.get("current-snapshot-id")
                    entry["columns"] = [
                        {
                            "name": f.get("name"),
                            "type": f.get("type"),
                            "required": f.get("required", False),
                        }
                        for f in active_schema.get("fields", [])
                    ]
            discovered_tables.append(entry)

    return {
        "status": "SUCCESS",
        "project_id": target_project,
        "catalog_id": catalog_id,
        "namespaces": namespaces,
        "table_count": len(discovered_tables),
        "tables": discovered_tables,
    }
