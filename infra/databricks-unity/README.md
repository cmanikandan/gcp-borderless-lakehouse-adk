# Standalone Databricks Portal Setup Guide (From Scratch — POC)

> **Proof-of-Concept (POC) Disclaimer**: These steps set up a minimal test environment in your standalone **Databricks Portal** (*Databricks Free Edition*—the modern Unity-Catalog-enabled successor to Community Edition—or a standard Databricks Workspace). For production deployments, always consult the [official Databricks Unity Catalog documentation](https://docs.databricks.com/en/data-governance/unity-catalog/index.html) and [Google Cloud Lakehouse Databricks setup guide](https://docs.cloud.google.com/lakehouse/docs/set-up-cross-cloud-connection-databricks).

---

## Step 1: Access Your Standalone Databricks Portal & Workspace URL

1. Sign up or log in at **[https://login.databricks.com](https://login.databricks.com)**.
   * **Important Hands-On Tier Note (*Free Edition* vs. *14-Day Free Trial / Standard Workspace*)**: On **Databricks Free Edition**, Databricks manages a shared multi-tenant metastore where users are **not** Metastore Admins—so the metastore-level **External data access** toggle and custom cloud **Storage Credentials** are locked (`External Data Access from non Databricks Compute environment is disabled for metastore`). To test Unity Catalog federation into Google Cloud Lakehouse, use a **14-day Databricks Free Trial** (or any Pay-As-You-Go / standard workspace where you are Metastore Admin; serverless SQL only bills for the ~10 seconds needed to run the seed query). *(If you only want to use Free Edition without upgrading, you can still run the full **AWS Glue + Amazon S3 + BigQuery** zero-ETL join immediately via `bash scripts/run_cross_cloud_join.sh --glue-only`!)*
2. Copy your workspace hostname from your browser's address bar **without** `https://` or trailing paths—for example:
   ```text
   dbc-a1b2c3d4-e5f6.cloud.databricks.com
   ```
   Save this as `DBX_WORKSPACE_HOST`.

---

## Step 2: Enable "External Data Access" on Your Unity Catalog Metastore

Google Cloud Lakehouse reads Apache Iceberg / Delta UniForm snapshots via the Unity Catalog REST API, which is disabled by default on fresh metastores:

1. In the left sidebar of your Databricks Portal, click **Catalog**.
2. Click the **Gear Icon (Metastore Settings)** at the top right (requires **Metastore Admin**).
3. Enable **External data access** (*Allow external engines to access data in Unity Catalog*).

---

## Step 3: Configure a Unity Catalog External Location

Google Cloud Lakehouse **does not support Databricks internal `default storage`**—your federated schema must live in a registered **Unity Catalog External Location** (backed by a GCS bucket or an S3 bucket you control):

1. Create a bucket in your Google Cloud project using our helper script:
   ```bash
   export PROJECT_ID="your-gcp-project-id"
   export GCP_REGION="us-east4"
   bash infra/databricks-unity/00_prepare_uc_external_storage.sh
   ```
2. In your Databricks Portal, go to **Catalog -> External Data -> Credentials -> Create credential**:
   * Choose **Google Cloud Storage** (or **AWS IAM Role** if you prefer pointing at an S3 bucket), name it `lakehouse_poc_cred`, and click **Create**.
   * Copy the **Service Account Email** Databricks displays on the credential dialog.
3. Grant that Service Account access to the bucket:
   ```bash
   export DBX_GCS_SA_EMAIL="<PASTE_DATABRICKS_GENERATED_SA_EMAIL>"
   bash infra/databricks-unity/00_prepare_uc_external_storage.sh
   ```
4. Back in the Databricks Portal, go to **Catalog -> External Data -> External Locations -> Create external location**:
   * **Name**: `lakehouse_poc_ext_loc`
   * **Storage credential**: `lakehouse_poc_cred`
   * **URL**: `gs://<YOUR_PROJECT_ID>-dbx-uc-ext/uc-external`
   * Click **Create** (and click **Test connection** to verify read/write).

---

## Step 4: Create a Databricks Service Principal & OAuth Secret

1. In your Databricks Portal, click your profile avatar (top right) -> **Settings -> Identity and access -> Service principals -> Manage -> Add service principal -> Add new**.
2. Name it `gcp-lakehouse-federation-sp` and click **Add**.
3. Open the newly created service principal -> **Secrets** tab -> click **Generate secret**:
   * Copy the **Application (Client) ID** (`UUID`) -> `DBX_SP_CLIENT_ID`
   * Copy the **Secret** immediately -> `DBX_SP_CLIENT_SECRET`

---

## Step 5: Seed the Delta UniForm (`iceberg`) Tables in Databricks SQL Editor

1. Open **SQL Editor** in the left sidebar of your Databricks Portal (attach to your Serverless Starter Warehouse).
2. Open [`databricks_uc_setup.sql`](databricks_uc_setup.sql), replace:
   * `<YOUR_UNITY_CATALOG_EXTERNAL_LOCATION_URI>` -> `gs://<YOUR_PROJECT_ID>-dbx-uc-ext/uc-external`
   * `<SERVICE_PRINCIPAL_APP_ID>` -> your `DBX_SP_CLIENT_ID` UUID (keep the backticks around the UUID!)
3. Run the script. It creates `omniretail_merch.supply_chain.supplier_risk_scores` and `sku_margins` with `'delta.universalFormat.enabledFormats' = 'iceberg'` and grants **`EXTERNAL USE SCHEMA`**.

---

## Step 6: Federate into Google Cloud Lakehouse

From your local terminal, run:

```bash
export PROJECT_ID="your-gcp-project-id"
export GCP_REGION="us-east4"
export DBX_WORKSPACE_HOST="dbc-a1b2c3d4-e5f6.cloud.databricks.com"
export DBX_UC_CATALOG="omniretail_merch"
export DBX_SP_CLIENT_ID="<YOUR_SP_APPLICATION_UUID>"
export DBX_SP_CLIENT_SECRET="<YOUR_SP_OAUTH_SECRET>"

bash infra/databricks-unity/setup_unity_federation.sh
```
