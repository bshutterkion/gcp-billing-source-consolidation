# GCP Billing Export Consolidator

CLI tool that consolidates multiple GCP billing export tables (one per billing account) into a single BigQuery table for use as a Kion billing source. Supports both standard (`gcp_billing_export_v1_*`) and detailed/resource-level (`gcp_billing_export_resource_v1_*`) exports.

## Background

Northwestern University is transitioning from 2 shared billing IDs to 39+ individual billing IDs (one per department). Each billing ID has its own BigQuery billing export in a separate project (in Burwood's org). Kion's billing source config expects a single `project.dataset.table` path. This tool consolidates all exports into one table.

## Prerequisites

- **Go 1.25+** installed
- **gcloud CLI** installed and authenticated:
  - `gcloud auth login` (for gcloud commands)
  - `gcloud auth application-default login` (for the Go tool's ADC auth)
- A **destination GCP project** with billing enabled and the following APIs:
  - `bigquery.googleapis.com`
  - `bigquerydatatransfer.googleapis.com` (for the `schedule` command)
- A **service account** in the destination project with:
  - `roles/bigquery.dataEditor` on the destination project (write consolidated data)
  - `roles/bigquery.jobUser` on the destination project (run queries)
  - `roles/bigquery.dataViewer` on **all source projects** (read billing exports)
  - `roles/resourcemanager.organizationViewer` (for `discover` command only)
- Export a **JSON key** for the service account — this is uploaded to Kion as well

### Enabling APIs

```bash
gcloud services enable bigquery.googleapis.com bigquerydatatransfer.googleapis.com \
  --project=DEST_PROJECT_ID
```

### Service Account Permissions

Grant read access to each source project (or at the org/folder level for all):

```bash
# Per source project:
gcloud projects add-iam-policy-binding SOURCE_PROJECT_ID \
  --member="serviceAccount:SA_NAME@DEST_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataViewer"

# On the destination project:
gcloud projects add-iam-policy-binding DEST_PROJECT_ID \
  --member="serviceAccount:SA_NAME@DEST_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"
```

For Northwestern with 39+ source projects, grant `roles/bigquery.dataViewer` at the **org or folder level** instead of per-project.

### Create Service Account Key

```bash
gcloud iam service-accounts keys create sa-key.json \
  --iam-account=SA_NAME@DEST_PROJECT_ID.iam.gserviceaccount.com \
  --project=DEST_PROJECT_ID
```

This key file is used both by the CLI tool (`--sa-key`) and uploaded to Kion.

## Build

```bash
go build -o billing-consolidator .
```

## Workflow

### 1. Discover billing export tables

Scan all projects in the GCP organization for billing export tables:

```bash
./billing-consolidator discover \
  --org-id ORG_ID \
  --output sources.json
```

This outputs a JSON file listing all `gcp_billing_export_v1_*` (standard) tables found. Detailed/resource-level exports (`gcp_billing_export_resource_v1_*`) are excluded by default.

To discover detailed/resource-level exports instead:

```bash
./billing-consolidator discover \
  --org-id ORG_ID \
  --output sources-detailed.json \
  --detailed
```

**Filtering sources:** If the org has more billing exports than you want to consolidate, edit `sources.json` to remove the ones you don't need before registering them in step 3.

### 2. Set up destination dataset

Create the destination dataset and metadata table:

```bash
./billing-consolidator setup \
  --project DEST_PROJECT_ID \
  --dataset billing_consolidated
```

This is idempotent — safe to run multiple times. Creates:

- The `billing_consolidated` dataset (US multi-region)
- The `billing_export_sources` metadata table to track registered sources

### 3. Register discovered sources

Load the discovered sources into the metadata table:

```bash
./billing-consolidator add-source \
  --project DEST_PROJECT_ID \
  --dataset billing_consolidated \
  --from-file sources.json
```

Duplicates are automatically skipped. Both standard and detailed sources can coexist in the metadata table — the `--detailed` flag on `sync` and `schedule` controls which type is used at runtime.

### 4. Run initial full load

Consolidate all source tables into a single table:

```bash
./billing-consolidator sync \
  --project DEST_PROJECT_ID \
  --dataset billing_consolidated
```

This creates `gcp_billing_export_consolidated` with **ingestion-time partitioning** and clustered by `billing_account_id`. Ingestion-time partitioning is used (rather than column-based partitioning) to match the partitioning scheme of standard GCP billing export tables — this ensures the `_PARTITIONTIME` pseudo-column is available, which Kion's financials-poller requires.

To sync detailed/resource-level exports instead of standard, add `--detailed`:

```bash
./billing-consolidator sync \
  --project DEST_PROJECT_ID \
  --dataset billing_consolidated \
  --detailed
```

To preview the SQL without executing:

```bash
./billing-consolidator sync \
  --project DEST_PROJECT_ID \
  --dataset billing_consolidated \
  --dry-run
```

**Schema differences are handled automatically.** The tool discovers each source's schema (including nested RECORD/STRUCT fields) and generates explicit column lists. Sources missing columns get `NULL AS column_name`; sources with mismatched struct sub-fields (e.g., `invoice` having different sub-fields) get reconstructed with `STRUCT(...)` expressions.

### 5. Run incremental sync

For ongoing daily syncs, use incremental mode (processes last N days only):

```bash
./billing-consolidator sync \
  --project DEST_PROJECT_ID \
  --dataset billing_consolidated \
  --incremental --days 3
```

The 3-day lookback window handles GCP's late-arriving billing corrections.

### 6. Set up scheduled query

Create a BQ Data Transfer scheduled query for automatic daily sync:

```bash
./billing-consolidator schedule \
  --project DEST_PROJECT_ID \
  --dataset billing_consolidated \
  --cron "every 24 hours" \
  --days 3 \
  --service-account SA_NAME@DEST_PROJECT_ID.iam.gserviceaccount.com
```

The `--service-account` flag is **required** — it specifies which service account runs the scheduled query. This SA needs read access to all source projects (see Prerequisites).

To schedule sync for detailed/resource-level exports, add `--detailed`:

```bash
./billing-consolidator schedule \
  --project DEST_PROJECT_ID \
  --dataset billing_consolidated \
  --cron "every 24 hours" \
  --days 3 \
  --service-account SA_NAME@DEST_PROJECT_ID.iam.gserviceaccount.com \
  --detailed
```

The scheduled query appears in the BigQuery console under **Data Transfers** (project: DEST_PROJECT_ID, location: us).

**Important:** The scheduled query SQL is generated statically at creation time. You must re-run the `schedule` command whenever:

- New sources are added with `add-source`
- A full `sync` is run (which may recreate the table with updated schema/partitioning)
- Source table schemas change (new columns added by GCP)

### 7. List registered sources

```bash
./billing-consolidator list-sources \
  --project DEST_PROJECT_ID \
  --dataset billing_consolidated
```

### 8. Add a new source later

When a new department gets a billing account (expected 1-2/month):

```bash
./billing-consolidator add-source \
  --project DEST_PROJECT_ID \
  --dataset billing_consolidated \
  --source-project proj-dept-new \
  --source-dataset billing_export \
  --source-table gcp_billing_export_v1_XXXXXX_YYYYYY_ZZZZZZ
```

Then re-run the schedule command to pick up the new source:

```bash
./billing-consolidator schedule \
  --project DEST_PROJECT_ID \
  --dataset billing_consolidated \
  --cron "every 24 hours" \
  --days 3 \
  --service-account SA_NAME@DEST_PROJECT_ID.iam.gserviceaccount.com
```

Also grant the SA read access on the new source project if not already covered by an org-level grant:

```bash
gcloud projects add-iam-policy-binding proj-dept-new \
  --member="serviceAccount:SA_NAME@DEST_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataViewer"
```

## Kion Billing Source Configuration

After the consolidated table is populated, configure the billing source in Kion:

1. **Upload the service account JSON key** (the same one used by the tool)
2. Fill in the billing source fields:

| Setting                                 | Value                                                                                                      |
| --------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| **Name**                                | (descriptive name, e.g., "Northwestern Consolidated Billing")                                              |
| **GCP Billing Account ID**              | The billing account ID (e.g., `01847D-AAED78-8435AF`)                                                      |
| **Billing Start Date**                  | The earliest `invoice.month` in the data (query with: `SELECT MIN(invoice.month) FROM consolidated_table`) |
| **Big Query Billing Export Project ID** | `DEST_PROJECT_ID`                                                                                          |
| **BigQuery Billing Export Dataset ID**  | `billing_consolidated`                                                                                     |
| **Override Default Table Name**         | `gcp_billing_export_consolidated`                                                                          |
| **Table Format**                        | `standard` (or `detailed` if consolidating detailed exports)                                               |

1. Test the connection — Kion should report "Billing connection successful!"
2. Save — Kion will discover all GCP projects with charges under that billing account in the "Accounts not in Kion" section

### Finding the billing start date

```bash
bq query --project_id=DEST_PROJECT_ID --use_legacy_sql=false \
  "SELECT MIN(invoice.month) as earliest_month
   FROM \`DEST_PROJECT_ID.billing_consolidated.gcp_billing_export_consolidated\`"
```

## Global Flags

| Flag        | Description                                             |
| ----------- | ------------------------------------------------------- |
| `--project` | Destination GCP project ID                              |
| `--dataset` | Destination BigQuery dataset ID                         |
| `--sa-key`  | Path to service account JSON key file (defaults to ADC) |

### Per-command Flags

| Flag         | Commands                       | Description                                                                                         |
| ------------ | ------------------------------ | --------------------------------------------------------------------------------------------------- |
| `--detailed` | `discover`, `sync`, `schedule` | Operate on detailed/resource-level exports (`gcp_billing_export_resource_v1_*`) instead of standard |

## Architecture

### Metadata-driven source registry

Sources are tracked in a `billing_export_sources` table in the destination dataset. Adding a new billing export is a single INSERT — no SQL changes needed. Only registered sources are included in the consolidation.

### Ingestion-time partitioning

The consolidated table uses **ingestion-time partitioning** (`PARTITION BY _PARTITIONDATE`), matching the partitioning scheme of standard GCP billing export tables. This is critical because Kion's financials-poller queries the `_PARTITIONTIME` pseudo-column — which only exists on ingestion-time partitioned tables. Column-based partitioning (e.g., `PARTITION BY DATE(usage_start_time)`) would cause the poller to fail with "error finding monthly usage data in BigQuery".

Because BigQuery DDL cannot create ingestion-time partitioned tables via CTAS, the full load uses a three-step script: DROP, CREATE TABLE with explicit schema, then INSERT INTO with an explicit column list (also required for ingestion-time partitioned tables).

### Partition replacement strategy

Incremental syncs delete and re-insert the last N days (default 3) rather than rebuilding the entire table. This is efficient at scale and handles GCP's billing correction window.

### Schema compatibility

Both standard (`gcp_billing_export_v1_*`) and detailed (`gcp_billing_export_resource_v1_*`) exports generally share the same schema within their type, but older exports may have fewer columns or struct fields that differ (e.g., `price` may have 4 sub-fields in one source vs 7 in another). The tool handles this automatically:

- Discovers the full schema of each source table via the BQ API
- Computes the superset of all columns and nested struct fields
- Generates explicit SELECT lists with `NULL AS col` for missing top-level columns
- Reconstructs STRUCT fields with `STRUCT(existing.field, CAST(NULL AS <type>) AS missing_field)` for mismatched nested types, using each sub-field's actual type (e.g., `NUMERIC`, `STRING`)

The `billing_account_id` column is present in every row, so data lineage is preserved natively after UNION ALL.

## Verification

After running the full workflow, verify:

```bash
# Row count matches sum of individual sources
bq query --project_id=DEST_PROJECT_ID --use_legacy_sql=false \
  "SELECT COUNT(*) as total_rows, COUNT(DISTINCT billing_account_id) as billing_accounts
   FROM \`DEST_PROJECT_ID.billing_consolidated.gcp_billing_export_consolidated\`"

# Check the scheduled query status in BQ console
bq ls --transfer_config --project_id=DEST_PROJECT_ID --transfer_location=us

# Trigger a manual run to test the schedule
bq mk --transfer_run --project_id=DEST_PROJECT_ID \
  --run_time="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  projects/PROJECT_NUMBER/locations/us/transferConfigs/CONFIG_ID
```

## Troubleshooting

### "Billing has not been enabled for this project"

The destination project needs a billing account linked. BQ DML queries (INSERT, DELETE) are not available in the free tier.

### "Permission denied" on source tables

The service account needs `roles/bigquery.dataViewer` on each source project. For many sources, grant at the org or folder level instead.

### "Failed to find a valid credential" when creating scheduled query

The `--service-account` flag is required for the `schedule` command. Pass the email of a service account that has the necessary BQ permissions.

### Scheduled query shows FAILED state

Check the run logs:

```bash
bq show --format=prettyjson --transfer_run \
  projects/PROJECT_NUMBER/locations/us/transferConfigs/CONFIG_ID/runs/RUN_ID
```

Common causes:

- Service account lacks `bigquery.dataViewer` on a source project
- Service account lacks `bigquery.jobUser` on the destination project
- A source table was deleted or renamed

### Scheduled query not picking up new sources

The scheduled query SQL is generated statically at creation time. After adding new sources with `add-source`, re-run the `schedule` command to regenerate the SQL.

### "Column count mismatch" or "incompatible types" in UNION ALL

This is handled automatically by the tool's schema discovery. If you see this error, it likely means someone ran a manual `SELECT *` query. Use the tool's `sync` command instead, which generates schema-aware SQL.

### Kion shows "Could not find the specified billing table"

Make sure you set the **Override Default Table Name** to `gcp_billing_export_consolidated` and **Table Format** to `standard`. Without the table name override, Kion looks for the default `gcp_billing_export_v1_*` pattern which won't exist in the consolidated dataset.

### Kion financials-poller: "error finding monthly usage data in BigQuery"

This error means the consolidated table is missing the `_PARTITIONTIME` pseudo-column. This happens if the table was created with column-based partitioning (e.g., `PARTITION BY DATE(usage_start_time)`) instead of ingestion-time partitioning. Re-run `sync` (full load) to recreate the table with the correct partitioning scheme. The tool uses `PARTITION BY _PARTITIONDATE` which enables the `_PARTITIONTIME` pseudo-column that Kion expects.

### Large initial load takes too long

The full load processes all historical data. Consider running with `--dry-run` first to review the SQL, then execute directly in the BQ console where you can monitor progress.
