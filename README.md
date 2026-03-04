# GCP Billing Consolidation — Setup Guide

Consolidate multiple GCP billing export tables (one per billing account) into a single BigQuery table for use as a Kion billing source.

## Prerequisites

### GCP setup (required for all options)

- [ ] A **destination GCP project** with billing enabled and these APIs:
  - `bigquery.googleapis.com`
  - `bigquerydatatransfer.googleapis.com`
- [ ] A **service account** in the destination project with:
  - `bigquery.dataEditor` on the destination project
  - `bigquery.jobUser` on the destination project
  - `bigquery.dataViewer` on all source projects (or grant at org/folder level)
  - `resourcemanager.organizationViewer` on the org (for the `discover` command only)
- [ ] A **JSON key** exported for the service account

#### Enable APIs

```bash
gcloud services enable bigquery.googleapis.com bigquerydatatransfer.googleapis.com \
  --project=DEST_PROJECT_ID
```

#### Grant service account permissions

```bash
# On the destination project:
gcloud projects add-iam-policy-binding DEST_PROJECT_ID \
  --member="serviceAccount:SA_NAME@DEST_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

# On each source project (or at the org/folder level for all):
gcloud projects add-iam-policy-binding SOURCE_PROJECT_ID \
  --member="serviceAccount:SA_NAME@DEST_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataViewer"
```

#### Export service account key

```bash
gcloud iam service-accounts keys create sa-key.json \
  --iam-account=SA_NAME@DEST_PROJECT_ID.iam.gserviceaccount.com \
  --project=DEST_PROJECT_ID
```

Place `sa-key.json` in the root of this project directory. The tool looks for it there by default (override with `--sa-key path/to/key.json`).

### Option A: Pre-built binary

Download the binary for your platform from the [GitHub releases](../../releases) page. Place it in this directory and make it executable:

```bash
chmod +x billing-consolidator-*
mv billing-consolidator-* billing-consolidator
```

### Option B: Docker

Requires **Docker** and **Docker Compose** installed.

Place your `sa-key.json` in the project root, then run commands via:

```bash
docker compose run --rm billing-consolidator <command> [flags]
```

For example:

```bash
docker compose run --rm billing-consolidator discover --org-id ORG_ID --output sources.json
```

### Option C: Build from source

Requires **Go 1.21+** installed.

```bash
go build -o billing-consolidator .
```

## Step-by-step Workflow

All examples below use `./billing-consolidator`. If using Docker, replace with:

```bash
docker compose run --rm billing-consolidator
```

### 1. Discover billing export tables

Scan the GCP organization for all billing export tables:

```bash
./billing-consolidator discover \
  --org-id ORG_ID \
  --output sources.json
```

For detailed/resource-level exports, add `--detailed`:

```bash
./billing-consolidator discover \
  --org-id ORG_ID \
  --output sources-detailed.json \
  --detailed
```

Review `sources.json` and remove any exports you don't want to consolidate.

### 2. Create the destination dataset

```bash
./billing-consolidator setup \
  --project DEST_PROJECT_ID \
  --dataset billing_consolidated
```

### 3. Register discovered sources

```bash
./billing-consolidator add-source \
  --project DEST_PROJECT_ID \
  --dataset billing_consolidated \
  --from-file sources.json
```

### 4. Run initial full load

```bash
./billing-consolidator sync \
  --project DEST_PROJECT_ID \
  --dataset billing_consolidated
```

Add `--detailed` for detailed exports. Add `--dry-run` to preview SQL without executing.

### 5. Set up scheduled daily sync

```bash
./billing-consolidator schedule \
  --project DEST_PROJECT_ID \
  --dataset billing_consolidated \
  --cron "every 24 hours" \
  --days 3 \
  --service-account SA_NAME@DEST_PROJECT_ID.iam.gserviceaccount.com
```

Add `--detailed` for detailed exports. The 3-day lookback handles GCP's late-arriving billing corrections.

### 6. Configure the billing source in Kion

- [ ] Upload the service account JSON key
- [ ] Fill in:

| Setting                      | Value                             |
| ---------------------------- | --------------------------------- |
| GCP Billing Account ID       | Your billing account ID           |
| BQ Billing Export Project ID | `DEST_PROJECT_ID`                 |
| BQ Billing Export Dataset ID | `billing_consolidated`            |
| Override Default Table Name  | `gcp_billing_export_consolidated` |
| Table Format                 | `standard` or `detailed`          |

- [ ] Test connection — should report "Billing connection successful!"
- [ ] Save

## Ongoing: Adding new billing accounts

When a new department gets a billing account:

```bash
# Register the new source
./billing-consolidator add-source \
  --project DEST_PROJECT_ID \
  --dataset billing_consolidated \
  --source-project NEW_SOURCE_PROJECT \
  --source-dataset billing_export \
  --source-table gcp_billing_export_v1_XXXXXX_YYYYYY_ZZZZZZ

# Re-run schedule to pick up the new source
./billing-consolidator schedule \
  --project DEST_PROJECT_ID \
  --dataset billing_consolidated \
  --cron "every 24 hours" \
  --days 3 \
  --service-account SA_NAME@DEST_PROJECT_ID.iam.gserviceaccount.com
```

Grant the service account `bigquery.dataViewer` on the new source project if not already covered by an org-level grant.

## Verification

```bash
# Row count and billing account count
bq query --project_id=DEST_PROJECT_ID --use_legacy_sql=false \
  "SELECT COUNT(*) as total_rows, COUNT(DISTINCT billing_account_id) as billing_accounts
   FROM \`DEST_PROJECT_ID.billing_consolidated.gcp_billing_export_consolidated\`"

# Earliest billing month
bq query --project_id=DEST_PROJECT_ID --use_legacy_sql=false \
  "SELECT MIN(invoice.month) FROM \`DEST_PROJECT_ID.billing_consolidated.gcp_billing_export_consolidated\`"

# Scheduled query status
bq ls --transfer_config --project_id=DEST_PROJECT_ID --transfer_location=us
```

## Notes

- The scheduled query SQL is generated at creation time. Re-run the `schedule` command after adding new sources.
- GCP detailed billing exports update with a **24-48 hour delay**. Allow 1-2 days after initial setup for data to appear.
- The consolidated table uses ingestion-time partitioning to match what Kion's financials-poller expects.
