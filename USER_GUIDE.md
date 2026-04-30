# Dremio Load — User Guide

## Table of Contents

1. [Installation](#1-installation)
2. [First Launch](#2-first-launch)
3. [Configuring a Target (Dremio)](#3-configuring-a-target-dremio)
4. [Creating Your First Job](#4-creating-your-first-job)
5. [Running a Job](#5-running-a-job)
6. [Scheduling Jobs](#6-scheduling-jobs)
7. [Viewing Run History](#7-viewing-run-history)
8. [Pipeline View](#8-pipeline-view)
9. [Google Ads — OAuth Setup](#9-google-ads--oauth-setup)
10. [LinkedIn Ads Setup](#10-linkedin-ads-setup)
11. [Incremental vs Full Load](#11-incremental-vs-full-load)
12. [PII Masking](#12-pii-masking)
13. [Secrets Management](#13-secrets-management)
14. [Notifications](#14-notifications)
15. [Copy Into (Dremio-native file ingest)](#15-copy-into-dremio-native-file-ingest)
16. [Health & Explorer](#16-health--explorer)
17. [Upgrading](#17-upgrading)
18. [Troubleshooting](#18-troubleshooting)

---

## 1. Installation

### Option A — Docker (recommended)

Pull the image from Docker Hub:

```bash
docker pull mshainman/dremio-load:latest
```

Create a persistent volume for job state and configuration:

```bash
docker volume create dremio-load-data
```

Seed a minimal config file (required on first run):

```bash
docker run --rm -v dremio-load-data:/data busybox \
  sh -c "echo 'jobs: []\ntarget: {}' > /data/config.yml"
```

Start the container:

```bash
docker run -d \
  --name dremio-load \
  -p 7071:7071 \
  -v dremio-load-data:/data \
  mshainman/dremio-load:latest
```

Open the UI at **http://localhost:7071**.

### Option B — Docker Compose

```bash
git clone https://github.com/dremio-community/dremio-load.git
cd dremio-load
cp config.example.yml data/config.yml   # edit as needed
docker compose up -d
```

### Option C — Local Python

```bash
git clone https://github.com/dremio-community/dremio-load.git
cd dremio-load
pip install -r requirements.txt
cp config.example.yml config.yml
python main.py
```

---

## 2. First Launch

When the container starts, navigate to **http://localhost:7071**. You will see the sidebar with these sections:

- **Jobs / Sources** — manage load jobs
- **Pipeline** — visual source → job → target overview
- **Runs** — full run history
- **Scheduler** — upcoming scheduled runs
- **Health** — connector status
- **Explorer** — browse Dremio tables
- **Copy Into** — Dremio-native file ingestion
- **Target** — configure your Dremio write target
- **Settings** — secrets, notifications, AI Agent

---

## 3. Configuring a Target (Dremio)

Before creating jobs, set up where data will be written.

1. Click **Target** in the sidebar.
2. Fill in your Dremio connection details:
   - **Host** — Dremio hostname or IP
   - **Port** — default `9047`
   - **Username / Password** — Dremio credentials (or use a Personal Access Token)
   - **Catalog** — the Dremio source or space where tables will be created
   - **Schema** — default schema/folder within the catalog
3. Choose **Write Mode**:
   - **Mode A (Dremio SQL)** — writes via Dremio REST API using MERGE or INSERT statements. Requires Dremio 24+.
   - **Mode B (PyIceberg Direct)** — writes directly to Iceberg files without going through Dremio SQL. Requires an Iceberg REST catalog.
4. Click **Test Connection** to verify.
5. Click **Save**.

---

## 4. Creating Your First Job

1. Click **Jobs / Sources** in the sidebar.
2. Click **New Job**.
3. Fill in the job form:

   **Basic Info**
   - **Job Name** — a friendly name (e.g. "Postgres Orders")
   - **Source Type** — select from the dropdown (postgres, mysql, s3, google_ads, etc.)
   - **Load Mode** — `incremental` (only new/changed rows) or `full` (replace everything each run)
   - **Schedule** — cron expression (e.g. `0 * * * *` for hourly) or leave blank for manual-only

   **Connection**
   Fields change based on source type. Common fields:
   - **Host / Port** — database server address
   - **Username / Password** — credentials
   - **Database** — database name

   **Tables**
   - Enter one table name per line (e.g. `public.orders`)
   - For S3, enter path prefixes (e.g. `orders/2024/`)

   **Target Table**
   - The Dremio/Iceberg table to write to (e.g. `my_space.orders`)

4. Click **Save Job**.

The job appears in the job list. Jobs created in the UI are persisted to SQLite and will be restored automatically when the container restarts.

---

## 5. Running a Job

**Manual trigger:** Click the green **▶ Play** button on the job card, or click **Run Now** from the Pipeline Detail page.

**Scheduled run:** Jobs with a cron schedule run automatically. The Scheduler page shows upcoming runs.

While a job is running, the status badge shows **Running** with a spinner. When complete, it updates to **OK** (green) or **Error** (red).

### Enable / Disable a Job

Click the **⏸ Pause** button on the job card to toggle a job on or off. Disabled jobs are skipped by the scheduler but can still be triggered manually.

### Edit a Job

Click the **✏ Pencil** button on the job card to re-open the job form and change any settings.

### Delete a Job

Click the **🗑 Trash** button and confirm. This removes the job and its schedule but does not delete data already written to Dremio.

---

## 6. Scheduling Jobs

Dremio Load uses standard cron syntax for schedules:

```
┌─ minute (0-59)
│  ┌─ hour (0-23)
│  │  ┌─ day of month (1-31)
│  │  │  ┌─ month (1-12)
│  │  │  │  ┌─ day of week (0-7, 0=Sunday)
│  │  │  │  │
*  *  *  *  *
```

Common examples:

| Expression | Meaning |
|---|---|
| `0 * * * *` | Every hour |
| `0 6 * * *` | Daily at 6am |
| `30 5 * * 1` | Every Monday at 5:30am |
| `0 */4 * * *` | Every 4 hours |
| `0 0 1 * *` | First day of each month |

The **Scheduler** page shows all upcoming runs in chronological order so you can check for conflicts or gaps.

---

## 7. Viewing Run History

Click **Runs** in the sidebar to see the history of all job runs across all jobs.

Each row shows:
- **Status** — OK or Error
- **Job name**
- **Table** — which table was loaded
- **Rows** — rows written in that run
- **Duration** — elapsed time
- **Started** — timestamp
- **Error** — error message if the run failed

**Filter by job:** Use the dropdown at the top to narrow to a single job.

**Reset offset (force full reload):** Click the reset icon on a run row to clear the incremental cursor, so the next run re-loads all data from the beginning.

---

## 8. Pipeline View

Click **Pipeline** in the sidebar for a visual overview of all configured jobs.

Each card shows:
- **Source block** — source type and tables (click to go to Jobs page)
- **Job block** — job name, load mode, schedule
- **Target block** — Dremio host and destination table (click to go to Target page)
- **Status badge** — last run result and success rate

**Click any pipeline card** to open the **Pipeline Detail** page for that job.

### Pipeline Detail Page

The detail page shows the full end-to-end view for a single job:

- **Source panel** — emoji, connection summary, per-table row counts and status icons
- **Center panel** — load mode, schedule, success rate progress bar, run history dots (hover for details), last run summary card
- **Target panel** — Dremio host, destination table, **Preview Target Data** button (queries Dremio and shows a live data sample)
- **Per-table run history table** — the last 30 individual table runs with status, rows, duration, and error messages

Use the **Run Now** button at the top to trigger the job directly from this page.

---

## 9. Google Ads — OAuth Setup

Google Ads requires OAuth credentials. The UI makes this easy with a built-in OAuth flow.

### Prerequisites

1. A Google Cloud project with the **Google Ads API** enabled.
2. An **OAuth 2.0 Client ID** of type **Desktop app** (localhost redirect URIs are allowed automatically).
3. A Google Ads **developer token** (apply at [ads.google.com/nav/selectaccount](https://ads.google.com/nav/selectaccount) → Tools → API Center). Basic Access is required for production use.

### Creating the Job

1. Click **New Job** and select **google_ads** as the source type.
2. Fill in:
   - **Developer Token** — from Google Ads API Center
   - **Client ID** — from Google Cloud Console (OAuth 2.0 credentials)
   - **Client Secret** — from Google Cloud Console
   - **Customer ID** — your Google Ads account ID (digits only, no dashes)
3. Click **Connect with Google** — a popup opens asking you to sign in with Google and grant access.
4. After approving, the popup closes and the **refresh token is automatically filled in**.
5. Select the tables you want to load:
   - `campaigns`, `ad_groups`, `ad_group_ads`
   - `campaign_performance`, `ad_group_performance`, `search_terms`
   - `keywords`, `audience_performance`
6. Set a **Target Table** and click **Save Job**.

> **Manager (MCC) accounts:** Dremio Load automatically detects if the customer ID is a manager account. If sub-accounts exist, metrics are pulled from the first sub-account. If no sub-accounts are linked, metric tables return empty results gracefully.

---

## 10. LinkedIn Ads Setup

1. Create a LinkedIn Developer App at [linkedin.com/developers](https://www.linkedin.com/developers/apps).
2. Request the **Marketing Developer Platform** product for your app.
3. Generate an access token with `r_ads` and `r_ads_reporting` scopes.
4. Create a new job with source type **linkedin_ads**.
5. Enter your **Access Token** and **Account ID**.

Available tables: `campaigns`, `campaign_groups`, `creatives`, `ad_analytics`, `ad_analytics_creative`.

---

## 11. Incremental vs Full Load

### Incremental (recommended for large tables)

```yaml
load_mode: incremental
options:
  snapshot_cursor_column: updated_at
```

On each run the engine:
1. Reads the last cursor value from SQLite.
2. Queries the source for rows where `updated_at > last_value`.
3. Writes those rows to Dremio using MERGE (upsert) or INSERT.
4. Saves the new cursor value.

Use this for tables with a reliable `updated_at`, `created_at`, or auto-increment `id` column.

### Full Snapshot

```yaml
load_mode: full
```

On each run the engine reads the entire source table and replaces the Dremio table. Use for small lookup/reference tables or when the source has no reliable cursor column.

### Resetting the Offset

To force a full reload on the next incremental run, go to **Runs**, find any run for that job, and click the reset icon. The cursor is cleared and the next run will load all rows.

---

## 12. PII Masking

Add a `masking` block to any job to redact sensitive fields before writing to Dremio:

```yaml
masking:
  fields:
    email: hash         # SHA-256 hex digest
    phone: redact       # replaced with ***
    ssn: mask           # shows last 4: ***-**-1234
    name: pseudonymize  # deterministic fake name (same input → same output)
```

Masking is applied in-memory before any data reaches Dremio — the original values are never written.

---

## 13. Secrets Management

### Environment Variables

Reference env vars in config values using `${VAR_NAME}`:

```yaml
password: ${DREMIO_PASSWORD}
```

Set them in your shell or in a `.env` file before starting the container.

### HashiCorp Vault

```yaml
password: vault:secret/dremio#password
```

Configure Vault in the **Settings** page or in `config.yml`:

```yaml
secrets:
  vault:
    url: https://vault.example.com
    auth_method: token
    token: ${VAULT_TOKEN}
    mount: secret
```

---

## 14. Notifications

Configure alerts for job failures (or successes) in **Settings → Notifications**.

Supported channels:

| Channel | What you need |
|---|---|
| **Slack** | Incoming webhook URL |
| **Email** | SMTP host, port, credentials, from/to addresses |
| **Webhook** | Any HTTP endpoint — receives a JSON POST on each run event |

Click **Test** after saving to send a test notification.

---

## 15. Copy Into (Dremio-native file ingest)

The **Copy Into** page lets you run Dremio's native `COPY INTO` SQL — useful when files already live in a Dremio-registered S3, GCS, or ADLS source and you want Dremio to read them directly without the ETL engine.

1. Click **Copy Into** in the sidebar.
2. Select your Dremio source and folder path.
3. Choose file format (Parquet, JSON, CSV).
4. Select the target Iceberg table.
5. Click **Preview** to see matched files, then **Run** to execute.

You can also configure Copy Into jobs in `config.yml`:

```yaml
- id: s3_customers
  name: "S3 Customers"
  source_type: copy_into
  schedule: "30 * * * *"
  source_location: "@my_s3_source/customers/"
  target_table: my_space.customers
  file_format: parquet
```

---

## 16. Health & Explorer

### Health

Click **Health** to see connector status for all configured jobs — whether the source is reachable, last ping time, and any connection errors.

### Explorer

Click **Explorer** to browse your Dremio namespace without leaving the UI:
- Expand catalogs, schemas, and tables.
- Click any table to see a live data preview (up to 100 rows).
- Useful for confirming that jobs wrote data successfully.

---

## 17. Upgrading

To upgrade to the latest version:

```bash
docker pull mshainman/dremio-load:latest
docker stop dremio-load
docker rm dremio-load
docker run -d \
  --name dremio-load \
  -p 7071:7071 \
  -v dremio-load-data:/data \
  mshainman/dremio-load:latest
```

Your jobs, offsets, and schemas are stored in the `dremio-load-data` volume and are preserved across upgrades.

---

## 18. Troubleshooting

### UI not loading

Check that the container is running and the port is mapped correctly:

```bash
docker ps
docker logs dremio-load
```

### Job shows "Never run" after restart

Jobs created in the UI are restored from SQLite automatically on startup. If a job is missing after a restart, check the container logs for restore errors. Ensure the volume is mounted at `/data` and `/data/config.yml` exists (even if empty).

### "Config file not found: /data/config.yml"

The volume exists but the config file was not seeded. Run:

```bash
docker run --rm -v dremio-load-data:/data busybox \
  sh -c "echo 'jobs: []\ntarget: {}' > /data/config.yml"
```

Then restart the container.

### Google Ads: DEVELOPER_TOKEN_NOT_APPROVED

Your Google Ads developer token is at **Test** level, which only works against test accounts. Apply for **Basic Access** at [ads.google.com](https://ads.google.com) → Tools → API Center.

### Google Ads: REQUESTED_METRICS_FOR_MANAGER

The customer ID you entered is a Manager (MCC) account. Dremio Load auto-detects this and routes metric queries to a sub-account. If no sub-accounts are linked, metric tables will return 0 rows — this is expected.

### "DremioSink has no attribute 'close'" (older versions)

This was fixed in v1.2. Pull the latest image:

```bash
docker pull mshainman/dremio-load:latest
```

### Incremental job re-loading all data

The offset cursor was reset, or this is the first run. After the first successful run, only new/changed rows will be fetched. To reset intentionally, use the reset icon in the Runs page.

### Connection test fails in Target page

- Verify the host and port are reachable from inside the container (not just from your laptop).
- If Dremio is running in another Docker container, use the container name or Docker network IP as the host, not `localhost`.
- Check that the Dremio user has permission to create tables in the target catalog/schema.
