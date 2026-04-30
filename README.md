# Dremio Load

A data ingestion tool that loads data from 25+ sources into Dremio (Apache Iceberg tables) via scheduled batch jobs. Includes a full web UI for managing jobs, viewing run history, and monitoring pipeline health.

[![Docker Hub](https://img.shields.io/docker/pulls/mshainman/dremio-load?style=flat-square&logo=docker)](https://hub.docker.com/r/mshainman/dremio-load)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue?style=flat-square)](LICENSE)

**Docker Hub:** [hub.docker.com/r/mshainman/dremio-load](https://hub.docker.com/r/mshainman/dremio-load) &nbsp;·&nbsp; **[Visual User Guide](docs/VISUAL_GUIDE.html)** &nbsp;·&nbsp; **[User Guide](USER_GUIDE.md)**

```bash
docker pull mshainman/dremio-load:latest
```

## Sources

| Category | Sources |
|---|---|
| **Cloud Storage** | S3 / MinIO, Azure Blob / ADLS, Google Cloud Storage |
| **Databases** | PostgreSQL, MySQL / MariaDB, SQL Server, Oracle, MongoDB, Cassandra, ClickHouse |
| **Data Warehouses** | Snowflake, Databricks |
| **Table Formats** | Delta Lake, Apache Hudi |
| **SaaS / CRM** | Salesforce, HubSpot, Zendesk |
| **Ad Platforms** | Google Ads, LinkedIn Ads |
| **NoSQL / Cloud DB** | DynamoDB, Cosmos DB, Google Cloud Spanner, Apache Pinot |
| **Other** | Splunk, COPY INTO (Dremio-native file ingest) |

## Architecture

```
Source (read) ──► ETL Engine ──► Dremio / Iceberg (write)
                      │
                      ├── Offset store (SQLite) — incremental cursor tracking
                      ├── Schema store (SQLite) — schema evolution
                      ├── Masking engine — PII redaction
                      └── Notifier — Slack / webhook / email on failure
```

Two write modes:
- **Mode A** — Dremio SQL sink (MERGE / INSERT via REST API)
- **Mode B** — PyIceberg direct write (no Dremio SQL layer required)

## Quick Start

### Docker (recommended)

```bash
# 1. Create a named volume for persistent config and job state
docker volume create dremio-load-data

# 2. Seed a minimal config (required on first run)
docker run --rm -v dremio-load-data:/data busybox \
  sh -c "echo 'jobs: []\ntarget: {}' > /data/config.yml"

# 3. Run the container
docker run -d \
  --name dremio-load \
  -p 7071:7071 \
  -v dremio-load-data:/data \
  mshainman/dremio-load:latest
```

The UI is available at `http://localhost:7071`.

Jobs and settings created in the UI are persisted in the volume and survive container restarts.

### Docker Compose

```bash
cp config.example.yml data/config.yml   # edit as needed
docker compose up -d
```

### Local (Python)

```bash
pip install -r requirements.txt
cp config.example.yml config.yml
# Edit config.yml with your sources and Dremio target
python main.py
```

## Web UI

The built-in React UI (served by Flask on port 7071) provides:

| Page | Description |
|---|---|
| **Jobs / Sources** | Create, edit, enable/disable, and manually trigger jobs |
| **Pipeline** | Visual overview of all pipelines — source → job → target |
| **Pipeline Detail** | Per-job drill-down: source tables, run history dots, success rate, target preview |
| **Runs** | Full run history across all jobs with row counts and error messages |
| **Scheduler** | View and manage upcoming scheduled job runs |
| **Health** | Connector health checks and system status |
| **Explorer** | Browse Dremio namespaces and preview tables |
| **Copy Into** | Run Dremio-native COPY INTO operations |
| **Target** | Configure the Dremio or Iceberg write target |
| **Settings** | Secrets, notifications (Slack / email / webhook), AI Agent |

## Configuration

```yaml
target:
  mode: a                      # a = Dremio SQL | b = PyIceberg direct
  host: localhost
  port: 9047
  user: admin
  password: ${DREMIO_PASSWORD}

jobs:
  - id: my_job
    name: "My Job"
    source_type: postgres      # see Sources table above
    load_mode: incremental     # full | incremental
    schedule: "0 * * * *"     # cron expression
    connection:
      host: postgres-host
      port: 5432
      user: loader
      password: ${PG_PASSWORD}
      database: mydb
    tables:
      - public.orders
    options:
      snapshot_cursor_column: updated_at
    target_table: my_space.orders
```

See [`config.example.yml`](config.example.yml) for full examples covering Google Ads, LinkedIn Ads, S3, MySQL, and more.

## Source Configuration Examples

### S3 / MinIO
```yaml
source_type: s3
connection:
  bucket: my-bucket
  prefix: data/
  endpoint_url: http://minio:9000   # omit for AWS S3
  aws_access_key_id: ${AWS_KEY}
  aws_secret_access_key: ${AWS_SECRET}
```

### PostgreSQL / MySQL / SQL Server / Oracle
```yaml
source_type: postgres   # or mysql, sqlserver, oracle
connection:
  host: db-host
  port: 5432
  user: loader
  password: ${DB_PASSWORD}
  database: mydb
tables:
  - public.orders
  - public.customers
options:
  snapshot_cursor_column: updated_at
```

### Google Ads
```yaml
source_type: google_ads
connection:
  developer_token: ${GOOGLE_ADS_DEVELOPER_TOKEN}
  client_id: ${GOOGLE_ADS_CLIENT_ID}
  client_secret: ${GOOGLE_ADS_CLIENT_SECRET}
  refresh_token: ${GOOGLE_ADS_REFRESH_TOKEN}
  customer_id: "1234567890"
tables:
  - campaigns
  - ad_groups
  - campaign_performance
  - ad_group_performance
  - search_terms
```

> **Tip:** When creating a Google Ads job in the UI, use the **Connect with Google** button — it opens an OAuth popup and automatically fills in the refresh token.

### LinkedIn Ads
```yaml
source_type: linkedin_ads
connection:
  access_token: ${LINKEDIN_ADS_ACCESS_TOKEN}
  account_id: "123456789"
tables:
  - campaigns
  - campaign_groups
  - ad_analytics
```

### Salesforce
```yaml
source_type: salesforce
connection:
  username: ${SF_USERNAME}
  password: ${SF_PASSWORD}
  security_token: ${SF_TOKEN}
tables:
  - Account
  - Contact
  - Opportunity
```

### Snowflake
```yaml
source_type: snowflake
connection:
  account: myorg-myaccount
  user: loader
  password: ${SNOWFLAKE_PASSWORD}
  warehouse: COMPUTE_WH
  database: MYDB
  schema: PUBLIC
tables:
  - ORDERS
```

## Incremental Load

For database sources, set `load_mode: incremental` and specify a cursor column:

```yaml
options:
  snapshot_cursor_column: updated_at
```

The engine stores the last cursor value in SQLite and only fetches rows where `cursor_col > last_value` on subsequent runs. Use **Runs → Reset Offset** in the UI to force a full reload.

## PII Masking

```yaml
masking:
  fields:
    email: hash         # SHA-256 hash
    phone: redact       # replace with ***
    ssn: mask           # show last 4 only
    name: pseudonymize  # deterministic fake name
```

## Secrets

Supports environment variables (`${VAR}`) and HashiCorp Vault (`vault:secret/path#key`) in config values.

## Scheduling

Jobs run on cron schedules defined per job. The UI at `http://localhost:7071` shows job history, run status, row counts, and allows manual triggers.

## Requirements

- Python 3.11+ (if running locally)
- Dremio 24+ (for SQL sink mode)
- Docker (recommended)

Source-specific packages are included in the Docker image. For local installs:

| Source | Package |
|---|---|
| Google Ads | `google-ads>=24.0.0` |
| LinkedIn Ads | `requests` (included) |
| Salesforce | `simple-salesforce` |
| Snowflake | `snowflake-connector-python` |
| Cassandra | `cassandra-driver` |
| ClickHouse | `clickhouse-connect` |
| Delta Lake | `deltalake` |
| Cosmos DB | `azure-cosmos` |
| Google Spanner | `google-cloud-spanner` |

## Running Tests

```bash
pytest tests/ -v
```

## License

Apache 2.0
