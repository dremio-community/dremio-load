# Dremio Load

A data ingestion framework that loads data from 25+ sources into Dremio (Apache Iceberg tables) via scheduled batch jobs. Supports full and incremental load modes, schema evolution, PII masking, offset tracking, and webhook notifications.

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
| **Other** | Splunk |

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
- **Mode B** — PyIceberg direct write (no Dremio required)

## Quick Start

### Docker (recommended)

```bash
# Copy and edit the config
cp config.example.yml config.yml
vi config.yml

# Start
docker compose up -d
```

The UI is available at `http://localhost:5050`.

### Local

```bash
pip install -r requirements.txt
cp config.example.yml config.yml
# Edit config.yml with your sources and Dremio target
python main.py
```

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
      host: postgres
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

See [`config.example.yml`](config.example.yml) for full examples including Google Ads, LinkedIn Ads, S3, MySQL, and more.

## Source Configuration

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

The engine stores the last cursor value in SQLite and only fetches rows where `cursor_col > last_value` on subsequent runs.

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

Jobs run on cron schedules defined per job. The UI at `http://localhost:5050` shows job history, run status, row counts, and allows manual triggers.

## Web UI

The built-in Flask UI provides:
- Job list with status and last run time
- Manual trigger button per job
- Run history with row counts and error messages
- Offset reset (force full reload)
- Notification settings (Slack, email, webhook)

## Requirements

- Python 3.10+
- Dremio 24+ (for SQL sink mode)
- Docker (optional, for containerized deployment)

Install dependencies:
```bash
pip install -r requirements.txt
```

Source-specific packages (installed as needed):
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

## Running Tests

```bash
pytest tests/ -v
```

## License

MIT
