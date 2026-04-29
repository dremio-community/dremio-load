"""
Integration + unit tests for the new dremio-load sources.

Real services used (must be running):
  - LocalStack DynamoDB  : localhost:4566  (tables: customers, orders)
  - Spanner Emulator     : localhost:9010  (project=test-project, instance=test-instance, db=testdb)
  - Delta Lake           : /tmp/delta_test (created here)

Mock-based (no live service needed):
  - Salesforce, Cosmos DB, Pinot, Splunk, Hudi
"""
import os
import sys
import json
import tempfile
import pytest

# Make sure project root is on path
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

os.environ.setdefault("SPANNER_EMULATOR_HOST", "localhost:9010")


# ── helpers ───────────────────────────────────────────────────────────────────

def _make_cfg(source_type: str, connection: dict, tables: list = None, tables_config: dict = None) -> dict:
    return {
        "source_type": source_type,
        "connection": connection,
        "tables": tables or [],
        "tables_config": tables_config or {},
    }


def collect(gen) -> list:
    return list(gen)


# ══════════════════════════════════════════════════════════════════════════════
# DynamoDB — LocalStack
# ══════════════════════════════════════════════════════════════════════════════

DYNAMO_CFG = _make_cfg("dynamodb", {
    "region_name": "us-east-1",
    "aws_access_key_id": "test",
    "aws_secret_access_key": "test",
    "endpoint_url": "http://localhost:4566",
}, tables_config={
    "customers": {"cursor_attribute": "id", "cursor_type": "string"},
    "orders":    {"cursor_attribute": "id", "cursor_type": "string"},
})


class TestDynamoDB:
    @pytest.fixture(autouse=True)
    def source(self):
        from sources.dynamodb import DynamoDBSource
        src = DynamoDBSource("dynamo_test", DYNAMO_CFG)
        src.connect()
        yield src

    def test_snapshot_customers(self, source):
        events = collect(source.snapshot("customers"))
        assert len(events) >= 1
        row = events[0].after
        assert "id" in row or "name" in row
        print(f"  DynamoDB customers: {len(events)} rows, sample={row}")

    def test_snapshot_orders(self, source):
        events = collect(source.snapshot("orders"))
        assert len(events) >= 1
        print(f"  DynamoDB orders: {len(events)} rows")

    def test_schema_inference(self, source):
        cols = source.get_schema("customers")
        assert len(cols) >= 1
        names = [c.name for c in cols]
        print(f"  DynamoDB schema: {names}")

    def test_incremental_no_filter(self, source):
        events = collect(source.incremental_snapshot("customers", "id", None, 1000))
        assert len(events) >= 1

    def test_cursor_column(self, source):
        assert source.get_cursor_column("customers") == "id"


# ══════════════════════════════════════════════════════════════════════════════
# Spanner — Emulator
# ══════════════════════════════════════════════════════════════════════════════

SPANNER_CFG = _make_cfg("spanner", {
    "project": "test-project",
    "instance": "test-instance",
    "database": "testdb",
    "emulator_host": "localhost:9010",
}, tables_config={
    "Employees": {"cursor_column": "UpdatedAt"},
})


def _setup_spanner():
    """Create Employees table and seed data if not present."""
    from google.cloud import spanner
    client  = spanner.Client(project="test-project")
    inst    = client.instance("test-instance")
    db      = inst.database("testdb")

    # Check if table exists
    with db.snapshot() as snap:
        try:
            list(snap.execute_sql("SELECT 1 FROM Employees LIMIT 1"))
            return  # table already exists
        except Exception:
            pass

    # Create table
    op = db.update_ddl([
        """CREATE TABLE Employees (
            Id      INT64 NOT NULL,
            Name    STRING(100),
            Email   STRING(200),
            Salary  FLOAT64,
            UpdatedAt TIMESTAMP
        ) PRIMARY KEY (Id)"""
    ])
    op.result(timeout=30)

    # Insert rows
    with db.batch() as batch:
        batch.insert(
            table="Employees",
            columns=["Id", "Name", "Email", "Salary", "UpdatedAt"],
            values=[
                (1, "Alice Smith",   "alice@example.com",   95000.0, "2026-01-01T00:00:00Z"),
                (2, "Bob Jones",     "bob@example.com",     82000.0, "2026-02-01T00:00:00Z"),
                (3, "Carol White",   "carol@example.com",  110000.0, "2026-03-01T00:00:00Z"),
            ],
        )


class TestSpanner:
    @pytest.fixture(autouse=True)
    def source(self):
        _setup_spanner()
        from sources.spanner import SpannerSource
        src = SpannerSource("spanner_test", SPANNER_CFG)
        src.connect()
        yield src
        src.close()

    def test_schema(self, source):
        cols = source.get_schema("Employees")
        assert len(cols) >= 4
        names = [c.name for c in cols]
        assert "Id" in names
        assert "Name" in names
        print(f"  Spanner schema: {names}")

    def test_snapshot(self, source):
        events = collect(source.snapshot("Employees"))
        assert len(events) == 3
        names = [e.after["Name"] for e in events]
        assert "Alice Smith" in names
        print(f"  Spanner snapshot: {len(events)} rows")

    def test_incremental_no_cursor(self, source):
        events = collect(source.incremental_snapshot("Employees", "UpdatedAt", None, 1000))
        assert len(events) == 3

    def test_incremental_with_cursor(self, source):
        events = collect(source.incremental_snapshot(
            "Employees", "UpdatedAt", "2026-01-15T00:00:00Z", 1000
        ))
        # Should only return rows after 2026-01-15
        assert len(events) == 2
        names = [e.after["Name"] for e in events]
        assert "Alice Smith" not in names
        print(f"  Spanner incremental (after Jan 15): {names}")

    def test_cursor_column(self, source):
        assert source.get_cursor_column("Employees") == "UpdatedAt"


# ══════════════════════════════════════════════════════════════════════════════
# Delta Lake — local filesystem
# ══════════════════════════════════════════════════════════════════════════════

@pytest.fixture(scope="module")
def delta_table_path(tmp_path_factory):
    import pyarrow as pa
    from deltalake.writer import write_deltalake

    path = str(tmp_path_factory.mktemp("delta") / "products")
    data = pa.table({
        "id":       pa.array([1, 2, 3, 4, 5], type=pa.int64()),
        "name":     pa.array(["Widget A", "Widget B", "Gadget X", "Gadget Y", "Doohickey"], type=pa.string()),
        "price":    pa.array([9.99, 19.99, 49.99, 39.99, 5.99], type=pa.float64()),
        "in_stock": pa.array([True, True, False, True, True], type=pa.bool_()),
    })
    write_deltalake(path, data)
    return path


class TestDelta:
    @pytest.fixture(autouse=True)
    def source(self, delta_table_path):
        from sources.delta import DeltaSource
        cfg = {"source_type": "delta", "connection": {"table_uri": os.path.dirname(delta_table_path)}, "tables": [], "tables_config": {}}
        src = DeltaSource("delta_test", cfg)
        src.connect()
        self._table = os.path.basename(delta_table_path)
        yield src

    def test_schema(self, source):
        cols = source.get_schema(self._table)
        assert len(cols) == 4
        col_map = {c.name: c.data_type for c in cols}
        assert col_map["id"] == "bigint"
        assert col_map["price"] == "double"
        assert col_map["in_stock"] == "boolean"
        print(f"  Delta schema: {col_map}")

    def test_snapshot(self, source):
        events = collect(source.snapshot(self._table))
        assert len(events) == 5
        names = [e.after["name"] for e in events]
        assert "Widget A" in names
        print(f"  Delta snapshot: {len(events)} rows")

    def test_incremental_no_prior_version(self, source):
        events = collect(source.incremental_snapshot(self._table, "_delta_version", None, 1000))
        assert len(events) == 5

    def test_incremental_already_at_version(self, source):
        # Version is 0 (first write). Passing 0 as start_after → no new data
        events = collect(source.incremental_snapshot(self._table, "_delta_version", 0, 1000))
        assert len(events) == 0

    def test_cursor_column(self, source):
        assert source.get_cursor_column(self._table) == "_delta_version"


# ══════════════════════════════════════════════════════════════════════════════
# Salesforce — mock
# ══════════════════════════════════════════════════════════════════════════════

class TestSalesforceMock:
    @pytest.fixture(autouse=True)
    def source(self, mocker):
        from sources.salesforce import SalesforceSource
        cfg = _make_cfg("salesforce", {
            "username": "test@example.com",
            "password": "pass",
            "security_token": "tok",
            "domain": "test",
        })
        src = SalesforceSource("sf_test", cfg)

        # Mock simple_salesforce
        mock_sf = mocker.MagicMock()
        mock_account = mocker.MagicMock()
        mock_account.describe.return_value = {"fields": [
            {"name": "Id",           "type": "id"},
            {"name": "Name",         "type": "string"},
            {"name": "Industry",     "type": "string"},
            {"name": "AnnualRevenue","type": "currency"},
            {"name": "IsActive",     "type": "boolean"},
            {"name": "SystemModstamp","type": "datetime"},
        ]}
        mock_sf.Account = mock_account
        mock_sf.query_all.return_value = {"records": [
            {"attributes": {}, "Id": "001A", "Name": "Acme Corp",   "Industry": "Tech",    "AnnualRevenue": 5000000.0, "IsActive": True,  "SystemModstamp": "2026-01-01T00:00:00.000+0000"},
            {"attributes": {}, "Id": "001B", "Name": "Globex Corp", "Industry": "Finance", "AnnualRevenue": 2000000.0, "IsActive": False, "SystemModstamp": "2026-02-01T00:00:00.000+0000"},
        ]}
        src._sf = mock_sf
        yield src

    def test_schema(self, source):
        cols = source.get_schema("Account")
        assert len(cols) == 6
        col_map = {c.name: c.data_type for c in cols}
        assert col_map["AnnualRevenue"] == "double"
        assert col_map["IsActive"] == "boolean"
        print(f"  Salesforce schema: {col_map}")

    def test_snapshot(self, source):
        events = collect(source.snapshot("Account"))
        assert len(events) == 2
        names = [e.after["Name"] for e in events]
        assert "Acme Corp" in names
        print(f"  Salesforce snapshot: {len(events)} records")

    def test_cursor_column(self, source):
        assert source.get_cursor_column("Account") == "SystemModstamp"


# ══════════════════════════════════════════════════════════════════════════════
# Cosmos DB — mock
# ══════════════════════════════════════════════════════════════════════════════

class TestCosmosDBMock:
    @pytest.fixture(autouse=True)
    def source(self, mocker):
        from sources.cosmosdb import CosmosDBSource
        cfg = _make_cfg("cosmosdb", {
            "endpoint": "https://fake.documents.azure.com:443/",
            "key": "fakekey==",
            "database": "testdb",
        })
        src = CosmosDBSource("cosmos_test", cfg)

        mock_client = mocker.MagicMock()
        mock_db     = mocker.MagicMock()
        mock_cc     = mocker.MagicMock()

        DOCS = [
            {"id": "1", "customerId": "c1", "amount": 99.99,  "_ts": 1700000000, "status": "paid"},
            {"id": "2", "customerId": "c2", "amount": 199.99, "_ts": 1700001000, "status": "pending"},
            {"id": "3", "customerId": "c3", "amount": 49.99,  "_ts": 1700002000, "status": "paid"},
        ]
        # Return a fresh iterator each call so schema inference doesn't exhaust the snapshot iterator
        mock_cc.query_items.side_effect = lambda *a, **kw: iter(DOCS)
        mock_db.get_container_client.return_value = mock_cc
        mock_client.get_database_client.return_value = mock_db
        src._client = mock_client
        yield src

    def test_snapshot(self, source):
        events = collect(source.snapshot("orders"))
        assert len(events) == 3
        assert events[0].after["status"] == "paid"
        print(f"  CosmosDB snapshot: {len(events)} docs")

    def test_cursor_column(self, source):
        assert source.get_cursor_column("orders") == "_ts"


# ══════════════════════════════════════════════════════════════════════════════
# Pinot — mock
# ══════════════════════════════════════════════════════════════════════════════

class TestPinotMock:
    @pytest.fixture(autouse=True)
    def source(self, mocker):
        from sources.pinot import PinotSource
        cfg = _make_cfg("pinot", {"host": "localhost", "port": 8099}, tables_config={
            "pageviews": {"cursor_column": "eventTime", "cursor_type": "millis"}
        })
        src = PinotSource("pinot_test", cfg)

        mock_conn   = mocker.MagicMock()
        mock_cursor = mocker.MagicMock()
        mock_cursor.description = [("eventTime",), ("page",), ("userId",), ("views",)]
        mock_cursor.fetchall.return_value = [
            (1700000000000, "/home",    "u1", 5),
            (1700001000000, "/pricing", "u2", 3),
            (1700002000000, "/docs",    "u1", 8),
        ]
        mock_conn.cursor.return_value = mock_cursor
        src._conn = mock_conn
        yield src

    def test_snapshot(self, source):
        events = collect(source.snapshot("pageviews"))
        assert len(events) == 3
        assert events[0].after["page"] == "/home"
        print(f"  Pinot snapshot: {len(events)} rows")

    def test_cursor_column(self, source):
        assert source.get_cursor_column("pageviews") == "eventTime"


# ══════════════════════════════════════════════════════════════════════════════
# Splunk — mock
# ══════════════════════════════════════════════════════════════════════════════

class TestSplunkMock:
    @pytest.fixture(autouse=True)
    def source(self, mocker):
        from sources.splunk import SplunkSource
        cfg = _make_cfg("splunk", {
            "host": "splunk.example.com",
            "token": "faketoken",
        }, tables_config={
            "security_events": {
                "search": "index=security sourcetype=syslog",
                "earliest_time": "-7d",
            }
        })
        src = SplunkSource("splunk_test", cfg)

        EVENTS = [
            {"_time": "2026-04-27T10:00:00.000+00:00", "host": "server1", "source": "syslog", "event": "login failed"},
            {"_time": "2026-04-27T10:05:00.000+00:00", "host": "server2", "source": "syslog", "event": "login success"},
        ]
        src._run_search = mocker.MagicMock(return_value=EVENTS)
        yield src

    def test_snapshot(self, source):
        events = collect(source.snapshot("security_events"))
        assert len(events) == 2
        assert events[0].after["host"] == "server1"
        print(f"  Splunk snapshot: {len(events)} events")

    def test_incremental(self, source):
        events = collect(source.incremental_snapshot(
            "security_events", "_time", "2026-04-27T09:00:00Z", 1000
        ))
        assert len(events) == 2
        source._run_search.assert_called_with(
            "index=security sourcetype=syslog",
            "2026-04-27T09:00:00Z",
            "now",
            1000,
        )

    def test_cursor_column(self, source):
        assert source.get_cursor_column("security_events") == "_time"


# ══════════════════════════════════════════════════════════════════════════════
# Hudi — mock filesystem
# ══════════════════════════════════════════════════════════════════════════════

class TestHudiMock:
    @pytest.fixture(autouse=True)
    def source(self, mocker):
        from sources.hudi import HudiSource
        cfg = _make_cfg("hudi", {"table_uri": "/tmp/hudi_tables"})
        src = HudiSource("hudi_test", cfg)
        src._fs = mocker.MagicMock()
        yield src

    def test_cursor_column(self, source):
        assert source.get_cursor_column("rides") == "_hoodie_commit_time"

    def test_table_path(self, source):
        assert source._table_path("rides") == "/tmp/hudi_tables/rides"

    def test_strip_scheme(self, source):
        assert source._strip_scheme("s3://bucket/path") == "bucket/path"
        assert source._strip_scheme("/local/path") == "/local/path"


# ══════════════════════════════════════════════════════════════════════════════
# HubSpot — mock
# ══════════════════════════════════════════════════════════════════════════════

class TestHubSpotMock:
    @pytest.fixture(autouse=True)
    def source(self, mocker):
        from sources.hubspot import HubSpotSource
        cfg = _make_cfg("hubspot", {"token": "pat-na1-fake"})
        src = HubSpotSource("hs_test", cfg)

        mock_session = mocker.MagicMock()

        def _mock_get(url, **kwargs):
            resp = mocker.MagicMock()
            resp.status_code = 200
            if "properties" in url:
                resp.json.return_value = {"results": [
                    {"name": "firstname",  "type": "string",  "hidden": False},
                    {"name": "lastname",   "type": "string",  "hidden": False},
                    {"name": "email",      "type": "string",  "hidden": False},
                    {"name": "annualrevenue", "type": "number", "hidden": False},
                ]}
            elif "owners" in url:
                resp.json.return_value = {"results": [
                    {"id": "1", "email": "alice@co.com", "firstName": "Alice", "lastName": "Smith",
                     "userId": 101, "createdAt": "2026-01-01", "updatedAt": "2026-04-01", "archived": False},
                ]}
            else:
                resp.json.return_value = {}
            return resp

        def _mock_post(url, **kwargs):
            resp = mocker.MagicMock()
            resp.status_code = 200
            resp.json.return_value = {"results": [
                {"id": "001", "properties": {"firstname": "Bob", "lastname": "Jones", "email": "bob@co.com", "annualrevenue": "50000"}},
                {"id": "002", "properties": {"firstname": "Carol", "lastname": "Lee",  "email": "carol@co.com", "annualrevenue": "80000"}},
            ], "paging": {}}
            return resp

        mock_session.get.side_effect  = _mock_get
        mock_session.post.side_effect = _mock_post
        src._session = mock_session
        yield src

    def test_schema(self, source):
        cols = source.get_schema("contacts")
        names = [c.name for c in cols]
        assert "id" in names
        assert "email" in names
        print(f"  HubSpot schema: {names}")

    def test_snapshot_contacts(self, source):
        events = collect(source.snapshot("contacts"))
        assert len(events) == 2
        assert events[0].after["firstname"] == "Bob"
        print(f"  HubSpot snapshot: {len(events)} contacts")

    def test_snapshot_owners(self, source):
        events = collect(source.snapshot("owners"))
        assert len(events) == 1
        assert events[0].after["email"] == "alice@co.com"

    def test_cursor_column(self, source):
        assert source.get_cursor_column("contacts") == "hs_lastmodifieddate"
        assert source.get_cursor_column("owners") == "updatedAt"


# ══════════════════════════════════════════════════════════════════════════════
# Zendesk — mock
# ══════════════════════════════════════════════════════════════════════════════

class TestZendeskMock:
    @pytest.fixture(autouse=True)
    def source(self, mocker):
        from sources.zendesk import ZendeskSource
        cfg = _make_cfg("zendesk", {
            "subdomain": "testco", "email": "admin@testco.com", "token": "faketoken"
        })
        src = ZendeskSource("zd_test", cfg)

        TICKETS = [
            {"id": 1, "subject": "Login issue",   "status": "open",   "priority": "high",
             "requester_id": 101, "assignee_id": 202, "organization_id": 1, "group_id": 1,
             "tags": ["urgent"], "created_at": "2026-04-01T10:00:00Z", "updated_at": "2026-04-27T10:00:00Z"},
            {"id": 2, "subject": "Billing query", "status": "solved", "priority": "normal",
             "requester_id": 102, "assignee_id": 203, "organization_id": 1, "group_id": 2,
             "tags": [], "created_at": "2026-04-05T09:00:00Z", "updated_at": "2026-04-26T15:00:00Z"},
        ]

        def _mock_get(url, **kwargs):
            resp = mocker.MagicMock()
            resp.status_code = 200
            resp.json.return_value = {"tickets": TICKETS, "next_page": None}
            resp.raise_for_status.return_value = None
            return resp

        mock_session = mocker.MagicMock()
        mock_session.get.side_effect = _mock_get
        src._session = mock_session
        yield src

    def test_schema(self, source):
        cols = source.get_schema("tickets")
        names = [c.name for c in cols]
        assert "id" in names and "subject" in names and "status" in names
        print(f"  Zendesk schema: {names}")

    def test_snapshot(self, source):
        events = collect(source.snapshot("tickets"))
        assert len(events) == 2
        assert events[0].after["subject"] == "Login issue"
        print(f"  Zendesk snapshot: {len(events)} tickets")

    def test_cursor_column(self, source):
        assert source.get_cursor_column("tickets") == "updated_at"


# ══════════════════════════════════════════════════════════════════════════════
# ClickHouse — live container (localhost:8123)
# ══════════════════════════════════════════════════════════════════════════════

CLICKHOUSE_CFG = _make_cfg("clickhouse", {
    "host": "localhost",
    "port": 8123,
    "username": "default",
    "password": "",
    "database": "default",
}, tables_config={
    "products": {"cursor_column": "updated_at", "cursor_type": "timestamp"},
})


def _setup_clickhouse():
    import clickhouse_connect
    client = clickhouse_connect.get_client(host="localhost", port=8123, username="default", password="")
    client.command("DROP TABLE IF EXISTS products")
    client.command("""
        CREATE TABLE products (
            id          UInt32,
            name        String,
            price       Float64,
            in_stock    Bool,
            updated_at  DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY id
    """)
    client.command("""
        INSERT INTO products (id, name, price, in_stock) VALUES
        (1, 'Widget A',  9.99,  true),
        (2, 'Widget B',  19.99, true),
        (3, 'Gadget X',  49.99, false),
        (4, 'Gadget Y',  39.99, true),
        (5, 'Doohickey', 5.99,  true)
    """)
    client.close()


@pytest.mark.skipif(
    not __import__("socket").create_connection(("localhost", 8123), timeout=1) if False else False,
    reason="ClickHouse not available"
)
class TestClickHouse:
    @pytest.fixture(autouse=True)
    def source(self):
        _setup_clickhouse()
        from sources.clickhouse import ClickHouseSource
        src = ClickHouseSource("ch_test", CLICKHOUSE_CFG)
        src.connect()
        yield src
        src.close()

    def test_schema(self, source):
        cols = source.get_schema("products")
        assert len(cols) >= 4
        col_map = {c.name: c.data_type for c in cols}
        assert col_map["id"] == "bigint"
        assert col_map["price"] == "double"
        assert col_map["in_stock"] == "boolean"
        print(f"  ClickHouse schema: {col_map}")

    def test_snapshot(self, source):
        events = collect(source.snapshot("products"))
        assert len(events) == 5
        names = [e.after["name"] for e in events]
        assert "Widget A" in names
        print(f"  ClickHouse snapshot: {len(events)} rows")

    def test_incremental_no_cursor(self, source):
        events = collect(source.incremental_snapshot("products", "updated_at", None, 1000))
        assert len(events) == 5

    def test_incremental_with_cursor(self, source):
        events = collect(source.incremental_snapshot(
            "products", "updated_at", "1970-01-01 00:00:00", 1000
        ))
        assert len(events) == 5

    def test_cursor_column(self, source):
        assert source.get_cursor_column("products") == "updated_at"


# ══════════════════════════════════════════════════════════════════════════════
# Cassandra — live container (localhost:9042)
# ══════════════════════════════════════════════════════════════════════════════

CASSANDRA_CFG = _make_cfg("cassandra", {
    "contact_points": "localhost",
    "port": 9042,
    "keyspace": "testks",
}, tables_config={
    "products": {"cursor_column": "created_at", "cursor_type": "timestamp"},
})


def _setup_cassandra():
    from cassandra.cluster import Cluster
    from cassandra.query import SimpleStatement
    import time

    cluster = Cluster(["localhost"], port=9042)
    # Retry connect — Cassandra takes ~60s to fully start
    for attempt in range(30):
        try:
            session = cluster.connect()
            break
        except Exception:
            if attempt == 29:
                raise
            time.sleep(5)

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS testks
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    session.set_keyspace("testks")
    session.execute("DROP TABLE IF EXISTS products")
    session.execute("""
        CREATE TABLE products (
            id          UUID PRIMARY KEY,
            name        text,
            price       double,
            in_stock    boolean,
            created_at  timestamp
        )
    """)
    from datetime import datetime, timezone
    import uuid
    rows = [
        (uuid.uuid4(), "Widget A",  9.99,  True,  datetime(2026, 1, 1, tzinfo=timezone.utc)),
        (uuid.uuid4(), "Widget B",  19.99, True,  datetime(2026, 2, 1, tzinfo=timezone.utc)),
        (uuid.uuid4(), "Gadget X",  49.99, False, datetime(2026, 3, 1, tzinfo=timezone.utc)),
        (uuid.uuid4(), "Gadget Y",  39.99, True,  datetime(2026, 4, 1, tzinfo=timezone.utc)),
        (uuid.uuid4(), "Doohickey", 5.99,  True,  datetime(2026, 4, 15, tzinfo=timezone.utc)),
    ]
    for row in rows:
        session.execute(
            "INSERT INTO products (id, name, price, in_stock, created_at) VALUES (%s, %s, %s, %s, %s)",
            row
        )
    cluster.shutdown()


class TestCassandra:
    @pytest.fixture(autouse=True)
    def source(self):
        _setup_cassandra()
        from sources.cassandra import CassandraSource
        src = CassandraSource("cass_test", CASSANDRA_CFG)
        src.connect()
        yield src
        src.close()

    def test_schema(self, source):
        cols = source.get_schema("testks.products")
        assert len(cols) >= 4
        names = [c.name for c in cols]
        assert "id" in names
        assert "name" in names
        print(f"  Cassandra schema: {names}")

    def test_snapshot(self, source):
        events = collect(source.snapshot("testks.products"))
        assert len(events) == 5
        names = [e.after["name"] for e in events]
        assert "Widget A" in names
        print(f"  Cassandra snapshot: {len(events)} rows")

    def test_incremental_no_cursor(self, source):
        events = collect(source.incremental_snapshot("testks.products", "created_at", None, 1000))
        assert len(events) == 5

    def test_incremental_with_cursor(self, source):
        events = collect(source.incremental_snapshot(
            "testks.products", "created_at", "2026-02-15T00:00:00", 1000
        ))
        # Should return rows after Feb 15: Gadget X (Mar), Gadget Y (Apr), Doohickey (Apr 15)
        assert len(events) == 3

    def test_cursor_column(self, source):
        assert source.get_cursor_column("products") == "created_at"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
