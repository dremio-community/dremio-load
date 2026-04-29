"""
ClickHouse load source.

Reads ClickHouse tables using clickhouse-connect (the official Python client).
Supports full snapshot and incremental loads using any date/datetime/integer column.

Config keys under connection:
  host      ClickHouse host (default: localhost)
  port      HTTP interface port (default: 8123)
  username  ClickHouse user (default: default)
  password  ClickHouse password (default: "")
  database  Default database (default: default)
  secure    true/false — use HTTPS (default: false)
  verify    true/false — verify SSL cert (default: true)
  compress  true/false — enable LZ4 compression (default: true)

Per-table config:
  cursor_column   Column for incremental loads (e.g. updated_at, event_time)
  cursor_type     timestamp | date | integer (default: timestamp)
  query           Override SELECT query (replaces auto-generated SELECT *)
  settings        Dict of ClickHouse query settings, e.g. {"max_threads": 4}
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional

from core.event import ChangeEvent, ColumnSchema, Operation
from sources.base import LoadSource

logger = logging.getLogger(__name__)

_CH_TYPE_MAP = {
    # Integer types
    "Int8": "integer", "Int16": "integer", "Int32": "integer", "Int64": "bigint",
    "Int128": "bigint", "Int256": "bigint",
    "UInt8": "integer", "UInt16": "integer", "UInt32": "bigint", "UInt64": "bigint",
    "UInt128": "bigint", "UInt256": "bigint",
    # Float
    "Float32": "float", "Float64": "double",
    # Decimal
    "Decimal": "decimal", "Decimal32": "decimal", "Decimal64": "decimal", "Decimal128": "decimal",
    # String
    "String": "varchar", "FixedString": "varchar", "UUID": "varchar",
    "Enum8": "varchar", "Enum16": "varchar", "IPv4": "varchar", "IPv6": "varchar",
    # Boolean
    "Bool": "boolean",
    # Date/Time
    "Date": "date", "Date32": "date",
    "DateTime": "timestamp", "DateTime64": "timestamp",
    # Collections → varchar (serialized)
    "Array": "varchar", "Tuple": "varchar", "Map": "varchar",
    "Nested": "varchar", "JSON": "varchar", "Object": "varchar",
    "LowCardinality": "varchar",
    # Binary
    "Nothing": "varchar", "Nullable": "varchar",
}


def _ch_base_type(type_str: str) -> str:
    """Extract base ClickHouse type, stripping Nullable(...) and parameters."""
    s = type_str.strip()
    if s.startswith("Nullable(") and s.endswith(")"):
        s = s[9:-1].strip()
    if s.startswith("LowCardinality(") and s.endswith(")"):
        s = s[15:-1].strip()
    return s.split("(")[0].strip()


class ClickHouseSource(LoadSource):

    def __init__(self, name: str, cfg: Dict[str, Any]):
        super().__init__(name, cfg)
        conn = cfg.get("connection", {})
        self._host     = conn.get("host", "localhost")
        self._port     = int(conn.get("port", 8123))
        self._username = conn.get("username", "default")
        self._password = conn.get("password", "")
        self._database = conn.get("database", "default")
        self._secure   = str(conn.get("secure", "false")).lower() == "true"
        self._verify   = str(conn.get("verify", "true")).lower() != "false"
        self._compress = str(conn.get("compress", "true")).lower() != "false"
        self._client   = None

    def connect(self):
        try:
            import clickhouse_connect
        except ImportError:
            raise SystemExit("clickhouse-connect required: pip install clickhouse-connect")

        self._client = clickhouse_connect.get_client(
            host=self._host,
            port=self._port,
            username=self._username,
            password=self._password,
            database=self._database,
            secure=self._secure,
            verify=self._verify,
            compress=self._compress,
        )
        version = self._client.server_version
        logger.info("[clickhouse] Connected to %s:%d (version %s) database=%s",
                    self._host, self._port, version, self._database)

    def close(self):
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass

    def get_schema(self, table: str) -> List[ColumnSchema]:
        try:
            db, tbl = self._resolve_table(table)
            result = self._client.query(
                f"DESCRIBE TABLE `{db}`.`{tbl}`"
            )
            cols = []
            for row in result.named_results():
                base = _ch_base_type(row["type"])
                dt = _CH_TYPE_MAP.get(base, "varchar")
                cols.append(ColumnSchema(name=row["name"], data_type=dt))
            return cols
        except Exception:
            return []

    def _resolve_table(self, table: str):
        if "." in table:
            db, tbl = table.split(".", 1)
        else:
            db, tbl = self._database, table
        return db, tbl

    def _query_rows(self, sql: str, settings: dict = None) -> List[Dict]:
        result = self._client.query(sql, settings=settings)
        return list(result.named_results())

    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        schema    = self.get_schema(table)
        table_cfg = self._table_cfg(table)
        db, tbl   = self._resolve_table(table)
        cursor_col = table_cfg.get("cursor_column")
        order_by   = f" ORDER BY `{cursor_col}` ASC" if cursor_col else ""
        sql = table_cfg.get("query") or f"SELECT * FROM `{db}`.`{tbl}`{order_by}"
        settings = table_cfg.get("settings")

        logger.info("[%s/%s] Snapshot: %s", self.name, table, sql)
        rows = self._query_rows(sql, settings)
        logger.info("[%s/%s] Snapshot — %d rows", self.name, table, len(rows))

        for row in rows:
            clean = {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in row.items()}
            yield ChangeEvent(
                op=Operation.SNAPSHOT,
                source_name=self.name, source_table=table,
                before=None, after=clean, schema=schema,
                timestamp=datetime.now(timezone.utc),
                offset=str(clean.get(cursor_col, "")) if cursor_col else None,
            )

    def incremental_snapshot(
        self, table: str, cursor_col: str, start_after: Any, chunk_size: int
    ) -> Generator[ChangeEvent, None, None]:
        schema    = self.get_schema(table)
        table_cfg = self._table_cfg(table)
        db, tbl   = self._resolve_table(table)
        col       = table_cfg.get("cursor_column", cursor_col)
        c_type    = table_cfg.get("cursor_type", "timestamp")
        settings  = table_cfg.get("settings")

        where = ""
        if start_after and col:
            if c_type == "integer":
                try:
                    where = f" WHERE `{col}` > {int(float(str(start_after)))}"
                except ValueError:
                    pass
            elif c_type == "date":
                where = f" WHERE `{col}` > toDate('{start_after}')"
            else:
                where = f" WHERE `{col}` > toDateTime('{start_after}')"

        sql = (table_cfg.get("query") or
               f"SELECT * FROM `{db}`.`{tbl}`{where} ORDER BY `{col}` ASC LIMIT {chunk_size}")
        logger.info("[%s/%s] Incremental: %s", self.name, table, sql)

        rows = self._query_rows(sql, settings)
        logger.info("[%s/%s] Incremental — %d rows since %s", self.name, table, len(rows), start_after)

        count = 0
        for row in rows:
            clean = {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in row.items()}
            yield ChangeEvent(
                op=Operation.SNAPSHOT,
                source_name=self.name, source_table=table,
                before=None, after=clean, schema=schema,
                timestamp=datetime.now(timezone.utc),
                offset=str(clean.get(col, "")),
            )
            count += 1
            if count >= chunk_size:
                return

    def get_cursor_column(self, table: str) -> str:
        return self._table_cfg(table).get("cursor_column", "created_at")

    def _table_cfg(self, table: str) -> Dict:
        return self.cfg.get("tables_config", {}).get(table, {})
