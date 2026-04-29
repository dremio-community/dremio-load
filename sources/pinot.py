"""
Apache Pinot load source.

Reads data from Apache Pinot via its SQL broker endpoint using pinotdb.

Config keys under connection:
  host            Pinot broker hostname (default: localhost)
  port            Pinot broker port (default: 8099)
  scheme          http or https (default: http)
  username        (optional)
  password        (optional)
  verify_ssl      true/false (default: true)

Table name = Pinot table name (realtime or offline).

Incremental mode:
  Requires a date/time column on the table. Configure per table:

    tables_config:
      my_table:
        cursor_column: eventTime     # column name
        cursor_type: millis          # millis | seconds | iso
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional

from core.event import ChangeEvent, ColumnSchema, Operation
from sources.base import LoadSource

logger = logging.getLogger(__name__)

_PINOT_TYPE_MAP = {
    "INT": "integer", "LONG": "bigint", "FLOAT": "float", "DOUBLE": "double",
    "BOOLEAN": "boolean", "TIMESTAMP": "timestamp",
    "STRING": "varchar", "JSON": "varchar", "BYTES": "varbinary",
    "INT_ARRAY": "varchar", "LONG_ARRAY": "varchar",
    "FLOAT_ARRAY": "varchar", "DOUBLE_ARRAY": "varchar", "STRING_ARRAY": "varchar",
}


class PinotSource(LoadSource):

    def __init__(self, name: str, cfg: Dict[str, Any]):
        super().__init__(name, cfg)
        conn = cfg.get("connection", {})
        self._host       = conn.get("host", "localhost")
        self._port       = int(conn.get("port", 8099))
        self._scheme     = conn.get("scheme", "http")
        self._username   = conn.get("username")
        self._password   = conn.get("password")
        self._verify_ssl = str(conn.get("verify_ssl", "true")).lower() != "false"
        self._conn       = None

    def connect(self):
        try:
            import pinotdb
        except ImportError:
            raise SystemExit("pinotdb required: pip install pinotdb")

        kwargs: Dict[str, Any] = {
            "host": self._host,
            "port": self._port,
            "scheme": self._scheme,
            "verify_ssl": self._verify_ssl,
        }
        if self._username:
            kwargs["username"] = self._username
        if self._password:
            kwargs["password"] = self._password

        self._conn = pinotdb.connect(**kwargs)
        logger.info("[pinot] Connected to %s://%s:%d", self._scheme, self._host, self._port)

    def close(self):
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass

    def _cursor(self):
        return self._conn.cursor()

    def get_schema(self, table: str) -> List[ColumnSchema]:
        try:
            cur = self._cursor()
            cur.execute(f"SELECT * FROM {table} LIMIT 0")
            desc = cur.description or []
            return [ColumnSchema(name=col[0], data_type="varchar") for col in desc]
        except Exception:
            return []

    def _query_rows(self, sql: str) -> List[Dict]:
        cur = self._cursor()
        cur.execute(sql)
        cols = [d[0] for d in (cur.description or [])]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        schema    = self.get_schema(table)
        table_cfg = self._table_cfg(table)
        limit     = table_cfg.get("limit", 1_000_000)
        order_col = table_cfg.get("cursor_column")
        order_by  = f" ORDER BY {order_col} ASC" if order_col else ""
        sql       = f"SELECT * FROM {table}{order_by} LIMIT {limit}"

        logger.info("[%s/%s] Snapshot: %s", self.name, table, sql)
        rows = self._query_rows(sql)
        logger.info("[%s/%s] Snapshot — %d rows", self.name, table, len(rows))

        for row in rows:
            yield ChangeEvent(
                op=Operation.SNAPSHOT,
                source_name=self.name, source_table=table,
                before=None, after=row, schema=schema,
                timestamp=datetime.now(timezone.utc),
                offset=str(row.get(order_col, "")) if order_col else None,
            )

    def incremental_snapshot(
        self, table: str, cursor_col: str, start_after: Any, chunk_size: int
    ) -> Generator[ChangeEvent, None, None]:
        schema    = self.get_schema(table)
        table_cfg = self._table_cfg(table)
        col       = table_cfg.get("cursor_column", cursor_col)
        c_type    = table_cfg.get("cursor_type", "millis")

        where = ""
        if start_after:
            if c_type == "millis":
                try:
                    where = f" WHERE {col} > {int(float(str(start_after)))}"
                except ValueError:
                    pass
            elif c_type == "seconds":
                try:
                    where = f" WHERE {col} > {int(float(str(start_after)))}"
                except ValueError:
                    pass
            else:
                where = f" WHERE {col} > '{start_after}'"

        sql = f"SELECT * FROM {table}{where} ORDER BY {col} ASC LIMIT {chunk_size}"
        logger.info("[%s/%s] Incremental: %s", self.name, table, sql)
        rows = self._query_rows(sql)
        logger.info("[%s/%s] Incremental — %d rows since %s", self.name, table, len(rows), start_after)

        for row in rows:
            yield ChangeEvent(
                op=Operation.SNAPSHOT,
                source_name=self.name, source_table=table,
                before=None, after=row, schema=schema,
                timestamp=datetime.now(timezone.utc),
                offset=str(row.get(col, "")),
            )

    def get_cursor_column(self, table: str) -> str:
        return self._table_cfg(table).get("cursor_column", "eventTime")

    def _table_cfg(self, table: str) -> Dict:
        return self.cfg.get("tables_config", {}).get(table, {})
