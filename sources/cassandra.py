"""
Apache Cassandra load source.

Reads Cassandra tables via the DataStax Python driver (cassandra-driver).
Supports full snapshot and incremental loads using a timestamp/timeuuid
clustering or regular column as the cursor.

Config keys under connection:
  contact_points  Comma-separated hostnames or IPs (default: "localhost")
  port            CQL native port (default: 9042)
  username        Cassandra username (optional)
  password        Cassandra password (optional)
  keyspace        Default keyspace (required)
  local_dc        Local datacenter name for DCAwareRoundRobinPolicy (optional)
  ssl             true/false — enable TLS (default: false)

Table name = <keyspace>.<table_name>  OR just <table_name> (uses connection keyspace).

Incremental mode:
  Requires a timestamp/timeuuid/date column on the table.
  Configure per table:

    tables_config:
      orders:
        cursor_column: created_at
        cursor_type: timestamp    # timestamp | timeuuid | date | bigint
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional, Tuple

from core.event import ChangeEvent, ColumnSchema, Operation
from sources.base import LoadSource

logger = logging.getLogger(__name__)

_CQL_TYPE_MAP = {
    "ascii": "varchar", "text": "varchar", "varchar": "varchar",
    "uuid": "varchar", "timeuuid": "varchar", "inet": "varchar",
    "boolean": "boolean",
    "tinyint": "integer", "smallint": "integer", "int": "integer",
    "bigint": "bigint", "counter": "bigint", "varint": "bigint",
    "float": "float", "double": "double", "decimal": "decimal",
    "date": "date", "time": "bigint",
    "timestamp": "timestamp",
    "blob": "varbinary",
    "list": "varchar", "set": "varchar", "map": "varchar",
    "tuple": "varchar", "udt": "varchar", "frozen": "varchar",
    "duration": "varchar",
}


def _cql_base_type(cql_type_str: str) -> str:
    """Extract base type name from CQL type string like 'list<text>', 'frozen<map<...>>'."""
    s = cql_type_str.lower().split("<")[0].strip()
    return s


class CassandraSource(LoadSource):

    def __init__(self, name: str, cfg: Dict[str, Any]):
        super().__init__(name, cfg)
        conn = cfg.get("connection", {})
        hosts = conn.get("contact_points", "localhost")
        self._contact_points = [h.strip() for h in hosts.split(",")]
        self._port     = int(conn.get("port", 9042))
        self._username = conn.get("username")
        self._password = conn.get("password")
        self._keyspace = conn.get("keyspace", "")
        self._local_dc = conn.get("local_dc")
        self._ssl      = str(conn.get("ssl", "false")).lower() == "true"
        self._session  = None
        self._cluster  = None

    def connect(self):
        try:
            from cassandra.cluster import Cluster
            from cassandra.auth import PlainTextAuthProvider
            from cassandra.policies import DCAwareRoundRobinPolicy
        except ImportError:
            raise SystemExit("cassandra-driver required: pip install cassandra-driver")

        kwargs: Dict[str, Any] = {"port": self._port}
        if self._username and self._password:
            kwargs["auth_provider"] = PlainTextAuthProvider(self._username, self._password)
        if self._local_dc:
            kwargs["load_balancing_policy"] = DCAwareRoundRobinPolicy(local_dc=self._local_dc)
        if self._ssl:
            import ssl as ssl_mod
            kwargs["ssl_context"] = ssl_mod.create_default_context()

        self._cluster = Cluster(self._contact_points, **kwargs)
        self._session = self._cluster.connect(self._keyspace or None)
        logger.info("[cassandra] Connected to %s keyspace=%s",
                    self._contact_points, self._keyspace)

    def close(self):
        try:
            if self._session:
                self._session.shutdown()
            if self._cluster:
                self._cluster.shutdown()
        except Exception:
            pass

    def _resolve_table(self, table: str) -> Tuple[str, str]:
        """Split 'keyspace.table' or just 'table' using connection keyspace."""
        if "." in table:
            ks, tbl = table.split(".", 1)
        else:
            ks, tbl = self._keyspace, table
        return ks, tbl

    def get_schema(self, table: str) -> List[ColumnSchema]:
        ks, tbl = self._resolve_table(table)
        try:
            meta = self._cluster.metadata.keyspaces[ks].tables[tbl]
            cols = []
            for col_name, col_meta in meta.columns.items():
                base = _cql_base_type(str(col_meta.cql_type))
                dt = _CQL_TYPE_MAP.get(base, "varchar")
                cols.append(ColumnSchema(name=col_name, data_type=dt))
            return cols
        except Exception:
            return []

    def _execute(self, cql: str, params=None, fetch_size: int = 1000) -> List[dict]:
        from cassandra.query import SimpleStatement
        stmt = SimpleStatement(cql, fetch_size=fetch_size)
        result = self._session.execute(stmt, params or ())
        rows = []
        for row in result:
            d = {}
            for k, v in row._asdict().items():
                if hasattr(v, "isoformat"):
                    v = v.isoformat()
                elif isinstance(v, (set, frozenset, list)):
                    v = str(list(v))
                elif isinstance(v, dict):
                    v = str(v)
                d[k] = v
            rows.append(d)
        return rows

    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        ks, tbl = self._resolve_table(table)
        schema  = self.get_schema(table)
        table_cfg = self._table_cfg(table)
        cql = table_cfg.get("query") or f'SELECT * FROM "{ks}"."{tbl}"'

        logger.info("[%s/%s] Snapshot: %s", self.name, table, cql)
        rows = self._execute(cql)
        logger.info("[%s/%s] Snapshot — %d rows", self.name, table, len(rows))

        cursor_col = table_cfg.get("cursor_column")
        for row in rows:
            yield ChangeEvent(
                op=Operation.SNAPSHOT,
                source_name=self.name, source_table=table,
                before=None, after=row, schema=schema,
                timestamp=datetime.now(timezone.utc),
                offset=str(row.get(cursor_col, "")) if cursor_col else None,
            )

    def incremental_snapshot(
        self, table: str, cursor_col: str, start_after: Any, chunk_size: int
    ) -> Generator[ChangeEvent, None, None]:
        ks, tbl   = self._resolve_table(table)
        schema    = self.get_schema(table)
        table_cfg = self._table_cfg(table)
        col       = table_cfg.get("cursor_column", cursor_col)
        c_type    = table_cfg.get("cursor_type", "timestamp")

        where = ""
        allow_filtering = ""
        if start_after and col:
            allow_filtering = " ALLOW FILTERING"
            if c_type in ("timestamp", "date"):
                where = f" WHERE \"{col}\" > '{start_after}'"
            elif c_type == "bigint":
                try:
                    where = f" WHERE \"{col}\" > {int(float(str(start_after)))}"
                except ValueError:
                    allow_filtering = ""
            else:
                where = f" WHERE \"{col}\" > '{start_after}'"

        cql = table_cfg.get("query") or f'SELECT * FROM "{ks}"."{tbl}"{where} LIMIT {chunk_size}{allow_filtering}'
        logger.info("[%s/%s] Incremental: %s", self.name, table, cql)

        rows = self._execute(cql)
        logger.info("[%s/%s] Incremental — %d rows since %s", self.name, table, len(rows), start_after)

        count = 0
        for row in rows:
            yield ChangeEvent(
                op=Operation.SNAPSHOT,
                source_name=self.name, source_table=table,
                before=None, after=row, schema=schema,
                timestamp=datetime.now(timezone.utc),
                offset=str(row.get(col, "")),
            )
            count += 1
            if count >= chunk_size:
                return

    def get_cursor_column(self, table: str) -> str:
        return self._table_cfg(table).get("cursor_column", "created_at")

    def _table_cfg(self, table: str) -> Dict:
        return self.cfg.get("tables_config", {}).get(table, {})
