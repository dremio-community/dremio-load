"""
Google Cloud Spanner load source.

Reads rows from Cloud Spanner using the google-cloud-spanner SDK.

Config keys under connection:
  project         GCP project ID (required)
  instance        Spanner instance ID (required)
  database        Spanner database ID (required)
  credentials_file  Path to service account JSON key file
                    (omit for Application Default Credentials / Workload Identity)
  emulator_host   Set to "localhost:9010" to use the Spanner emulator

Table name = Spanner table name.

Incremental mode:
  Requires a TIMESTAMP column on the table. Configure per table:

    tables_config:
      Orders:
        cursor_column: UpdatedAt
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional

from core.event import ChangeEvent, ColumnSchema, Operation
from sources.base import LoadSource

logger = logging.getLogger(__name__)

_SPANNER_TYPE_MAP = {
    "INT64": "bigint", "FLOAT64": "double", "FLOAT32": "float",
    "BOOL": "boolean", "STRING": "varchar", "BYTES": "varbinary",
    "DATE": "date", "TIMESTAMP": "timestamp",
    "NUMERIC": "decimal", "JSON": "varchar",
    "ARRAY": "varchar",
}


class SpannerSource(LoadSource):

    def __init__(self, name: str, cfg: Dict[str, Any]):
        super().__init__(name, cfg)
        conn = cfg.get("connection", {})
        self._project     = conn.get("project", "")
        self._instance_id = conn.get("instance", "")
        self._database_id = conn.get("database", "")
        self._cred_file   = conn.get("credentials_file")
        self._emulator    = conn.get("emulator_host")
        self._db          = None

    def connect(self):
        try:
            from google.cloud import spanner
        except ImportError:
            raise SystemExit("google-cloud-spanner required: pip install google-cloud-spanner")

        import os
        if self._emulator:
            os.environ.setdefault("SPANNER_EMULATOR_HOST", self._emulator)

        if self._cred_file:
            from google.oauth2 import service_account
            creds = service_account.Credentials.from_service_account_file(self._cred_file)
            client = spanner.Client(project=self._project, credentials=creds)
        else:
            client = spanner.Client(project=self._project)

        instance = client.instance(self._instance_id)
        self._db = instance.database(self._database_id)
        logger.info("[spanner] Connected to %s/%s/%s%s",
                    self._project, self._instance_id, self._database_id,
                    f" (emulator: {self._emulator})" if self._emulator else "")

    def close(self):
        self._db = None

    def get_schema(self, table: str) -> List[ColumnSchema]:
        try:
            sql = (
                "SELECT c.COLUMN_NAME, c.SPANNER_TYPE "
                "FROM INFORMATION_SCHEMA.COLUMNS c "
                f"WHERE c.TABLE_NAME = '{table}' "
                "ORDER BY c.ORDINAL_POSITION"
            )
            with self._db.snapshot() as snap:
                rows = list(snap.execute_sql(sql))
            cols = []
            for col_name, spanner_type in rows:
                base = spanner_type.split("(")[0].split("<")[0].strip()
                dt = _SPANNER_TYPE_MAP.get(base, "varchar")
                cols.append(ColumnSchema(name=col_name, data_type=dt))
            return cols
        except Exception:
            return []

    def _rows_to_dicts(self, result) -> List[Dict]:
        # Materialize streamed result before accessing .fields (metadata populates on consume)
        raw_rows = list(result)
        fields = [f.name for f in result.fields]
        rows = []
        for row in raw_rows:
            d = {}
            for i, v in enumerate(row):
                if hasattr(v, "isoformat"):
                    v = v.isoformat()
                elif isinstance(v, bytes):
                    v = v.hex()
                d[fields[i]] = v
            rows.append(d)
        return rows

    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        schema    = self.get_schema(table)
        table_cfg = self._table_cfg(table)
        cursor_col = table_cfg.get("cursor_column")
        order_by   = f" ORDER BY {cursor_col} ASC" if cursor_col else ""
        sql        = f"SELECT * FROM `{table}`{order_by}"

        logger.info("[%s/%s] Snapshot: %s", self.name, table, sql)
        with self._db.snapshot() as snap:
            result = snap.execute_sql(sql)
            rows = self._rows_to_dicts(result)

        logger.info("[%s/%s] Snapshot — %d rows", self.name, table, len(rows))
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
        schema    = self.get_schema(table)
        table_cfg = self._table_cfg(table)
        col       = table_cfg.get("cursor_column", cursor_col)

        where = ""
        if start_after and col:
            where = f" WHERE `{col}` > TIMESTAMP '{start_after}'"

        sql = f"SELECT * FROM `{table}`{where} ORDER BY `{col}` ASC LIMIT {chunk_size}"
        logger.info("[%s/%s] Incremental: %s", self.name, table, sql)

        with self._db.snapshot() as snap:
            result = snap.execute_sql(sql)
            rows = self._rows_to_dicts(result)

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
        return self._table_cfg(table).get("cursor_column", "UpdatedAt")

    def _table_cfg(self, table: str) -> Dict:
        return self.cfg.get("tables_config", {}).get(table, {})
