"""
Databricks load source — queries via Databricks SQL Connector.

Config keys under connection:
  host       Databricks workspace hostname (e.g. adb-xxx.azuredatabricks.net)
  http_path  SQL warehouse HTTP path (e.g. /sql/1.0/warehouses/xxx)
  token      Personal access token
  catalog    Unity Catalog catalog name
  schema     Schema name
"""
from __future__ import annotations

import logging
from typing import Generator, List, Optional

from sources.base import ChangeEvent, ColumnSchema, LoadSource

logger = logging.getLogger(__name__)


class DatabricksSource(LoadSource):
    def __init__(self, job_id: str, cfg: dict):
        self._job_id = job_id
        self._cfg = cfg
        self._conn_cfg = cfg.get("connection", {})
        self._conn = None

    def connect(self):
        from databricks import sql
        c = self._conn_cfg
        self._conn = sql.connect(
            server_hostname=c["host"],
            http_path=c["http_path"],
            access_token=c["token"],
            catalog=c.get("catalog"),
            schema=c.get("schema"),
        )
        logger.info("[databricks] Connected to %s", c["host"])

    def close(self):
        if self._conn:
            self._conn.close()

    def get_schema(self, table: str) -> List[ColumnSchema]:
        with self._conn.cursor() as cur:
            cur.execute(f"DESCRIBE TABLE {table}")
            return [ColumnSchema(name=r[0], dtype=r[1]) for r in cur.fetchall()
                    if r[0] and not r[0].startswith("#")]

    def _fetch(self, sql: str, params=None) -> List[dict]:
        with self._conn.cursor() as cur:
            cur.execute(sql, params or [])
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        for row in self._fetch(f"SELECT * FROM {table}"):
            yield ChangeEvent(op="insert", table=table, after=row)

    def incremental_snapshot(self, table: str, cursor_col: str, start_after,
                             chunk_size: int = 10_000) -> Generator[ChangeEvent, None, None]:
        offset = 0
        while True:
            rows = self._fetch(
                f"SELECT * FROM {table} WHERE `{cursor_col}` > ? ORDER BY `{cursor_col}` LIMIT {chunk_size} OFFSET {offset}",
                [start_after]
            )
            if not rows:
                break
            for row in rows:
                yield ChangeEvent(op="insert", table=table, after=row,
                                  cursor_value=row.get(cursor_col))
            if len(rows) < chunk_size:
                break
            offset += chunk_size

    def get_cursor_column(self, table: str) -> Optional[str]:
        cols = self.get_schema(table)
        return cols[0].name if cols else None
