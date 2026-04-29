"""
Oracle load source — snapshot via python-oracledb (thin mode, no client needed).

Config keys under connection:
  host         Oracle host
  port         Port (default: 1521)
  service_name Oracle service name (e.g. ORCL, XEPDB1)
  user         Username
  password     Password
"""
from __future__ import annotations

import logging
from typing import Generator, List, Optional

from sources.base import ChangeEvent, ColumnSchema, LoadSource

logger = logging.getLogger(__name__)


class OracleSource(LoadSource):
    def __init__(self, job_id: str, cfg: dict):
        self._job_id = job_id
        self._cfg = cfg
        self._conn_cfg = cfg.get("connection", {})
        self._conn = None

    def connect(self):
        import oracledb
        oracledb.init_oracle_client()  # thin mode fallback if no client
        c = self._conn_cfg
        dsn = f"{c['host']}:{c.get('port', 1521)}/{c['service_name']}"
        self._conn = oracledb.connect(user=c["user"], password=c["password"], dsn=dsn)
        logger.info("[oracle] Connected to %s", dsn)

    def close(self):
        if self._conn:
            self._conn.close()

    def get_schema(self, table: str) -> List[ColumnSchema]:
        cur = self._conn.cursor()
        try:
            schema, tname = (table.upper().split(".", 1) + [None])[:2]
            if tname is None:
                tname = schema
                schema = self._conn.username.upper()
            cur.execute(
                "SELECT COLUMN_NAME, DATA_TYPE FROM ALL_TAB_COLUMNS "
                "WHERE OWNER=:1 AND TABLE_NAME=:2 ORDER BY COLUMN_ID",
                (schema, tname)
            )
            return [ColumnSchema(name=r[0], dtype=r[1]) for r in cur.fetchall()]
        finally:
            cur.close()

    def _fetch(self, sql: str, params=None) -> List[dict]:
        cur = self._conn.cursor()
        try:
            cur.execute(sql, params or [])
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        finally:
            cur.close()

    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        for row in self._fetch(f"SELECT * FROM {table}"):
            yield ChangeEvent(op="insert", table=table, after=row)

    def incremental_snapshot(self, table: str, cursor_col: str, start_after,
                             chunk_size: int = 10_000) -> Generator[ChangeEvent, None, None]:
        offset = 0
        while True:
            rows = self._fetch(
                f"SELECT * FROM {table} WHERE {cursor_col} > :1 "
                f"ORDER BY {cursor_col} OFFSET :2 ROWS FETCH NEXT :3 ROWS ONLY",
                [start_after, offset, chunk_size]
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
