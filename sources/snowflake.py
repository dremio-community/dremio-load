"""
Snowflake load source — bulk snapshot via snowflake-connector-python.

Config keys under connection:
  account    Snowflake account identifier (e.g. myaccount.us-east-1)
  user       Username
  password   Password (or use private_key_file)
  database   Database name
  schema     Schema name
  warehouse  Virtual warehouse
  role       Role (optional)
"""
from __future__ import annotations

import logging
from typing import Generator, List, Optional

from sources.base import ChangeEvent, ColumnSchema, LoadSource

logger = logging.getLogger(__name__)


class SnowflakeSource(LoadSource):
    def __init__(self, job_id: str, cfg: dict):
        self._job_id = job_id
        self._cfg = cfg
        self._conn_cfg = cfg.get("connection", {})
        self._conn = None

    def connect(self):
        import snowflake.connector
        c = self._conn_cfg
        kwargs = dict(
            account=c["account"], user=c["user"], password=c.get("password", ""),
            database=c["database"], schema=c["schema"], warehouse=c.get("warehouse"),
        )
        if c.get("role"):
            kwargs["role"] = c["role"]
        self._conn = snowflake.connector.connect(**kwargs)
        logger.info("[snowflake] Connected to %s / %s", c["account"], c["database"])

    def close(self):
        if self._conn:
            self._conn.close()

    def get_schema(self, table: str) -> List[ColumnSchema]:
        cur = self._conn.cursor()
        try:
            cur.execute(f'DESCRIBE TABLE "{table}"')
            return [ColumnSchema(name=r[0], dtype=r[1]) for r in cur.fetchall()]
        finally:
            cur.close()

    def _fetch(self, sql: str, params=None) -> List[dict]:
        cur = self._conn.cursor()
        try:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        finally:
            cur.close()

    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        rows = self._fetch(f'SELECT * FROM "{table}"')
        for row in rows:
            yield ChangeEvent(op="insert", table=table, after=row)

    def incremental_snapshot(self, table: str, cursor_col: str, start_after,
                             chunk_size: int = 10_000) -> Generator[ChangeEvent, None, None]:
        offset = 0
        while True:
            rows = self._fetch(
                f'SELECT * FROM "{table}" WHERE "{cursor_col}" > %s ORDER BY "{cursor_col}" LIMIT %s OFFSET %s',
                (start_after, chunk_size, offset)
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
        pk = next((c.name for c in cols if "PRIMARY KEY" in (c.dtype or "").upper()), None)
        return pk or (cols[0].name if cols else None)
