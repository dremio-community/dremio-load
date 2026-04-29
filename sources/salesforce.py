"""
Salesforce REST API load source.

Reads standard and custom Salesforce objects via SOQL using the
simple-salesforce SDK.

Config keys under connection:
  username        Salesforce username (required)
  password        Salesforce password (required)
  security_token  Salesforce security token (required unless using OAuth)
  domain          'login' (prod) or 'test' (sandbox) — default: 'login'
  consumer_key    For OAuth2 client credentials flow (optional)
  consumer_secret For OAuth2 client credentials flow (optional)

Per-table config (table name = Salesforce object API name, e.g. 'Account'):
  fields          List of field API names to query (default: all queryable fields)
  where_clause    Extra SOQL WHERE clause, e.g. "IsDeleted = false"
  cursor_field    Field to use for incremental loads (default: SystemModstamp)
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional

from core.event import ChangeEvent, ColumnSchema, Operation
from sources.base import LoadSource

logger = logging.getLogger(__name__)


class SalesforceSource(LoadSource):

    def __init__(self, name: str, cfg: Dict[str, Any]):
        super().__init__(name, cfg)
        self._sf = None
        conn = cfg.get("connection", {})
        self._username       = conn.get("username", "")
        self._password       = conn.get("password", "")
        self._token          = conn.get("security_token", "")
        self._domain         = conn.get("domain", "login")
        self._consumer_key   = conn.get("consumer_key")
        self._consumer_secret = conn.get("consumer_secret")

    def connect(self):
        try:
            from simple_salesforce import Salesforce
        except ImportError:
            raise SystemExit("simple-salesforce required: pip install simple-salesforce")

        if self._consumer_key and self._consumer_secret:
            self._sf = Salesforce(
                consumer_key=self._consumer_key,
                consumer_secret=self._consumer_secret,
                username=self._username,
                password=self._password,
                domain=self._domain,
            )
        else:
            self._sf = Salesforce(
                username=self._username,
                password=self._password,
                security_token=self._token,
                domain=self._domain,
            )
        logger.info("[salesforce] Connected to org %s", self._domain)

    def close(self):
        self._sf = None

    def _object_fields(self, obj_name: str, table_cfg: Dict) -> List[str]:
        configured = table_cfg.get("fields")
        if configured:
            return configured
        desc = getattr(self._sf, obj_name).describe()
        return [f["name"] for f in desc["fields"] if f.get("type") != "base64"]

    def get_schema(self, table: str) -> List[ColumnSchema]:
        table_cfg = self._table_cfg(table)
        try:
            desc = getattr(self._sf, table).describe()
            sf_type_map = {
                "boolean": "boolean", "int": "integer", "double": "double",
                "currency": "double", "percent": "double",
                "date": "date", "datetime": "timestamp",
                "id": "varchar", "reference": "varchar",
            }
            cols = []
            for f in desc["fields"]:
                if f.get("type") == "base64":
                    continue
                dt = sf_type_map.get(f["type"], "varchar")
                cols.append(ColumnSchema(name=f["name"], data_type=dt))
            return cols
        except Exception:
            return []

    def _query_all(self, soql: str) -> List[Dict]:
        result = self._sf.query_all(soql)
        rows = []
        for record in result.get("records", []):
            row = {k: v for k, v in record.items() if k != "attributes"}
            rows.append(row)
        return rows

    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        table_cfg = self._table_cfg(table)
        schema    = self.get_schema(table)
        fields    = self._object_fields(table, table_cfg)
        where     = table_cfg.get("where_clause", "")
        soql      = f"SELECT {', '.join(fields)} FROM {table}"
        if where:
            soql += f" WHERE {where}"
        soql += " ORDER BY SystemModstamp ASC"

        logger.info("[%s/%s] Snapshot SOQL: %s", self.name, table, soql)
        rows = self._query_all(soql)
        logger.info("[%s/%s] Snapshot — %d records", self.name, table, len(rows))
        for row in rows:
            yield ChangeEvent(
                op=Operation.SNAPSHOT,
                source_name=self.name, source_table=table,
                before=None, after=row, schema=schema,
                timestamp=datetime.now(timezone.utc),
                offset=row.get("SystemModstamp") or row.get("Id"),
            )

    def incremental_snapshot(
        self, table: str, cursor_col: str, start_after: Any, chunk_size: int
    ) -> Generator[ChangeEvent, None, None]:
        table_cfg   = self._table_cfg(table)
        schema      = self.get_schema(table)
        fields      = self._object_fields(table, table_cfg)
        cursor_field = table_cfg.get("cursor_field", "SystemModstamp")
        where_extra  = table_cfg.get("where_clause", "")

        where_parts = []
        if start_after:
            where_parts.append(f"{cursor_field} > {start_after}")
        if where_extra:
            where_parts.append(f"({where_extra})")

        soql = f"SELECT {', '.join(fields)} FROM {table}"
        if where_parts:
            soql += " WHERE " + " AND ".join(where_parts)
        soql += f" ORDER BY {cursor_field} ASC"

        logger.info("[%s/%s] Incremental SOQL: %s", self.name, table, soql)
        rows = self._query_all(soql)
        logger.info("[%s/%s] Incremental — %d records since %s",
                    self.name, table, len(rows), start_after)

        count = 0
        for row in rows:
            yield ChangeEvent(
                op=Operation.SNAPSHOT,
                source_name=self.name, source_table=table,
                before=None, after=row, schema=schema,
                timestamp=datetime.now(timezone.utc),
                offset=str(row.get(cursor_field, "")),
            )
            count += 1
            if count >= chunk_size:
                return

    def get_cursor_column(self, table: str) -> str:
        return self._table_cfg(table).get("cursor_field", "SystemModstamp")

    def _table_cfg(self, table: str) -> Dict:
        return self.cfg.get("tables_config", {}).get(table, {})
