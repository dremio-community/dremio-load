"""
HubSpot CRM load source.

Reads HubSpot CRM objects via the HubSpot REST API using a Private App token.

Config keys under connection:
  token       HubSpot Private App access token (required)

Tables (CRM object types):
  contacts, companies, deals, tickets, products, line_items,
  calls, emails, meetings, notes, tasks, owners

Per-table config:
  properties    List of property names to fetch (default: all)
  cursor_field  Property to use as incremental cursor (default: hs_lastmodifieddate)
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional

import requests

from core.event import ChangeEvent, ColumnSchema, Operation
from sources.base import LoadSource

logger = logging.getLogger(__name__)

_BASE = "https://api.hubapi.com"

_OWNERS_SCHEMA = [
    ColumnSchema(name="id",         data_type="bigint"),
    ColumnSchema(name="email",      data_type="varchar"),
    ColumnSchema(name="firstName",  data_type="varchar"),
    ColumnSchema(name="lastName",   data_type="varchar"),
    ColumnSchema(name="userId",     data_type="bigint"),
    ColumnSchema(name="createdAt",  data_type="varchar"),
    ColumnSchema(name="updatedAt",  data_type="varchar"),
    ColumnSchema(name="archived",   data_type="boolean"),
]

_HS_TYPE_MAP = {
    "string": "varchar", "enumeration": "varchar", "phone_number": "varchar",
    "html": "varchar", "json": "varchar", "date": "varchar", "datetime": "varchar",
    "number": "double", "currency": "double",
    "bool": "boolean", "checkbox": "boolean",
}


class HubSpotSource(LoadSource):

    def __init__(self, name: str, cfg: Dict[str, Any]):
        super().__init__(name, cfg)
        conn = cfg.get("connection", {})
        self._token   = conn.get("token", "")
        self._session = None

    def connect(self):
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        })
        # Verify token
        r = self._session.get(f"{_BASE}/crm/v3/owners", params={"limit": 1}, timeout=10)
        r.raise_for_status()
        logger.info("[hubspot] Connected to HubSpot API")

    def close(self):
        if self._session:
            self._session.close()

    def _get(self, path: str, params: dict = None) -> dict:
        r = self._session.get(f"{_BASE}{path}", params=params or {}, timeout=30)
        if r.status_code == 429:
            retry = int(r.headers.get("Retry-After", 10))
            logger.warning("[hubspot] Rate limited — sleeping %ds", retry)
            time.sleep(retry)
            r = self._session.get(f"{_BASE}{path}", params=params or {}, timeout=30)
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, body: dict) -> dict:
        r = self._session.post(f"{_BASE}{path}", json=body, timeout=30)
        if r.status_code == 429:
            retry = int(r.headers.get("Retry-After", 10))
            time.sleep(retry)
            r = self._session.post(f"{_BASE}{path}", json=body, timeout=30)
        r.raise_for_status()
        return r.json()

    def _object_properties(self, obj_type: str) -> List[str]:
        table_cfg = self._table_cfg(obj_type)
        if table_cfg.get("properties"):
            return table_cfg["properties"]
        try:
            data = self._get(f"/crm/v3/properties/{obj_type}")
            return [p["name"] for p in data.get("results", []) if not p.get("hidden")]
        except Exception:
            return []

    def get_schema(self, table: str) -> List[ColumnSchema]:
        if table == "owners":
            return _OWNERS_SCHEMA
        try:
            data = self._get(f"/crm/v3/properties/{table}")
            cols = [ColumnSchema(name="id", data_type="varchar")]
            for prop in data.get("results", []):
                if prop.get("hidden"):
                    continue
                dt = _HS_TYPE_MAP.get(prop.get("type", "string"), "varchar")
                cols.append(ColumnSchema(name=prop["name"], data_type=dt))
            return cols
        except Exception:
            return []

    def _flatten_record(self, record: dict) -> dict:
        row = {"id": record.get("id")}
        row.update(record.get("properties", {}))
        return row

    def _fetch_owners(self) -> List[dict]:
        data = self._get("/crm/v3/owners", params={"limit": 500})
        rows = []
        for o in data.get("results", []):
            rows.append({
                "id": o.get("id"), "email": o.get("email"),
                "firstName": o.get("firstName"), "lastName": o.get("lastName"),
                "userId": o.get("userId"), "createdAt": o.get("createdAt"),
                "updatedAt": o.get("updatedAt"), "archived": o.get("archived"),
            })
        return rows

    def _search_all(self, obj_type: str, properties: List[str],
                    filter_groups: list = None, after_ts_ms: int = None) -> List[dict]:
        body: Dict[str, Any] = {"limit": 100, "properties": properties}
        if filter_groups:
            body["filterGroups"] = filter_groups
        rows = []
        after = None
        while True:
            if after:
                body["after"] = after
            data = self._post(f"/crm/v3/objects/{obj_type}/search", body)
            for r in data.get("results", []):
                rows.append(self._flatten_record(r))
            paging = data.get("paging", {}).get("next", {})
            after = paging.get("after")
            if not after:
                break
        return rows

    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        schema = self.get_schema(table)
        if table == "owners":
            rows = self._fetch_owners()
        else:
            props = self._object_properties(table)
            rows = self._search_all(table, props)

        logger.info("[%s/%s] Snapshot — %d records", self.name, table, len(rows))
        for row in rows:
            yield ChangeEvent(
                op=Operation.SNAPSHOT,
                source_name=self.name, source_table=table,
                before=None, after=row, schema=schema,
                timestamp=datetime.now(timezone.utc),
                offset=row.get("hs_lastmodifieddate") or row.get("updatedAt"),
            )

    def incremental_snapshot(
        self, table: str, cursor_col: str, start_after: Any, chunk_size: int
    ) -> Generator[ChangeEvent, None, None]:
        schema    = self.get_schema(table)
        table_cfg = self._table_cfg(table)
        cursor    = table_cfg.get("cursor_field", "hs_lastmodifieddate")

        filter_groups = []
        if start_after and table != "owners":
            filter_groups = [{"filters": [{
                "propertyName": cursor,
                "operator": "GT",
                "value": str(start_after),
            }]}]

        if table == "owners":
            rows = self._fetch_owners()
            if start_after:
                rows = [r for r in rows if (r.get("updatedAt") or "") > str(start_after)]
        else:
            props = self._object_properties(table)
            rows = self._search_all(table, props, filter_groups=filter_groups or None)

        logger.info("[%s/%s] Incremental — %d records since %s",
                    self.name, table, len(rows), start_after)
        count = 0
        for row in rows:
            yield ChangeEvent(
                op=Operation.SNAPSHOT,
                source_name=self.name, source_table=table,
                before=None, after=row, schema=schema,
                timestamp=datetime.now(timezone.utc),
                offset=row.get(cursor) or row.get("updatedAt"),
            )
            count += 1
            if count >= chunk_size:
                return

    def get_cursor_column(self, table: str) -> str:
        if table == "owners":
            return "updatedAt"
        return self._table_cfg(table).get("cursor_field", "hs_lastmodifieddate")

    def _table_cfg(self, table: str) -> Dict:
        return self.cfg.get("tables_config", {}).get(table, {})
