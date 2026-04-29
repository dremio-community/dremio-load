"""
Zendesk Support load source.

Reads Zendesk Support objects via the Zendesk REST API using Basic Auth
(email/token) or API token.

Config keys under connection:
  subdomain   Your Zendesk subdomain (e.g. 'acme' for acme.zendesk.com) (required)
  email       Agent email address (required for email+token auth)
  token       API token generated in Admin → Apps & Integrations → Zendesk API (required)

Tables:
  tickets, users, organizations, groups, ticket_metrics, satisfaction_ratings

Incremental mode:
  Uses Zendesk's Incremental Export API for tickets and users (most efficient).
  Other objects use updated_at cursor filtering.

  cursor_col = 'updated_at' for most objects, 'end_time' for incremental exports.
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

_SCHEMAS: Dict[str, List[ColumnSchema]] = {
    "tickets": [
        ColumnSchema(name="id",              data_type="bigint"),
        ColumnSchema(name="subject",         data_type="varchar"),
        ColumnSchema(name="description",     data_type="varchar"),
        ColumnSchema(name="status",          data_type="varchar"),
        ColumnSchema(name="priority",        data_type="varchar"),
        ColumnSchema(name="type",            data_type="varchar"),
        ColumnSchema(name="requester_id",    data_type="bigint"),
        ColumnSchema(name="assignee_id",     data_type="bigint"),
        ColumnSchema(name="organization_id", data_type="bigint"),
        ColumnSchema(name="group_id",        data_type="bigint"),
        ColumnSchema(name="tags",            data_type="varchar"),
        ColumnSchema(name="created_at",      data_type="varchar"),
        ColumnSchema(name="updated_at",      data_type="varchar"),
    ],
    "users": [
        ColumnSchema(name="id",              data_type="bigint"),
        ColumnSchema(name="name",            data_type="varchar"),
        ColumnSchema(name="email",           data_type="varchar"),
        ColumnSchema(name="role",            data_type="varchar"),
        ColumnSchema(name="active",          data_type="boolean"),
        ColumnSchema(name="organization_id", data_type="bigint"),
        ColumnSchema(name="created_at",      data_type="varchar"),
        ColumnSchema(name="updated_at",      data_type="varchar"),
    ],
    "organizations": [
        ColumnSchema(name="id",           data_type="bigint"),
        ColumnSchema(name="name",         data_type="varchar"),
        ColumnSchema(name="domain_names", data_type="varchar"),
        ColumnSchema(name="group_id",     data_type="bigint"),
        ColumnSchema(name="created_at",   data_type="varchar"),
        ColumnSchema(name="updated_at",   data_type="varchar"),
    ],
    "groups": [
        ColumnSchema(name="id",          data_type="bigint"),
        ColumnSchema(name="name",        data_type="varchar"),
        ColumnSchema(name="description", data_type="varchar"),
        ColumnSchema(name="deleted",     data_type="boolean"),
        ColumnSchema(name="created_at",  data_type="varchar"),
        ColumnSchema(name="updated_at",  data_type="varchar"),
    ],
    "ticket_metrics": [
        ColumnSchema(name="id",                            data_type="bigint"),
        ColumnSchema(name="ticket_id",                     data_type="bigint"),
        ColumnSchema(name="reopens",                       data_type="integer"),
        ColumnSchema(name="replies",                       data_type="integer"),
        ColumnSchema(name="first_reply_time_calendar",     data_type="integer"),
        ColumnSchema(name="full_resolution_time_calendar", data_type="integer"),
        ColumnSchema(name="created_at",                    data_type="varchar"),
        ColumnSchema(name="updated_at",                    data_type="varchar"),
    ],
    "satisfaction_ratings": [
        ColumnSchema(name="id",         data_type="bigint"),
        ColumnSchema(name="ticket_id",  data_type="bigint"),
        ColumnSchema(name="score",      data_type="varchar"),
        ColumnSchema(name="comment",    data_type="varchar"),
        ColumnSchema(name="created_at", data_type="varchar"),
        ColumnSchema(name="updated_at", data_type="varchar"),
    ],
}

_ENDPOINTS = {
    "tickets":             "/api/v2/tickets.json",
    "users":               "/api/v2/users.json",
    "organizations":       "/api/v2/organizations.json",
    "groups":              "/api/v2/groups.json",
    "ticket_metrics":      "/api/v2/ticket_metrics.json",
    "satisfaction_ratings":"/api/v2/satisfaction_ratings.json",
}

_INCREMENTAL_ENDPOINTS = {
    "tickets": "/api/v2/incremental/tickets/cursor.json",
    "users":   "/api/v2/incremental/users/cursor.json",
}

_RESULT_KEYS = {
    "tickets": "tickets", "users": "users", "organizations": "organizations",
    "groups": "groups", "ticket_metrics": "ticket_metrics",
    "satisfaction_ratings": "satisfaction_ratings",
}


def _flatten(obj: dict) -> dict:
    """Flatten nested dicts/lists to strings."""
    return {
        k: (", ".join(str(x) for x in v) if isinstance(v, list)
            else str(v) if isinstance(v, dict)
            else v)
        for k, v in obj.items()
    }


class ZendeskSource(LoadSource):

    def __init__(self, name: str, cfg: Dict[str, Any]):
        super().__init__(name, cfg)
        conn = cfg.get("connection", {})
        self._subdomain = conn.get("subdomain", "")
        self._email     = conn.get("email", "")
        self._token     = conn.get("token", "")
        self._base      = f"https://{self._subdomain}.zendesk.com"
        self._session   = None

    def connect(self):
        self._session = requests.Session()
        self._session.auth = (f"{self._email}/token", self._token)
        self._session.headers["Content-Type"] = "application/json"
        # Verify credentials
        r = self._session.get(f"{self._base}/api/v2/users/me.json", timeout=10)
        r.raise_for_status()
        logger.info("[zendesk] Connected to %s.zendesk.com as %s",
                    self._subdomain, r.json().get("user", {}).get("email"))

    def close(self):
        if self._session:
            self._session.close()

    def _get(self, path: str, params: dict = None) -> dict:
        url = path if path.startswith("http") else f"{self._base}{path}"
        r = self._session.get(url, params=params or {}, timeout=30)
        if r.status_code == 429:
            retry = int(r.headers.get("Retry-After", 60))
            logger.warning("[zendesk] Rate limited — sleeping %ds", retry)
            time.sleep(retry)
            r = self._session.get(url, params=params or {}, timeout=30)
        r.raise_for_status()
        return r.json()

    def get_schema(self, table: str) -> List[ColumnSchema]:
        return _SCHEMAS.get(table, [])

    def _paginate(self, endpoint: str, result_key: str) -> List[dict]:
        rows = []
        url = endpoint
        while url:
            data = self._get(url)
            for obj in data.get(result_key, []):
                rows.append(_flatten(obj))
            url = data.get("next_page")
        return rows

    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        schema   = self.get_schema(table)
        endpoint = _ENDPOINTS.get(table, f"/api/v2/{table}.json")
        key      = _RESULT_KEYS.get(table, table)

        rows = self._paginate(endpoint, key)
        logger.info("[%s/%s] Snapshot — %d records", self.name, table, len(rows))

        for row in rows:
            yield ChangeEvent(
                op=Operation.SNAPSHOT,
                source_name=self.name, source_table=table,
                before=None, after=row, schema=schema,
                timestamp=datetime.now(timezone.utc),
                offset=row.get("updated_at"),
            )

    def incremental_snapshot(
        self, table: str, cursor_col: str, start_after: Any, chunk_size: int
    ) -> Generator[ChangeEvent, None, None]:
        schema = self.get_schema(table)

        # Use Zendesk Incremental Export for tickets/users (much more efficient)
        if table in _INCREMENTAL_ENDPOINTS and start_after:
            try:
                start_ts = int(datetime.fromisoformat(
                    str(start_after).replace("Z", "+00:00")
                ).timestamp())
            except Exception:
                start_ts = 0

            rows = []
            cursor = None
            endpoint = _INCREMENTAL_ENDPOINTS[table]
            while True:
                params = {"cursor": cursor} if cursor else {"start_time": start_ts}
                data = self._get(endpoint, params=params)
                key = "tickets" if "tickets" in data else "users"
                for obj in data.get(key, []):
                    rows.append(_flatten(obj))
                if data.get("end_of_stream"):
                    break
                cursor = data.get("after_cursor")
                if not cursor:
                    break
        else:
            # Fallback: full paginate (small tables like groups/orgs)
            endpoint = _ENDPOINTS.get(table, f"/api/v2/{table}.json")
            key      = _RESULT_KEYS.get(table, table)
            all_rows = self._paginate(endpoint, key)
            if start_after:
                rows = [r for r in all_rows if (r.get("updated_at") or "") > str(start_after)]
            else:
                rows = all_rows

        logger.info("[%s/%s] Incremental — %d records since %s",
                    self.name, table, len(rows), start_after)

        count = 0
        for row in rows:
            yield ChangeEvent(
                op=Operation.SNAPSHOT,
                source_name=self.name, source_table=table,
                before=None, after=row, schema=schema,
                timestamp=datetime.now(timezone.utc),
                offset=row.get("updated_at"),
            )
            count += 1
            if count >= chunk_size:
                return

    def get_cursor_column(self, table: str) -> str:
        return "updated_at"
