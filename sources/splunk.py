"""
Splunk load source.

Reads Splunk events via the Splunk REST API (splunk-sdk).
Each "table" maps to a saved search name or an inline SPL search string.

Config keys under connection:
  host            Splunk host (default: localhost)
  port            Splunk management port (default: 8089)
  username        Splunk username
  password        Splunk password
  token           Splunk auth token (alternative to username/password)
  scheme          http or https (default: https)
  verify_ssl      true/false (default: true)
  app             Splunk app context (default: search)

Per-table config (table = logical name):
  search          SPL search string, e.g. "index=main sourcetype=syslog"
                  OR a saved search name prefixed with "saved:"
  earliest_time   Splunk time modifier, e.g. "-24h" or "2024-01-01T00:00:00"
  latest_time     Splunk time modifier (default: "now")
  max_count       Max events to return (default: 50000)
  cursor_column   Field to use as incremental cursor (default: _time)
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional

from core.event import ChangeEvent, ColumnSchema, Operation
from sources.base import LoadSource

logger = logging.getLogger(__name__)


class SplunkSource(LoadSource):

    def __init__(self, name: str, cfg: Dict[str, Any]):
        super().__init__(name, cfg)
        conn = cfg.get("connection", {})
        self._host       = conn.get("host", "localhost")
        self._port       = int(conn.get("port", 8089))
        self._username   = conn.get("username", "")
        self._password   = conn.get("password", "")
        self._token      = conn.get("token", "")
        self._scheme     = conn.get("scheme", "https")
        self._verify_ssl = str(conn.get("verify_ssl", "true")).lower() != "false"
        self._app        = conn.get("app", "search")
        self._service    = None

    def connect(self):
        try:
            import splunklib.client as splunk_client
        except ImportError:
            raise SystemExit("splunk-sdk required: pip install splunk-sdk")

        kwargs: Dict[str, Any] = {
            "host": self._host,
            "port": self._port,
            "scheme": self._scheme,
            "app": self._app,
        }
        if self._token:
            kwargs["splunkToken"] = self._token
        else:
            kwargs["username"] = self._username
            kwargs["password"] = self._password

        self._service = splunk_client.connect(**kwargs)
        logger.info("[splunk] Connected to %s://%s:%d", self._scheme, self._host, self._port)

    def close(self):
        self._service = None

    def _run_search(self, spl: str, earliest: str, latest: str, max_count: int) -> List[Dict]:
        import splunklib.results as results_module

        job = self._service.jobs.create(
            f"search {spl}",
            earliest_time=earliest,
            latest_time=latest,
            max_count=max_count,
        )
        while not job.is_done():
            import time; time.sleep(0.5)
            job.refresh()

        rows = []
        for result in results_module.JSONResultsReader(
            job.results(output_mode="json", count=max_count)
        ):
            if isinstance(result, dict):
                rows.append({k: v for k, v in result.items()})
        job.cancel()
        return rows

    def _run_saved_search(self, saved_name: str, kwargs: Dict) -> List[Dict]:
        import splunklib.results as results_module
        saved = self._service.saved_searches[saved_name]
        job = saved.dispatch(**kwargs)
        while not job.is_done():
            import time; time.sleep(0.5)
            job.refresh()
        rows = []
        for result in results_module.JSONResultsReader(
            job.results(output_mode="json")
        ):
            if isinstance(result, dict):
                rows.append(result)
        job.cancel()
        return rows

    def get_schema(self, table: str) -> List[ColumnSchema]:
        table_cfg = self._table_cfg(table)
        search    = table_cfg.get("search", f"index=main | head 10")
        if search.startswith("saved:"):
            return []
        try:
            rows = self._run_search(search, earliest="-1h", latest="now", max_count=10)
            if not rows:
                return []
            cols = list(rows[0].keys())
            return [ColumnSchema(name=c, data_type="varchar") for c in cols]
        except Exception:
            return []

    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        schema    = self.get_schema(table)
        table_cfg = self._table_cfg(table)
        search    = table_cfg.get("search", f"index=main")
        earliest  = table_cfg.get("earliest_time", "-30d")
        latest    = table_cfg.get("latest_time", "now")
        max_count = int(table_cfg.get("max_count", 50_000))

        if search.startswith("saved:"):
            rows = self._run_saved_search(search[6:], {"earliest_time": earliest, "latest_time": latest})
        else:
            rows = self._run_search(search, earliest, latest, max_count)

        logger.info("[%s/%s] Snapshot — %d events", self.name, table, len(rows))
        for row in rows:
            yield ChangeEvent(
                op=Operation.SNAPSHOT,
                source_name=self.name, source_table=table,
                before=None, after=row, schema=schema,
                timestamp=datetime.now(timezone.utc),
                offset=row.get("_time"),
            )

    def incremental_snapshot(
        self, table: str, cursor_col: str, start_after: Any, chunk_size: int
    ) -> Generator[ChangeEvent, None, None]:
        schema    = self.get_schema(table)
        table_cfg = self._table_cfg(table)
        search    = table_cfg.get("search", "index=main")
        latest    = table_cfg.get("latest_time", "now")
        max_count = min(int(table_cfg.get("max_count", 50_000)), chunk_size)

        # Use start_after as earliest_time; Splunk accepts ISO strings natively
        earliest = str(start_after) if start_after else table_cfg.get("earliest_time", "-24h")

        if search.startswith("saved:"):
            rows = self._run_saved_search(search[6:], {"earliest_time": earliest, "latest_time": latest})
        else:
            rows = self._run_search(search, earliest, latest, max_count)

        logger.info("[%s/%s] Incremental — %d events since %s", self.name, table, len(rows), earliest)
        for row in rows:
            yield ChangeEvent(
                op=Operation.SNAPSHOT,
                source_name=self.name, source_table=table,
                before=None, after=row, schema=schema,
                timestamp=datetime.now(timezone.utc),
                offset=row.get("_time"),
            )

    def get_cursor_column(self, table: str) -> str:
        return self._table_cfg(table).get("cursor_column", "_time")

    def _table_cfg(self, table: str) -> Dict:
        return self.cfg.get("tables_config", {}).get(table, {})
