"""
Azure Cosmos DB load source.

Reads documents from Cosmos DB containers using the azure-cosmos SDK.
Supports SQL API (Core API) only.

Config keys under connection:
  endpoint          Cosmos DB account endpoint URL (required)
  key               Primary or secondary account key (or use connection_string)
  connection_string Full connection string (alternative to endpoint + key)
  database          Database name (required)

Table name = container name.

Incremental mode:
  cursor_col is "_ts" (Unix timestamp integer, always present on every document).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional

from core.event import ChangeEvent, ColumnSchema, Operation
from sources.base import LoadSource

logger = logging.getLogger(__name__)


class CosmosDBSource(LoadSource):

    def __init__(self, name: str, cfg: Dict[str, Any]):
        super().__init__(name, cfg)
        conn = cfg.get("connection", {})
        self._endpoint   = conn.get("endpoint", "")
        self._key        = conn.get("key", "")
        self._conn_str   = conn.get("connection_string", "")
        self._database   = conn.get("database", "")
        self._client     = None

    def connect(self):
        try:
            from azure.cosmos import CosmosClient
        except ImportError:
            raise SystemExit("azure-cosmos required: pip install azure-cosmos")

        if self._conn_str:
            self._client = CosmosClient.from_connection_string(self._conn_str)
        else:
            self._client = CosmosClient(self._endpoint, credential=self._key)
        logger.info("[cosmosdb] Connected to %s / %s", self._endpoint or "connection_string", self._database)

    def close(self):
        self._client = None

    def _container_client(self, container: str):
        db = self._client.get_database_client(self._database)
        return db.get_container_client(container)

    def get_schema(self, table: str) -> List[ColumnSchema]:
        cc = self._container_client(table)
        items = list(cc.query_items("SELECT * FROM c OFFSET 0 LIMIT 50", enable_cross_partition_query=True))
        if not items:
            return []
        cols: Dict[str, str] = {}
        for item in items:
            for k, v in item.items():
                if k not in cols:
                    if isinstance(v, bool):
                        cols[k] = "boolean"
                    elif isinstance(v, int):
                        cols[k] = "bigint"
                    elif isinstance(v, float):
                        cols[k] = "double"
                    else:
                        cols[k] = "varchar"
        return [ColumnSchema(name=k, data_type=v) for k, v in cols.items()]

    def _flatten(self, doc: dict) -> dict:
        return {k: (str(v) if isinstance(v, (dict, list)) else v) for k, v in doc.items()}

    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        schema = self.get_schema(table)
        cc     = self._container_client(table)
        query  = self._table_cfg(table).get("query", "SELECT * FROM c ORDER BY c._ts ASC")
        logger.info("[%s/%s] Snapshot query: %s", self.name, table, query)

        count = 0
        for item in cc.query_items(query, enable_cross_partition_query=True):
            row = self._flatten(item)
            yield ChangeEvent(
                op=Operation.SNAPSHOT,
                source_name=self.name, source_table=table,
                before=None, after=row, schema=schema,
                timestamp=datetime.now(timezone.utc),
                offset=str(item.get("_ts", "")),
            )
            count += 1
        logger.info("[%s/%s] Snapshot — %d documents", self.name, table, count)

    def incremental_snapshot(
        self, table: str, cursor_col: str, start_after: Any, chunk_size: int
    ) -> Generator[ChangeEvent, None, None]:
        schema = self.get_schema(table)
        cc     = self._container_client(table)
        table_cfg = self._table_cfg(table)

        where = ""
        if start_after:
            try:
                ts = int(float(str(start_after)))
                where = f" WHERE c._ts > {ts}"
            except ValueError:
                pass

        query = table_cfg.get("query") or f"SELECT * FROM c{where} ORDER BY c._ts ASC"
        logger.info("[%s/%s] Incremental query: %s", self.name, table, query)

        count = 0
        for item in cc.query_items(query, enable_cross_partition_query=True):
            row = self._flatten(item)
            yield ChangeEvent(
                op=Operation.SNAPSHOT,
                source_name=self.name, source_table=table,
                before=None, after=row, schema=schema,
                timestamp=datetime.now(timezone.utc),
                offset=str(item.get("_ts", "")),
            )
            count += 1
            if count >= chunk_size:
                return

    def get_cursor_column(self, table: str) -> str:
        return "_ts"

    def _table_cfg(self, table: str) -> Dict:
        return self.cfg.get("tables_config", {}).get(table, {})
