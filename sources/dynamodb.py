"""
Amazon DynamoDB load source.

Reads items from DynamoDB tables using boto3 Scan with pagination.

Config keys under connection:
  region_name            AWS region (default: us-east-1)
  aws_access_key_id      Access key (or use env / IAM role)
  aws_secret_access_key  Secret key
  endpoint_url           For DynamoDB Local: "http://localhost:8000"

Table name = DynamoDB table name.

Incremental mode:
  DynamoDB has no built-in change tracking in Scan, so incremental mode
  requires a "cursor_attribute" (a numeric Unix timestamp or ISO string
  attribute on each item). Configure it per table:

    tables_config:
      my_table:
        cursor_attribute: updated_at    # attribute name to filter on
        cursor_type: number             # 'number' (epoch) or 'string' (ISO)
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional

from core.event import ChangeEvent, ColumnSchema, Operation
from sources.base import LoadSource

logger = logging.getLogger(__name__)


def _py(val):
    """Convert DynamoDB Decimal to float for JSON serialization."""
    from decimal import Decimal
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, dict):
        return {k: _py(v) for k, v in val.items()}
    if isinstance(val, list):
        return [_py(v) for v in val]
    if isinstance(val, set):
        return [_py(v) for v in val]
    return val


class DynamoDBSource(LoadSource):

    def __init__(self, name: str, cfg: Dict[str, Any]):
        super().__init__(name, cfg)
        conn = cfg.get("connection", {})
        self._region     = conn.get("region_name", "us-east-1")
        self._access_key = conn.get("aws_access_key_id")
        self._secret_key = conn.get("aws_secret_access_key")
        self._endpoint   = conn.get("endpoint_url")
        self._dynamo     = None

    def connect(self):
        try:
            import boto3
        except ImportError:
            raise SystemExit("boto3 required: pip install boto3")

        kwargs: Dict[str, Any] = {"region_name": self._region}
        if self._access_key and self._secret_key:
            kwargs["aws_access_key_id"]     = self._access_key
            kwargs["aws_secret_access_key"] = self._secret_key
        if self._endpoint:
            kwargs["endpoint_url"] = self._endpoint

        self._dynamo = boto3.resource("dynamodb", **kwargs)
        logger.info("[dynamodb] Connected to region=%s endpoint=%s",
                    self._region, self._endpoint or "AWS")

    def close(self):
        self._dynamo = None

    def _scan_all(self, table_name: str, filter_expr=None, expr_values=None) -> List[Dict]:
        tbl = self._dynamo.Table(table_name)
        kwargs: Dict[str, Any] = {}
        if filter_expr:
            kwargs["FilterExpression"] = filter_expr
            if expr_values:
                kwargs["ExpressionAttributeValues"] = expr_values
        items = []
        while True:
            resp = tbl.scan(**kwargs)
            items.extend(resp.get("Items", []))
            lek = resp.get("LastEvaluatedKey")
            if not lek:
                break
            kwargs["ExclusiveStartKey"] = lek
        return items

    def get_schema(self, table: str) -> List[ColumnSchema]:
        try:
            items = self._scan_all(table)[:100]
            if not items:
                return []
            cols: Dict[str, str] = {}
            for item in items:
                for k, v in item.items():
                    if k in cols:
                        continue
                    v2 = _py(v)
                    if isinstance(v2, bool):
                        cols[k] = "boolean"
                    elif isinstance(v2, int):
                        cols[k] = "bigint"
                    elif isinstance(v2, float):
                        cols[k] = "double"
                    else:
                        cols[k] = "varchar"
            return [ColumnSchema(name=k, data_type=v) for k, v in cols.items()]
        except Exception:
            return []

    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        schema = self.get_schema(table)
        logger.info("[%s/%s] Snapshot scan", self.name, table)
        items = self._scan_all(table)
        logger.info("[%s/%s] Snapshot — %d items", self.name, table, len(items))
        for item in items:
            row = _py(item)
            yield ChangeEvent(
                op=Operation.SNAPSHOT,
                source_name=self.name, source_table=table,
                before=None, after=row, schema=schema,
                timestamp=datetime.now(timezone.utc),
                offset=None,
            )

    def incremental_snapshot(
        self, table: str, cursor_col: str, start_after: Any, chunk_size: int
    ) -> Generator[ChangeEvent, None, None]:
        from boto3.dynamodb.conditions import Attr
        schema    = self.get_schema(table)
        table_cfg = self._table_cfg(table)
        attr      = table_cfg.get("cursor_attribute", "updated_at")
        c_type    = table_cfg.get("cursor_type", "string")

        filter_expr = None
        if start_after:
            if c_type == "number":
                try:
                    val = float(str(start_after))
                    filter_expr = Attr(attr).gt(val)
                except ValueError:
                    pass
            else:
                filter_expr = Attr(attr).gt(str(start_after))

        items = self._scan_all(table, filter_expr=filter_expr)
        logger.info("[%s/%s] Incremental — %d items (cursor=%s > %s)",
                    self.name, table, len(items), attr, start_after)

        count = 0
        for item in items:
            row = _py(item)
            yield ChangeEvent(
                op=Operation.SNAPSHOT,
                source_name=self.name, source_table=table,
                before=None, after=row, schema=schema,
                timestamp=datetime.now(timezone.utc),
                offset=str(row.get(attr, "")),
            )
            count += 1
            if count >= chunk_size:
                return

    def get_cursor_column(self, table: str) -> str:
        return self._table_cfg(table).get("cursor_attribute", "updated_at")

    def _table_cfg(self, table: str) -> Dict:
        return self.cfg.get("tables_config", {}).get(table, {})
