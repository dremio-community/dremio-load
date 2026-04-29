"""
S3 / MinIO load source.

Reads CSV, Parquet, JSON, NDJSON, and Avro files directly from S3 or any
S3-compatible storage (MinIO, Ceph, etc.) using boto3.

Config keys under connection:
  bucket               S3 bucket name (required)
  prefix               Key prefix / folder path (default: "")
  endpoint_url         For MinIO: "http://localhost:9000" (omit for AWS S3)
  aws_access_key_id    Access key (or use env AWS_ACCESS_KEY_ID / IAM role)
  aws_secret_access_key Secret key
  region_name          AWS region (default: us-east-1)
  path_style           true = force path-style URLs (required for MinIO)

Per-table config (table name = key prefix pattern, e.g. "orders/" or "data/*.parquet"):
  file_format          csv | parquet | json | ndjson | avro (default: auto-detect)
  csv_delimiter        CSV field delimiter (default: ",")
  csv_has_header       true/false (default: true)
  max_files_per_run    Cap files per incremental run (default: unlimited)

Incremental mode:
  cursor_col is always "last_modified" for file sources.
  The engine saves the last S3 LastModified timestamp seen; next run only
  reads files newer than that watermark.
"""
from __future__ import annotations

import csv
import io
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional

from core.event import ChangeEvent, ColumnSchema, Operation
from sources.base import LoadSource

logger = logging.getLogger(__name__)

_INFER_LIMIT = 1000  # rows to scan when inferring schema from CSV/JSON


def _infer_type(val: Any) -> str:
    if isinstance(val, bool):   return "boolean"
    if isinstance(val, int):    return "bigint"
    if isinstance(val, float):  return "double"
    return "varchar"


def _infer_schema_from_rows(rows: List[Dict]) -> List[ColumnSchema]:
    if not rows:
        return []
    # Union all keys, pick the widest type seen per column
    type_order = {"varchar": 0, "double": 1, "bigint": 2, "boolean": 3}
    cols: Dict[str, str] = {}
    for row in rows:
        for k, v in row.items():
            t = _infer_type(v)
            if k not in cols or type_order.get(t, 0) < type_order.get(cols[k], 0):
                cols[k] = t
    return [ColumnSchema(name=k, data_type=v) for k, v in cols.items()]


def _detect_format(key: str) -> str:
    key_lower = key.lower()
    if key_lower.endswith(".parquet") or key_lower.endswith(".pq"):
        return "parquet"
    if key_lower.endswith(".avro"):
        return "avro"
    if key_lower.endswith(".ndjson") or key_lower.endswith(".jsonl"):
        return "ndjson"
    if key_lower.endswith(".json"):
        return "json"
    return "csv"


class S3Source(LoadSource):

    def __init__(self, name: str, cfg: Dict[str, Any]):
        super().__init__(name, cfg)
        self._s3        = None
        conn            = cfg.get("connection", {})
        self._bucket    = conn["bucket"]
        self._prefix    = conn.get("prefix", "")
        self._endpoint  = conn.get("endpoint_url") or conn.get("endpoint")
        self._access_key = conn.get("aws_access_key_id")
        self._secret_key = conn.get("aws_secret_access_key")
        self._region    = conn.get("region_name", "us-east-1")
        self._path_style = str(conn.get("path_style", "false")).lower() == "true"

    def connect(self):
        try:
            import boto3
            from botocore.config import Config
        except ImportError:
            raise SystemExit("boto3 required: pip install boto3")

        kwargs: Dict[str, Any] = {"region_name": self._region}
        if self._access_key and self._secret_key:
            kwargs["aws_access_key_id"]     = self._access_key
            kwargs["aws_secret_access_key"] = self._secret_key
        if self._endpoint:
            kwargs["endpoint_url"] = self._endpoint
        if self._path_style:
            kwargs["config"] = Config(s3={"addressing_style": "path"})

        self._s3 = boto3.client("s3", **kwargs)
        # Validate connectivity
        self._s3.head_bucket(Bucket=self._bucket)
        logger.info("Connected to S3/MinIO bucket=%s endpoint=%s",
                    self._bucket, self._endpoint or "AWS")

    def _list_files(self, prefix: str, newer_than: Optional[datetime] = None,
                    max_files: Optional[int] = None) -> List[Dict]:
        """List all objects under prefix, optionally filtered by LastModified."""
        paginator = self._s3.get_paginator("list_objects_v2")
        files = []
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith("/"):
                    continue
                lm = obj["LastModified"]
                if newer_than and lm <= newer_than:
                    continue
                files.append({"key": key, "last_modified": lm, "size": obj["Size"]})
                if max_files and len(files) >= max_files:
                    return files
        return sorted(files, key=lambda x: x["last_modified"])

    def _read_file(self, key: str, table_cfg: Dict) -> List[Dict]:
        """Download and parse a single file, returning a list of row dicts."""
        fmt = table_cfg.get("file_format") or _detect_format(key)
        body = self._s3.get_object(Bucket=self._bucket, Key=key)["Body"].read()

        if fmt == "parquet":
            return self._parse_parquet(body)
        if fmt in ("json", "ndjson", "jsonl"):
            return self._parse_json(body, fmt)
        if fmt == "avro":
            return self._parse_avro(body)
        return self._parse_csv(body, table_cfg)

    def _parse_parquet(self, data: bytes) -> List[Dict]:
        try:
            import pyarrow.parquet as pq
            import pyarrow as pa
            table = pq.read_table(io.BytesIO(data))
            return table.to_pydict()  # type: ignore
        except ImportError:
            raise SystemExit("pyarrow required for Parquet: pip install pyarrow")

    def _parse_json(self, data: bytes, fmt: str) -> List[Dict]:
        text = data.decode("utf-8")
        if fmt in ("ndjson", "jsonl") or "\n" in text.strip():
            rows = []
            for line in text.splitlines():
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
            return rows
        parsed = json.loads(text)
        return parsed if isinstance(parsed, list) else [parsed]

    def _parse_avro(self, data: bytes) -> List[Dict]:
        try:
            import fastavro
            reader = fastavro.reader(io.BytesIO(data))
            return [dict(r) for r in reader]
        except ImportError:
            raise SystemExit("fastavro required for Avro: pip install fastavro")

    def _parse_csv(self, data: bytes, table_cfg: Dict) -> List[Dict]:
        delimiter  = table_cfg.get("csv_delimiter", ",")
        has_header = str(table_cfg.get("csv_has_header", "true")).lower() != "false"
        text = data.decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(text), delimiter=delimiter) if has_header \
            else csv.reader(io.StringIO(text), delimiter=delimiter)
        rows = []
        if has_header:
            for row in reader:
                rows.append(dict(row))
        else:
            for i, row in enumerate(reader):
                rows.append({f"col_{j}": v for j, v in enumerate(row)})
        return rows

    # ── Parquet special case: read column names without downloading whole file ──

    def _get_parquet_schema(self, key: str) -> List[ColumnSchema]:
        try:
            import pyarrow.parquet as pq
            body = self._s3.get_object(Bucket=self._bucket, Key=key)["Body"].read()
            schema = pq.read_schema(io.BytesIO(body))
            type_map = {
                "int32": "integer", "int64": "bigint", "float": "float",
                "double": "double", "bool": "boolean",
                "utf8": "varchar", "large_utf8": "varchar",
                "timestamp[ms]": "timestamp", "timestamp[us]": "timestamp",
                "date32": "date",
            }
            return [
                ColumnSchema(name=field.name,
                             data_type=type_map.get(str(field.type), "varchar"))
                for field in schema
            ]
        except Exception:
            return []

    def get_schema(self, table: str) -> List[ColumnSchema]:
        """Infer schema by peeking at the first file under the table prefix."""
        table_cfg = self._table_cfg(table)
        prefix    = self._resolve_prefix(table)
        files     = self._list_files(prefix, max_files=1)
        if not files:
            return []
        key = files[0]["key"]
        fmt = table_cfg.get("file_format") or _detect_format(key)
        if fmt == "parquet":
            cols = self._get_parquet_schema(key)
            if cols:
                return cols
        rows = self._read_file(key, table_cfg)[:_INFER_LIMIT]
        return _infer_schema_from_rows(rows)

    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        """Read all files under the table prefix."""
        table_cfg = self._table_cfg(table)
        prefix    = self._resolve_prefix(table)
        schema    = self.get_schema(table)
        max_files = table_cfg.get("max_files_per_run")

        files = self._list_files(prefix, max_files=int(max_files) if max_files else None)
        logger.info("[%s/%s] Snapshot — %d files", self.name, table, len(files))

        for f in files:
            rows = self._read_file(f["key"], table_cfg)
            for row in rows:
                yield ChangeEvent(
                    op=Operation.SNAPSHOT,
                    source_name=self.name,
                    source_table=table,
                    before=None,
                    after=row,
                    schema=schema,
                    timestamp=datetime.now(timezone.utc),
                    offset=f["key"],
                )

    def incremental_snapshot(
        self, table: str, cursor_col: str, start_after: Any, chunk_size: int
    ) -> Generator[ChangeEvent, None, None]:
        """Read files newer than start_after (ISO timestamp string)."""
        table_cfg  = self._table_cfg(table)
        prefix     = self._resolve_prefix(table)
        schema     = self.get_schema(table)
        max_files  = table_cfg.get("max_files_per_run")

        newer_than = None
        if start_after:
            try:
                newer_than = datetime.fromisoformat(str(start_after).replace("Z", "+00:00"))
                if newer_than.tzinfo is None:
                    newer_than = newer_than.replace(tzinfo=timezone.utc)
            except Exception:
                newer_than = None

        files = self._list_files(prefix, newer_than=newer_than,
                                 max_files=int(max_files) if max_files else None)
        logger.info("[%s/%s] Incremental — %d new files since %s",
                    self.name, table, len(files), start_after)

        count = 0
        for f in files:
            rows = self._read_file(f["key"], table_cfg)
            for row in rows:
                yield ChangeEvent(
                    op=Operation.SNAPSHOT,
                    source_name=self.name,
                    source_table=table,
                    before=None,
                    after=row,
                    schema=schema,
                    timestamp=datetime.now(timezone.utc),
                    offset=f["last_modified"].isoformat(),
                )
                count += 1
                if count >= chunk_size:
                    return

    def get_cursor_column(self, table: str) -> str:
        return "last_modified"

    def _resolve_prefix(self, table: str) -> str:
        """Table name is the sub-prefix under the base prefix."""
        base = self._prefix.rstrip("/")
        if table and table != "*":
            return f"{base}/{table}".lstrip("/")
        return base

    def _table_cfg(self, table: str) -> Dict:
        """Per-table config, with connection-level format settings as defaults."""
        conn = self.cfg.get("connection", {})
        defaults = {k: conn[k] for k in ("file_format", "csv_delimiter", "csv_has_header") if k in conn}
        per_table = self.cfg.get("tables_config", {}).get(table, {})
        return {**defaults, **per_table}
