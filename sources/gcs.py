"""
Google Cloud Storage load source.

Reads Parquet, CSV, JSON, NDJSON, and Avro files from GCS using
the google-cloud-storage SDK.

Config keys under connection:
  project          GCP project ID (required)
  bucket           GCS bucket name (required)
  prefix           Object prefix / folder path (default: "")
  credentials_file Path to service account JSON key file
                   (omit to use Application Default Credentials / Workload Identity)
"""
from __future__ import annotations

import io
import logging
from typing import Generator, List, Optional

from sources.base import ChangeEvent, ColumnSchema, LoadSource

logger = logging.getLogger(__name__)


class GCSSource(LoadSource):
    def __init__(self, job_id: str, cfg: dict):
        self._job_id = job_id
        self._cfg = cfg
        self._conn_cfg = cfg.get("connection", {})
        self._client = None

    def connect(self):
        from google.cloud import storage
        cred_file = self._conn_cfg.get("credentials_file")
        if cred_file:
            from google.oauth2 import service_account
            creds = service_account.Credentials.from_service_account_file(cred_file)
            self._client = storage.Client(project=self._conn_cfg.get("project"), credentials=creds)
        else:
            self._client = storage.Client(project=self._conn_cfg.get("project"))
        logger.info("[gcs] Connected to project %s", self._conn_cfg.get("project"))

    def close(self):
        pass

    def get_schema(self, table: str) -> List[ColumnSchema]:
        return []

    def _bucket(self):
        return self._client.bucket(self._conn_cfg["bucket"])

    def _list_blobs(self, prefix: str, newer_than: Optional[str] = None):
        from datetime import datetime, timezone
        blobs = self._client.list_blobs(self._conn_cfg["bucket"], prefix=prefix)
        result = []
        for b in blobs:
            if newer_than:
                cutoff = datetime.fromisoformat(newer_than).replace(tzinfo=timezone.utc)
                if b.updated and b.updated <= cutoff:
                    continue
            result.append(b)
        result.sort(key=lambda b: b.updated or "")
        return result

    def _read_blob(self, blob, table_cfg: dict) -> List[dict]:
        data = blob.download_as_bytes()
        fmt = self._detect_format(blob.name, table_cfg)
        return self._parse(data, fmt, table_cfg)

    @staticmethod
    def _detect_format(name: str, table_cfg: dict) -> str:
        if table_cfg.get("file_format"):
            return table_cfg["file_format"]
        n = name.lower()
        if n.endswith(".parquet"): return "parquet"
        if n.endswith(".avro"):    return "avro"
        if n.endswith(".csv"):     return "csv"
        return "json"

    def _parse(self, data: bytes, fmt: str, table_cfg: dict) -> List[dict]:
        if fmt == "parquet":
            import pyarrow.parquet as pq
            return pq.read_table(io.BytesIO(data)).to_pydict()
        if fmt == "avro":
            import fastavro
            return list(fastavro.reader(io.BytesIO(data)))
        if fmt == "csv":
            import csv
            text = data.decode("utf-8-sig")
            return list(csv.DictReader(io.StringIO(text),
                                       delimiter=table_cfg.get("csv_delimiter", ",")))
        import json
        text = data.decode()
        try:
            obj = json.loads(text)
            return obj if isinstance(obj, list) else [obj]
        except json.JSONDecodeError:
            return [json.loads(line) for line in text.splitlines() if line.strip()]

    def _table_cfg(self) -> dict:
        """Connection-level format settings as defaults for all blobs."""
        return {k: self._conn_cfg[k] for k in ("file_format", "csv_delimiter", "csv_has_header") if k in self._conn_cfg}

    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        prefix = self._conn_cfg.get("prefix", "")
        table_cfg = self._table_cfg()
        for blob in self._list_blobs(prefix):
            for row in self._read_blob(blob, table_cfg):
                yield ChangeEvent(op="insert", table=table, after=row,
                                  cursor_value=blob.updated.isoformat() if blob.updated else None)

    def incremental_snapshot(self, table: str, cursor_col: str, start_after,
                             chunk_size: int = 10_000) -> Generator[ChangeEvent, None, None]:
        prefix = self._conn_cfg.get("prefix", "")
        table_cfg = self._table_cfg()
        for blob in self._list_blobs(prefix, newer_than=start_after):
            for row in self._read_blob(blob, table_cfg):
                yield ChangeEvent(op="insert", table=table, after=row,
                                  cursor_value=blob.updated.isoformat() if blob.updated else None)

    def get_cursor_column(self, table: str) -> str:
        return "last_modified"
