"""
Azure Blob Storage / ADLS Gen2 load source.

Reads Parquet, CSV, JSON, NDJSON, and Avro files from Azure Blob Storage
or Azure Data Lake Storage Gen2 using the azure-storage-blob SDK.

Config keys under connection:
  account_name    Storage account name (required)
  account_key     Storage account key  (or use client_id + client_secret + tenant_id)
  container       Blob container / filesystem name (required)
  prefix          Blob prefix / folder path (default: "")

  For ADLS Gen2 service principal auth:
  tenant_id       Azure AD tenant ID
  client_id       Application (client) ID
  client_secret   Client secret value
"""
from __future__ import annotations

import io
import logging
from typing import Generator, List, Optional

from sources.base import ChangeEvent, ColumnSchema, LoadSource

logger = logging.getLogger(__name__)


class AzureBlobSource(LoadSource):
    def __init__(self, job_id: str, cfg: dict):
        self._job_id = job_id
        self._cfg = cfg
        self._conn_cfg = cfg.get("connection", {})
        self._client = None

    def connect(self):
        from azure.storage.blob import BlobServiceClient
        from azure.identity import ClientSecretCredential

        account = self._conn_cfg["account_name"]
        key = self._conn_cfg.get("account_key")

        if key:
            url = f"https://{account}.blob.core.windows.net"
            self._client = BlobServiceClient(account_url=url, credential=key)
        else:
            cred = ClientSecretCredential(
                tenant_id=self._conn_cfg["tenant_id"],
                client_id=self._conn_cfg["client_id"],
                client_secret=self._conn_cfg["client_secret"],
            )
            url = f"https://{account}.blob.core.windows.net"
            self._client = BlobServiceClient(account_url=url, credential=cred)

        logger.info("[azure_blob] Connected to account %s", account)

    def close(self):
        if self._client:
            self._client.close()

    def get_schema(self, table: str) -> List[ColumnSchema]:
        return []

    def _container(self):
        return self._conn_cfg.get("container", "")

    def _list_blobs(self, prefix: str, newer_than: Optional[str] = None):
        from datetime import datetime, timezone
        cc = self._client.get_container_client(self._container())
        blobs = cc.list_blobs(name_starts_with=prefix)
        result = []
        for b in blobs:
            if newer_than:
                cutoff = datetime.fromisoformat(newer_than).replace(tzinfo=timezone.utc)
                if b.last_modified and b.last_modified <= cutoff:
                    continue
            result.append(b)
        result.sort(key=lambda b: b.last_modified or "")
        return result

    def _read_blob(self, blob_name: str, table_cfg: dict) -> List[dict]:
        cc = self._client.get_container_client(self._container())
        data = cc.download_blob(blob_name).readall()
        fmt = self._detect_format(blob_name, table_cfg)
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
            table = pq.read_table(io.BytesIO(data))
            return table.to_pydict()
        if fmt == "avro":
            import fastavro
            return list(fastavro.reader(io.BytesIO(data)))
        if fmt == "csv":
            import csv as csvmod
            text = data.decode("utf-8-sig")
            reader = csvmod.DictReader(io.StringIO(text),
                                       delimiter=table_cfg.get("csv_delimiter", ","))
            return list(reader)
        # json / ndjson
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
            for row in self._read_blob(blob.name, table_cfg):
                yield ChangeEvent(op="insert", table=table, after=row,
                                  cursor_value=blob.last_modified.isoformat() if blob.last_modified else None)

    def incremental_snapshot(self, table: str, cursor_col: str, start_after,
                             chunk_size: int = 10_000) -> Generator[ChangeEvent, None, None]:
        prefix = self._conn_cfg.get("prefix", "")
        table_cfg = self._table_cfg()
        for blob in self._list_blobs(prefix, newer_than=start_after):
            for row in self._read_blob(blob.name, table_cfg):
                yield ChangeEvent(op="insert", table=table, after=row,
                                  cursor_value=blob.last_modified.isoformat() if blob.last_modified else None)

    def get_cursor_column(self, table: str) -> str:
        return "last_modified"
