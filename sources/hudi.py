"""
Apache Hudi load source.

Reads Hudi Copy-on-Write (CoW) tables from S3, GCS, Azure Blob, or local
filesystem by scanning the Hudi timeline and reading base Parquet files
for the latest committed snapshot.

Config keys under connection:
  table_uri       Base URI containing Hudi tables, e.g. s3://bucket/hudi/
  aws_access_key_id      (optional, for S3)
  aws_secret_access_key  (optional, for S3)
  endpoint_url           (optional, for MinIO / S3-compat)
  region_name            (optional, default us-east-1)

Table name = sub-path under table_uri, e.g. "orders" → s3://bucket/hudi/orders/

Incremental mode:
  cursor_col is always "_hoodie_commit_time".
  The engine saves the last commit timestamp; next run reads only files
  written in commits after that watermark.
"""
from __future__ import annotations

import io
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional

from core.event import ChangeEvent, ColumnSchema, Operation
from sources.base import LoadSource

logger = logging.getLogger(__name__)

HOODIE_DIR = ".hoodie"
HOODIE_COMMIT_EXT = ".commit"
HOODIE_DELTACOMMIT_EXT = ".deltacommit"


class HudiSource(LoadSource):

    def __init__(self, name: str, cfg: Dict[str, Any]):
        super().__init__(name, cfg)
        conn = cfg.get("connection", {})
        self._base_uri   = conn.get("table_uri", "").rstrip("/")
        self._access_key = conn.get("aws_access_key_id")
        self._secret_key = conn.get("aws_secret_access_key")
        self._endpoint   = conn.get("endpoint_url")
        self._region     = conn.get("region_name", "us-east-1")
        self._fs         = None

    def connect(self):
        try:
            import pyarrow.fs as pafs  # noqa: F401
        except ImportError:
            raise SystemExit("pyarrow required: pip install pyarrow")
        self._fs = self._make_fs()
        logger.info("[hudi] Base URI: %s", self._base_uri)

    def _make_fs(self):
        import pyarrow.fs as pafs
        uri = self._base_uri
        if uri.startswith("s3://") or uri.startswith("s3a://"):
            kwargs: Dict[str, Any] = {"region": self._region}
            if self._access_key:
                kwargs["access_key"] = self._access_key
            if self._secret_key:
                kwargs["secret_key"] = self._secret_key
            if self._endpoint:
                kwargs["endpoint_override"] = self._endpoint
                kwargs["scheme"] = "http" if self._endpoint.startswith("http://") else "https"
                kwargs["force_virtual_addressing"] = False
            return pafs.S3FileSystem(**kwargs)
        if uri.startswith("gs://"):
            return pafs.GcsFileSystem()
        return pafs.LocalFileSystem()

    def _table_path(self, table: str) -> str:
        return f"{self._base_uri}/{table}" if self._base_uri else table

    def _strip_scheme(self, uri: str) -> str:
        for scheme in ("s3://", "s3a://", "gs://", "az://"):
            if uri.startswith(scheme):
                return uri[len(scheme):]
        return uri

    def _list_commits(self, table_path: str, after: Optional[str] = None) -> List[str]:
        """Return sorted list of completed commit timestamps."""
        hoodie_path = self._strip_scheme(f"{table_path}/{HOODIE_DIR}")
        try:
            selector = self._fs.get_file_info(
                __import__("pyarrow.fs", fromlist=["FileSelector"]).FileSelector(hoodie_path)
            )
        except Exception:
            return []
        commits = []
        for info in selector:
            name = info.base_name
            if name.endswith(HOODIE_COMMIT_EXT) or name.endswith(HOODIE_DELTACOMMIT_EXT):
                ts = name.split(".")[0]
                if after and ts <= after:
                    continue
                commits.append(ts)
        return sorted(commits)

    def _list_parquet_files(self, table_path: str, commit_ts: Optional[str] = None) -> List[str]:
        """List base Parquet files in the table (optionally filtered to a specific commit)."""
        import pyarrow.fs as pafs
        root = self._strip_scheme(table_path)
        selector = pafs.FileSelector(root, recursive=True)
        try:
            infos = self._fs.get_file_info(selector)
        except Exception:
            return []
        files = []
        for info in infos:
            p = info.path
            if "/.hoodie" in p or not p.endswith(".parquet"):
                continue
            if commit_ts:
                # Hudi base file naming: <partition>/<fileId>_<writeToken>_<commitTime>.parquet
                if f"_{commit_ts}.parquet" not in p:
                    continue
            files.append(p)
        return sorted(files)

    def _read_parquet(self, fs_path: str) -> List[Dict]:
        import pyarrow.parquet as pq
        with self._fs.open_input_file(fs_path) as f:
            data = f.read()
        table = pq.read_table(io.BytesIO(data))
        return table.to_pylist()

    def get_schema(self, table: str) -> List[ColumnSchema]:
        import pyarrow.parquet as pq
        table_path = self._table_path(table)
        files = self._list_parquet_files(table_path)
        if not files:
            return []
        try:
            with self._fs.open_input_file(files[0]) as f:
                data = f.read()
            schema = pq.read_schema(io.BytesIO(data))
            type_map = {
                "int32": "integer", "int64": "bigint", "float": "float",
                "double": "double", "bool": "boolean",
                "utf8": "varchar", "large_utf8": "varchar",
                "timestamp[ms]": "timestamp", "timestamp[us]": "timestamp",
                "date32": "date",
            }
            return [
                ColumnSchema(name=f.name, data_type=type_map.get(str(f.type), "varchar"))
                for f in schema
            ]
        except Exception:
            return []

    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        schema = self.get_schema(table)
        table_path = self._table_path(table)
        files = self._list_parquet_files(table_path)
        logger.info("[%s/%s] Snapshot — %d Parquet files", self.name, table, len(files))

        for fp in files:
            for row in self._read_parquet(fp):
                if row.get("_hoodie_is_deleted"):
                    continue
                commit_ts = row.get("_hoodie_commit_time", "")
                yield ChangeEvent(
                    op=Operation.SNAPSHOT,
                    source_name=self.name, source_table=table,
                    before=None, after=row, schema=schema,
                    timestamp=datetime.now(timezone.utc),
                    offset=commit_ts,
                )

    def incremental_snapshot(
        self, table: str, cursor_col: str, start_after: Any, chunk_size: int
    ) -> Generator[ChangeEvent, None, None]:
        schema     = self.get_schema(table)
        table_path = self._table_path(table)
        commits    = self._list_commits(table_path, after=str(start_after) if start_after else None)

        if not commits:
            logger.info("[%s/%s] No new commits since %s", self.name, table, start_after)
            return

        logger.info("[%s/%s] Incremental — %d new commits since %s",
                    self.name, table, len(commits), start_after)
        count = 0
        for commit_ts in commits:
            files = self._list_parquet_files(table_path, commit_ts=commit_ts)
            for fp in files:
                for row in self._read_parquet(fp):
                    if row.get("_hoodie_is_deleted"):
                        continue
                    yield ChangeEvent(
                        op=Operation.SNAPSHOT,
                        source_name=self.name, source_table=table,
                        before=None, after=row, schema=schema,
                        timestamp=datetime.now(timezone.utc),
                        offset=commit_ts,
                    )
                    count += 1
                    if count >= chunk_size:
                        return

    def close(self):
        self._fs = None

    def get_cursor_column(self, table: str) -> str:
        return "_hoodie_commit_time"
