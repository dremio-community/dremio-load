"""
Delta Lake load source.

Reads Delta tables from local filesystem, S3, Azure ADLS, or GCS using
the deltalake Python package (delta-rs).

Config keys under connection:
  table_uri       Base URI for Delta tables, e.g.:
                    s3://bucket/prefix        (S3)
                    az://container@account/   (Azure)
                    gs://bucket/prefix        (GCS)
                    /local/path/              (local)
  storage_options Dict of storage backend options:
                    For S3:  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
                    For Azure: AZURE_STORAGE_ACCOUNT_NAME, AZURE_STORAGE_ACCOUNT_KEY
                    For GCS:  GOOGLE_SERVICE_ACCOUNT_KEY

Table name resolves to <table_uri>/<table_name>.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional

from core.event import ChangeEvent, ColumnSchema, Operation
from sources.base import LoadSource

logger = logging.getLogger(__name__)

_ARROW_TYPE_MAP = {
    # PyArrow type strings (deltalake 0.x)
    "int8": "integer", "int16": "integer", "int32": "integer", "int64": "bigint",
    "uint8": "integer", "uint16": "integer", "uint32": "bigint", "uint64": "bigint",
    "float": "float", "double": "double", "decimal128": "decimal",
    "bool": "boolean",
    "utf8": "varchar", "large_utf8": "varchar",
    "date32": "date", "date64": "date",
    "timestamp[s]": "timestamp", "timestamp[ms]": "timestamp",
    "timestamp[us]": "timestamp", "timestamp[ns]": "timestamp",
    "binary": "varbinary", "large_binary": "varbinary",
    # Delta-native type strings (deltalake 1.x PrimitiveType)
    "byte": "integer", "short": "integer", "integer": "integer", "long": "bigint",
    "float": "float", "double": "double",
    "boolean": "boolean",
    "string": "varchar", "binary": "varbinary",
    "date": "date", "timestamp": "timestamp", "timestamp_ntz": "timestamp",
    "decimal": "decimal",
}


class DeltaSource(LoadSource):

    def __init__(self, name: str, cfg: Dict[str, Any]):
        super().__init__(name, cfg)
        conn = cfg.get("connection", {})
        self._base_uri       = conn.get("table_uri", "").rstrip("/")
        self._storage_options: Dict[str, str] = conn.get("storage_options") or {}

    def connect(self):
        try:
            import deltalake  # noqa: F401
        except ImportError:
            raise SystemExit("deltalake required: pip install deltalake")
        logger.info("[delta] Base URI: %s", self._base_uri)

    def close(self):
        pass

    def _open_table(self, table: str):
        from deltalake import DeltaTable
        uri = f"{self._base_uri}/{table}" if self._base_uri else table
        return DeltaTable(uri, storage_options=self._storage_options or None)

    @staticmethod
    def _delta_type_str(field_type) -> str:
        """Extract the base type name from deltalake field types (1.x or 0.x)."""
        s = str(field_type)
        # deltalake 1.x: 'PrimitiveType("long")' → 'long'
        if s.startswith("PrimitiveType("):
            inner = s[len("PrimitiveType(\""):-2].lower()
            return inner
        # deltalake 0.x / PyArrow: 'int64', 'utf8', etc.
        return s.lower().split("[")[0]  # strip e.g. "timestamp[ms]" → "timestamp"

    def get_schema(self, table: str) -> List[ColumnSchema]:
        try:
            dt = self._open_table(table)
            cols = []
            for field in dt.schema().fields:
                type_key = self._delta_type_str(field.type)
                dt_sql = _ARROW_TYPE_MAP.get(type_key, "varchar")
                cols.append(ColumnSchema(name=field.name, data_type=dt_sql))
            return cols
        except Exception:
            return []

    def _to_pylist(self, dt) -> List[Dict]:
        """Read all rows from a DeltaTable as a list of dicts (deltalake 1.x API)."""
        try:
            return dt.to_pyarrow_dataset().to_table().to_pylist()
        except AttributeError:
            return dt.to_pyarrow().to_pylist()  # deltalake 0.x fallback

    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        schema = self.get_schema(table)
        dt = self._open_table(table)
        version = dt.version()
        logger.info("[%s/%s] Snapshot — Delta version %d", self.name, table, version)

        rows = self._to_pylist(dt)
        logger.info("[%s/%s] Snapshot — %d rows", self.name, table, len(rows))

        for row in rows:
            yield ChangeEvent(
                op=Operation.SNAPSHOT,
                source_name=self.name, source_table=table,
                before=None, after=row, schema=schema,
                timestamp=datetime.now(timezone.utc),
                offset=str(version),
            )

    def incremental_snapshot(
        self, table: str, cursor_col: str, start_after: Any, chunk_size: int
    ) -> Generator[ChangeEvent, None, None]:
        from deltalake import DeltaTable
        schema  = self.get_schema(table)
        dt      = self._open_table(table)
        version = dt.version()

        last_version = None
        if start_after is not None and str(start_after) != "":
            try:
                last_version = int(str(start_after))
            except ValueError:
                pass

        if last_version is not None and last_version >= version:
            logger.info("[%s/%s] No new Delta versions (current=%d, last=%d)",
                        self.name, table, version, last_version)
            return

        # Read only rows added in versions newer than last_version
        # Use change data feed if available, otherwise full re-read filtered by version range
        try:
            if last_version is not None:
                cdf = dt.load_cdf(starting_version=last_version + 1).read_all()
                rows = cdf.to_pylist()
            else:
                rows = self._to_pylist(dt)
        except Exception:
            # CDF not enabled — fall back to full table read
            rows = self._to_pylist(dt)

        logger.info("[%s/%s] Incremental — %d rows (version %s → %d)",
                    self.name, table, len(rows), last_version, version)

        count = 0
        for row in rows:
            # Strip CDF internal columns if present
            clean = {k: v for k, v in row.items() if not k.startswith("_change_")}
            yield ChangeEvent(
                op=Operation.SNAPSHOT,
                source_name=self.name, source_table=table,
                before=None, after=clean, schema=schema,
                timestamp=datetime.now(timezone.utc),
                offset=str(version),
            )
            count += 1
            if count >= chunk_size:
                return

    def get_cursor_column(self, table: str) -> str:
        return "_delta_version"
