"""
Abstract base class for all load source connectors.
Unlike the CDC base, stream() is NOT required — load sources only need
snapshot() and/or incremental_snapshot().
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Generator, List, Optional

from core.event import ChangeEvent, ColumnSchema


class LoadSource(ABC):

    def __init__(self, name: str, cfg: Dict[str, Any]):
        self.name = name
        self.cfg  = cfg

    @abstractmethod
    def connect(self):
        """Open connection / validate credentials."""

    @abstractmethod
    def get_schema(self, table: str) -> List[ColumnSchema]:
        """Return column definitions for a table/path."""

    @abstractmethod
    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        """Full scan — yield all rows as SNAPSHOT ChangeEvents."""

    def incremental_snapshot(
        self, table: str, cursor_col: str, start_after: Any, chunk_size: int
    ) -> Generator[ChangeEvent, None, None]:
        """
        Yield one chunk of rows where cursor_col > start_after,
        ordered by cursor_col, limited to chunk_size.
        Default falls back to full snapshot (subclasses should override).
        """
        yield from self.snapshot(table)

    def get_cursor_column(self, table: str) -> Optional[str]:
        """
        Return the best column for incremental loads.
        For DB sources: first PK or updated_at column.
        For file sources: override to return 'last_modified'.
        """
        try:
            schema = self.get_schema(table)
            for col in schema:
                if col.primary_key:
                    return col.name
            return schema[0].name if schema else None
        except Exception:
            return None

    def close(self):
        pass

    @property
    def tables(self) -> List[str]:
        return self.cfg.get("tables", [])
