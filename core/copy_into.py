"""
COPY INTO job manager — wraps Dremio's native COPY INTO SQL command.

Use this when:
  - The source files are in an S3/GCS/ADLS storage source already registered
    in Dremio (visible in the catalog as @source_name).
  - Target is an Iceberg table in Dremio.

Dremio tracks which files it has already loaded, so re-running COPY INTO
only picks up new files (incremental by default).

For MinIO or sources NOT registered in Dremio, use the direct S3Source
in sources/s3.py instead.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Dremio-supported file formats for COPY INTO
SUPPORTED_FORMATS = ("parquet", "json", "csv", "avro", "orc")


def build_copy_into_sql(
    target_table: str,
    source_location: str,
    file_format: str = "parquet",
    format_options: Optional[Dict[str, str]] = None,
    files: Optional[List[str]] = None,
    pattern: Optional[str] = None,
) -> str:
    """
    Build a COPY INTO SQL statement.

    Args:
        target_table:    Fully qualified Iceberg table, e.g. "myspace.orders"
        source_location: Dremio source path, e.g. "@my_s3.path/to/files"
        file_format:     parquet | json | csv | avro | orc
        format_options:  CSV options like {"FIELD_DELIMITER": ",", "TRIM_SPACE": "true"}
        files:           Specific file list (mutually exclusive with pattern)
        pattern:         Glob pattern, e.g. "*.parquet"
    """
    fmt = file_format.upper()
    parts = [f"COPY INTO {target_table}"]
    parts.append(f"FROM '{source_location}'")

    if files:
        file_list = ", ".join(f"'{f}'" for f in files)
        parts.append(f"FILES ({file_list})")
    elif pattern:
        parts.append(f"REGEX '{pattern}'")

    parts.append(f"FILE_FORMAT '{fmt}'")

    if format_options:
        opts = ", ".join(f"'{k}' '{v}'" for k, v in format_options.items())
        parts.append(f"({opts})")

    return "\n".join(parts)


def build_create_pipe_sql(
    pipe_name: str,
    target_table: str,
    source_location: str,
    notification_provider: str,
    notification_queue_reference: str,
    file_format: str = "parquet",
    format_options: Optional[Dict[str, str]] = None,
    dedupe_lookback_days: int = 14,
) -> str:
    """Build a CREATE PIPE statement for event-driven ingestion via SQS."""
    copy_sql = build_copy_into_sql(
        target_table, source_location, file_format, format_options
    )
    return (
        f"CREATE PIPE {pipe_name}\n"
        f"  NOTIFICATION_PROVIDER '{notification_provider}'\n"
        f"  NOTIFICATION_QUEUE_REFERENCE '{notification_queue_reference}'\n"
        f"  DEDUPE_LOOKBACK_PERIOD {dedupe_lookback_days}\n"
        f"AS {copy_sql}"
    )


class CopyIntoJob:
    """
    Executes a COPY INTO SQL command against Dremio and records the result.
    Reuses DremioSink's SQL execution infrastructure.
    """

    def __init__(self, job_id: str, job_cfg: Dict[str, Any], target_cfg: Dict[str, Any]):
        self.job_id     = job_id
        self.job_cfg    = job_cfg
        self.target_cfg = target_cfg

    def run(self) -> Dict[str, Any]:
        from core.dremio_sink import DremioSink

        target_table     = self.job_cfg["target_table"]
        source_location  = self.job_cfg["source_location"]
        file_format      = self.job_cfg.get("file_format", "parquet")
        format_options   = self.job_cfg.get("format_options", {})
        pattern          = self.job_cfg.get("pattern")

        sql = build_copy_into_sql(
            target_table=target_table,
            source_location=source_location,
            file_format=file_format,
            format_options=format_options or None,
            pattern=pattern,
        )

        logger.info("[%s] Running COPY INTO:\n%s", self.job_id, sql)
        started = datetime.now(timezone.utc)

        sink = DremioSink(target_table, self.target_cfg)
        sink.connect()
        try:
            result = sink._sql(sql)
            rows = 0
            if result:
                # Dremio returns rows_loaded in job results for COPY INTO
                rows = result.get("rows_loaded") or result.get("rowCount") or 0
        except Exception as exc:
            elapsed = (datetime.now(timezone.utc) - started).total_seconds()
            logger.error("[%s] COPY INTO failed after %.1fs: %s", self.job_id, elapsed, exc)
            return {
                "job_id": self.job_id,
                "status": "error",
                "error": str(exc),
                "started": started.isoformat(),
                "duration_s": elapsed,
                "rows": 0,
                "sql": sql,
            }
        finally:
            sink.close()

        elapsed = (datetime.now(timezone.utc) - started).total_seconds()
        logger.info("[%s] COPY INTO complete — %d rows in %.1fs", self.job_id, rows, elapsed)
        return {
            "job_id": self.job_id,
            "status": "success",
            "started": started.isoformat(),
            "duration_s": elapsed,
            "rows": rows,
            "sql": sql,
        }

    def create_pipe(self) -> Dict[str, Any]:
        """Execute CREATE PIPE for event-driven ingestion."""
        from core.dremio_sink import DremioSink

        sql = build_create_pipe_sql(
            pipe_name=self.job_cfg["pipe_name"],
            target_table=self.job_cfg["target_table"],
            source_location=self.job_cfg["source_location"],
            notification_provider=self.job_cfg.get("notification_provider", "AWS_SQS"),
            notification_queue_reference=self.job_cfg["notification_queue_reference"],
            file_format=self.job_cfg.get("file_format", "parquet"),
            format_options=self.job_cfg.get("format_options"),
            dedupe_lookback_days=int(self.job_cfg.get("dedupe_lookback_days", 14)),
        )

        logger.info("[%s] Creating pipe:\n%s", self.job_id, sql)
        sink = DremioSink(self.job_cfg["target_table"], self.target_cfg)
        sink.connect()
        try:
            sink._sql(sql)
            return {"ok": True, "sql": sql}
        except Exception as exc:
            return {"ok": False, "error": str(exc), "sql": sql}
        finally:
            sink.close()

    def drop_pipe(self) -> Dict[str, Any]:
        from core.dremio_sink import DremioSink
        pipe_name = self.job_cfg.get("pipe_name", "")
        sql = f"DROP PIPE IF EXISTS {pipe_name}"
        sink = DremioSink(self.job_cfg["target_table"], self.target_cfg)
        sink.connect()
        try:
            sink._sql(sql)
            return {"ok": True}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}
        finally:
            sink.close()

    def set_pipe_running(self, running: bool) -> Dict[str, Any]:
        from core.dremio_sink import DremioSink
        pipe_name = self.job_cfg.get("pipe_name", "")
        flag = "TRUE" if running else "FALSE"
        sql = f"ALTER PIPE {pipe_name} SET PIPE_EXECUTION_RUNNING = {flag}"
        sink = DremioSink(self.job_cfg["target_table"], self.target_cfg)
        sink.connect()
        try:
            sink._sql(sql)
            return {"ok": True, "running": running}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}
        finally:
            sink.close()
