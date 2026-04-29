"""
Dremio Load Engine — batch/scheduled data ingestion.

Two load modes per job:
  copy_into   Use Dremio's native COPY INTO SQL command (files already in a
              Dremio-registered S3/GCS/ADLS source).  The engine just fires SQL.
  direct      Read source data (S3/MinIO via boto3, DB snapshot, REST API) and
              write to Dremio via DremioSink (MERGE/INSERT) or IcebergSink
              (PyIceberg direct write).

Each job runs on a cron schedule (or on-demand).  Runs are recorded in the
SQLite job store so the UI can show history, row counts, and errors.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from core.event import ChangeEvent, Operation
from core.offset_store import get_offset_store
from core.schema_store import SchemaStore
from core.masking import MaskingEngine

logger = logging.getLogger(__name__)

_REGISTRY: Dict[str, Any] = {}  # source_type -> class, populated by load_sources()

# Module-level notification settings — updated by the Flask app when saved
_notification_settings: Dict[str, Any] = {}


def set_notification_settings(settings: Dict[str, Any]) -> None:
    global _notification_settings
    _notification_settings = settings


def load_sources():
    """Register all source classes into _REGISTRY."""
    from sources.s3 import S3Source
    _REGISTRY["s3"] = S3Source
    _REGISTRY["minio"] = S3Source  # alias

    try:
        from sources.postgres import PostgresSource
        _REGISTRY["postgres"] = PostgresSource
    except Exception:
        pass
    try:
        from sources.mysql import MySQLSource
        _REGISTRY["mysql"] = MySQLSource
        _REGISTRY["mariadb"] = MySQLSource
    except Exception:
        pass
    try:
        from sources.mongodb import MongoDBSource
        _REGISTRY["mongodb"] = MongoDBSource
    except Exception:
        pass
    try:
        from sources.sqlserver import SQLServerSource
        _REGISTRY["sqlserver"] = SQLServerSource
    except Exception:
        pass
    try:
        from sources.azure_blob import AzureBlobSource
        _REGISTRY["azure_blob"] = AzureBlobSource
        _REGISTRY["adls"] = AzureBlobSource
    except Exception:
        pass
    try:
        from sources.gcs import GCSSource
        _REGISTRY["gcs"] = GCSSource
    except Exception:
        pass
    try:
        from sources.snowflake import SnowflakeSource
        _REGISTRY["snowflake"] = SnowflakeSource
    except Exception:
        pass
    try:
        from sources.databricks import DatabricksSource
        _REGISTRY["databricks"] = DatabricksSource
    except Exception:
        pass
    try:
        from sources.oracle import OracleSource
        _REGISTRY["oracle"] = OracleSource
    except Exception:
        pass
    try:
        from sources.salesforce import SalesforceSource
        _REGISTRY["salesforce"] = SalesforceSource
    except Exception:
        pass
    try:
        from sources.delta import DeltaSource
        _REGISTRY["delta"] = DeltaSource
        _REGISTRY["delta_lake"] = DeltaSource
    except Exception:
        pass
    try:
        from sources.hudi import HudiSource
        _REGISTRY["hudi"] = HudiSource
    except Exception:
        pass
    try:
        from sources.cosmosdb import CosmosDBSource
        _REGISTRY["cosmosdb"] = CosmosDBSource
        _REGISTRY["cosmos"] = CosmosDBSource
    except Exception:
        pass
    try:
        from sources.dynamodb import DynamoDBSource
        _REGISTRY["dynamodb"] = DynamoDBSource
    except Exception:
        pass
    try:
        from sources.pinot import PinotSource
        _REGISTRY["pinot"] = PinotSource
    except Exception:
        pass
    try:
        from sources.splunk import SplunkSource
        _REGISTRY["splunk"] = SplunkSource
    except Exception:
        pass
    try:
        from sources.spanner import SpannerSource
        _REGISTRY["spanner"] = SpannerSource
    except Exception:
        pass
    try:
        from sources.hubspot import HubSpotSource
        _REGISTRY["hubspot"] = HubSpotSource
    except Exception:
        pass
    try:
        from sources.zendesk import ZendeskSource
        _REGISTRY["zendesk"] = ZendeskSource
    except Exception:
        pass
    try:
        from sources.google_ads import GoogleAdsSource
        _REGISTRY["google_ads"] = GoogleAdsSource
    except Exception:
        pass
    try:
        from sources.linkedin_ads import LinkedInAdsSource
        _REGISTRY["linkedin_ads"] = LinkedInAdsSource
    except Exception:
        pass
    try:
        from sources.cassandra import CassandraSource
        _REGISTRY["cassandra"] = CassandraSource
    except Exception:
        pass
    try:
        from sources.clickhouse import ClickHouseSource
        _REGISTRY["clickhouse"] = ClickHouseSource
    except Exception:
        pass
    _REGISTRY["s3_compat"] = _REGISTRY.get("s3")  # same driver, endpoint_url differentiates


def _make_sink(target_cfg: Dict, table: str):
    """Instantiate the correct sink from target config."""
    mode = target_cfg.get("mode", "a")  # "a" = dremio sql, "b" = iceberg
    if mode == "b":
        from core.iceberg_sink import IcebergSink
        return IcebergSink(table, target_cfg)
    else:
        from core.dremio_sink import DremioSink
        return DremioSink(target_cfg)


# ── Job run record ─────────────────────────────────────────────────────────────

class JobRun:
    def __init__(self, job_id: str, table: str):
        self.job_id    = job_id
        self.table     = table
        self.started   = datetime.now(timezone.utc)
        self.finished  = None
        self.status    = "running"
        self.rows      = 0
        self.error     = None

    def complete(self, rows: int):
        self.finished = datetime.now(timezone.utc)
        self.rows     = rows
        self.status   = "success"

    def fail(self, error: str):
        self.finished = datetime.now(timezone.utc)
        self.error    = error
        self.status   = "error"

    @property
    def duration_s(self) -> Optional[float]:
        if self.finished:
            return (self.finished - self.started).total_seconds()
        return None


# ── Table worker ───────────────────────────────────────────────────────────────

class TableWorker:
    """Runs one load job for one table/path."""

    def __init__(self, job_id: str, table: str, source, target_cfg: Dict,
                 load_mode: str, chunk_size: int,
                 offset_store, schema_store: SchemaStore,
                 masking: Optional[MaskingEngine],
                 on_run_complete=None):
        self.job_id       = job_id
        self.table        = table
        self.source       = source
        self.target_cfg   = target_cfg
        self.load_mode    = load_mode   # "full" | "incremental"
        self.chunk_size   = chunk_size
        self.offset_store = offset_store
        self.schema_store = schema_store
        self.masking      = masking
        self.on_run_complete = on_run_complete

    def run(self) -> JobRun:
        run = JobRun(self.job_id, self.table)
        try:
            rows = self._execute()
            run.complete(rows)
        except Exception as exc:
            logger.error("[%s/%s] Load failed: %s", self.job_id, self.table, exc, exc_info=True)
            run.fail(str(exc))
        finally:
            if self.on_run_complete:
                self.on_run_complete(run)
        return run

    def _execute(self) -> int:
        source_name = self.source.name

        # ── Full load ──────────────────────────────────────────────────────────
        if self.load_mode in ("full", "ctas"):
            sink = _make_sink(self.target_cfg, self.table)
            sink.connect()
            rows = 0
            batch: List[ChangeEvent] = []
            dropped = False  # tracks whether we've done the CTAS drop yet
            for ev in self.source.snapshot(self.table):
                if self.masking:
                    ev = self.masking.apply(ev)
                batch.append(ev)
                if len(batch) >= self.chunk_size:
                    if self.load_mode == "ctas" and not dropped:
                        sink.drop_table(self.table)
                        dropped = True
                    sink.write_batch(batch)
                    rows += len(batch)
                    batch = []
            if batch:
                if self.load_mode == "ctas" and not dropped:
                    sink.drop_table(self.table)
                sink.write_batch(batch)
                rows += len(batch)
            sink.close()
            self.offset_store.set(source_name, self.table, "full:done")
            logger.info("[%s/%s] %s load complete — %d rows",
                        self.job_id, self.table, self.load_mode.upper(), rows)
            return rows

        # ── Incremental load ───────────────────────────────────────────────────
        cursor_col = self.source.get_cursor_column(self.table)
        saved      = self.offset_store.get(source_name, self.table)
        last_val   = None
        if saved and saved.startswith("inc:"):
            last_val = saved[4:]  # everything after "inc:"

        sink = _make_sink(self.target_cfg, self.table)
        sink.connect()
        rows = 0

        while True:
            batch = list(self.source.incremental_snapshot(
                self.table, cursor_col, last_val, self.chunk_size
            ))
            if not batch:
                break
            if self.masking:
                batch = [self.masking.apply(ev) for ev in batch]
            sink.write_batch(batch)
            rows += len(batch)
            # Advance cursor to last value in this chunk
            last_row = batch[-1].after or batch[-1].before or {}
            last_val = str(last_row.get(cursor_col, last_val))
            self.offset_store.set(source_name, self.table, f"inc:{last_val}")
            if len(batch) < self.chunk_size:
                break  # last chunk — no more data

        sink.close()
        logger.info("[%s/%s] Incremental load complete — %d rows (cursor=%s=%s)",
                    self.job_id, self.table, rows, cursor_col, last_val)
        return rows


# ── Load engine ────────────────────────────────────────────────────────────────

class LoadEngine:
    """
    Manages all configured load jobs.  Each job runs in its own thread when
    triggered (manually or by the scheduler).
    """

    def __init__(self, cfg: Dict[str, Any]):
        self.cfg        = cfg
        self._jobs: Dict[str, Dict] = {}   # job_id -> job config
        self._runs: List[JobRun]    = []   # in-memory history (UI reads this)
        self._lock      = threading.Lock()
        self._running   = {}               # job_id -> Thread

        db_path = cfg.get("options", {}).get("offset_db_path", "./load_offsets.db")
        self.offset_store  = get_offset_store(db_path)
        self.schema_store  = SchemaStore(cfg.get("options", {}).get("schema_db_path", "./load_schemas.db"))

        load_sources()

    def add_job(self, job_id: str, job_cfg: Dict):
        with self._lock:
            self._jobs[job_id] = job_cfg

    def remove_job(self, job_id: str):
        with self._lock:
            self._jobs.pop(job_id, None)

    def get_jobs(self) -> Dict:
        with self._lock:
            return dict(self._jobs)

    def get_runs(self, job_id: str = None) -> List[JobRun]:
        with self._lock:
            if job_id:
                return [r for r in self._runs if r.job_id == job_id]
            return list(self._runs)

    def trigger(self, job_id: str) -> Optional[threading.Thread]:
        """Start a job run in a background thread. Returns thread or None if already running."""
        with self._lock:
            if job_id in self._running and self._running[job_id].is_alive():
                logger.warning("[%s] Already running — skipping trigger", job_id)
                return None
            job_cfg = self._jobs.get(job_id)
            if not job_cfg:
                raise KeyError(f"Job {job_id!r} not found")

        t = threading.Thread(target=self._run_job, args=(job_id, job_cfg), daemon=True,
                             name=f"load/{job_id}")
        with self._lock:
            self._running[job_id] = t
        t.start()
        return t

    def _run_job(self, job_id: str, job_cfg: Dict):
        source_type = job_cfg["source_type"]
        source_cls  = _REGISTRY.get(source_type)
        if not source_cls:
            logger.error("[%s] Unknown source type: %s", job_id, source_type)
            return

        source = source_cls(job_id, job_cfg)
        try:
            source.connect()
        except Exception as exc:
            logger.error("[%s] Source connect failed: %s", job_id, exc)
            return

        target_cfg = self.cfg.get("target", {})
        load_mode  = job_cfg.get("load_mode", "incremental")
        chunk_size = int(job_cfg.get("chunk_size", 5000))
        tables     = job_cfg.get("tables", [])
        masking_cfg = job_cfg.get("masking", {})
        masking = MaskingEngine(masking_cfg) if masking_cfg else None

        for table in tables:
            worker = TableWorker(
                job_id=job_id,
                table=table,
                source=source,
                target_cfg=target_cfg,
                load_mode=load_mode,
                chunk_size=chunk_size,
                offset_store=self.offset_store,
                schema_store=self.schema_store,
                masking=masking,
                on_run_complete=self._on_run_complete,
            )
            worker.run()

        try:
            source.close()
        except Exception:
            pass

    def _on_run_complete(self, run: JobRun):
        with self._lock:
            self._runs.append(run)
            if len(self._runs) > 500:
                self._runs = self._runs[-500:]

        job_cfg = self._jobs.get(run.job_id, {})
        self._fire_hooks(run, job_cfg)

    def _fire_hooks(self, run: JobRun, job_cfg: Dict):
        from core.notifier import send_notification, fire_webhook

        # Notifications on failure
        if run.status == "error" and _notification_settings:
            msg = f"Table: {run.table}\nError: {run.error}\nDuration: {run.duration_s:.1f}s"
            send_notification(job_cfg.get("name", run.job_id), run.status, msg,
                              _notification_settings)

        # Webhook post-hooks
        payload = {
            "job_id": run.job_id,
            "job_name": job_cfg.get("name", run.job_id),
            "table": run.table,
            "status": run.status,
            "rows": run.rows,
            "error": run.error,
            "duration_s": run.duration_s,
        }
        if run.status == "error":
            url = job_cfg.get("on_failure_url") or job_cfg.get("config", {}).get("on_failure_url")
            if url:
                fire_webhook(url, payload)
        else:
            url = job_cfg.get("on_success_url") or job_cfg.get("config", {}).get("on_success_url")
            if url:
                fire_webhook(url, payload)

            # Transform Studio pipeline trigger
            ts_url   = job_cfg.get("ts_url")
            ts_token = job_cfg.get("ts_pipeline_token")
            if ts_url and ts_token:
                webhook_url = f"{ts_url.rstrip('/')}/api/webhooks/{ts_token}/trigger?mode=async"
                fire_webhook(webhook_url, {"source": "dremio-load", "job_id": run.job_id})

    def reset_offset(self, job_id: str, table: str):
        """Clear saved offset so next run does a full reload."""
        job_cfg = self._jobs.get(job_id, {})
        self.offset_store.set(job_id, table, None)
