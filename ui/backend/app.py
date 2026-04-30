"""Flask REST API + SPA serving for Dremio Load UI."""
from __future__ import annotations

import base64
import json
import logging
import os
import secrets as _secrets
import urllib.parse
import urllib.request
from typing import Any, Dict

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

from ui.backend.store import LoadStore

logger = logging.getLogger(__name__)

store = LoadStore(os.getenv("LOAD_DB_PATH", "./load_ui.db"))

FRONTEND_DIST = os.path.join(os.path.dirname(__file__), "..", "frontend", "dist")

SOURCE_TYPE_LABELS = {
    "s3": "Amazon S3", "azure_blob": "Azure Blob", "adls": "Azure Data Lake Gen2",
    "gcs": "Google Cloud Storage", "s3_compat": "S3-Compatible",
    "delta": "Delta Lake", "delta_lake": "Delta Lake", "hudi": "Apache Hudi",
    "postgres": "PostgreSQL", "mysql": "MySQL", "sqlserver": "SQL Server",
    "oracle": "Oracle", "mongodb": "MongoDB", "snowflake": "Snowflake",
    "dynamodb": "Amazon DynamoDB", "cosmosdb": "Azure Cosmos DB", "cosmos": "Azure Cosmos DB",
    "spanner": "Google Cloud Spanner",
    "salesforce": "Salesforce", "hubspot": "HubSpot", "zendesk": "Zendesk",
    "pinot": "Apache Pinot", "splunk": "Splunk",
    "cassandra": "Apache Cassandra", "clickhouse": "ClickHouse",
    "databricks": "Databricks", "copy_into": "COPY INTO",
    "google_ads": "Google Ads", "linkedin_ads": "LinkedIn Ads",
}

def _source_label(job_cfg: Dict) -> str:
    st = job_cfg.get("source_type", "")
    label = SOURCE_TYPE_LABELS.get(st, st)
    host = job_cfg.get("host") or job_cfg.get("bucket") or job_cfg.get("account")
    return f"{label} ({host})" if host else label

def _target_table(job_cfg: Dict, catalog: str, schema: str) -> str:
    tables = job_cfg.get("tables", [])
    if not tables:
        return f"{catalog}.{schema}.*" if catalog and schema else "—"
    if len(tables) == 1:
        t = tables[0]
        tbl = t.get("target_table") or t.get("table") or ""
        return f"{catalog}.{schema}.{tbl}" if catalog and schema else tbl
    return f"{catalog}.{schema}.* ({len(tables)} tables)" if catalog and schema else f"{len(tables)} tables"


def create_app(engine, cfg: Dict[str, Any]) -> Flask:
    app = Flask(__name__, static_folder=None)
    CORS(app)

    # Persist initial jobs from config into the store
    for job_cfg in cfg.get("jobs", []):
        store.upsert_job(
            job_id=job_cfg["id"],
            name=job_cfg.get("name", job_cfg["id"]),
            config=job_cfg,
            enabled=True,
        )

    # Restore any UI-created jobs from the DB that aren't in the config
    for db_job in store.get_jobs():
        job_id = db_job["id"]
        if job_id not in engine.get_jobs() and db_job.get("enabled", 1):
            try:
                job_cfg = json.loads(db_job["config_json"])
                engine.add_job(job_id, job_cfg)
            except Exception:
                pass

    # Wire run completion to store
    original_on_run = engine._on_run_complete

    def _on_run_with_persist(run):
        original_on_run(run)
        store.save_run(run)

    engine._on_run_complete = _on_run_with_persist

    # ── Health ────────────────────────────────────────────────────────────────

    @app.get("/api/health")
    def health():
        return jsonify({"status": "ok", "jobs": len(engine.get_jobs())})

    # ── Jobs ──────────────────────────────────────────────────────────────────

    @app.get("/api/jobs")
    def list_jobs():
        db_jobs = {j["id"]: j for j in store.get_jobs()}
        result = []
        for job_id, job_cfg in engine.get_jobs().items():
            db = db_jobs.get(job_id, {})
            is_running = job_id in engine._running and engine._running[job_id].is_alive()
            runs = store.get_runs(job_id, limit=1)
            last_run = runs[0] if runs else None
            result.append({
                "id": job_id,
                "name": job_cfg.get("name", job_id),
                "source_type": job_cfg.get("source_type"),
                "load_mode": job_cfg.get("load_mode", "incremental"),
                "schedule": job_cfg.get("schedule"),
                "enabled": bool(db.get("enabled", 1)),
                "running": is_running,
                "last_run": last_run,
                "tables": job_cfg.get("tables", []),
            })
        return jsonify(result)

    @app.post("/api/jobs")
    def create_job():
        body = request.json
        job_id = body.get("id") or body.get("name", "").lower().replace(" ", "_")
        if not job_id:
            return jsonify({"error": "id required"}), 400
        engine.add_job(job_id, body)
        store.upsert_job(job_id, body.get("name", job_id), body)
        return jsonify({"id": job_id}), 201

    @app.get("/api/jobs/<job_id>")
    def get_job(job_id):
        jobs = engine.get_jobs()
        if job_id not in jobs:
            return jsonify({"error": "not found"}), 404
        return jsonify(jobs[job_id])

    @app.put("/api/jobs/<job_id>")
    def update_job(job_id):
        body = request.json
        engine.add_job(job_id, body)
        store.upsert_job(job_id, body.get("name", job_id), body)
        return jsonify({"ok": True})

    @app.delete("/api/jobs/<job_id>")
    def delete_job(job_id):
        engine.remove_job(job_id)
        store.delete_job(job_id)
        return jsonify({"ok": True})

    @app.post("/api/jobs/<job_id>/run")
    def trigger_job(job_id):
        try:
            t = engine.trigger(job_id)
            if t is None:
                return jsonify({"ok": False, "message": "Already running"}), 409
            return jsonify({"ok": True, "message": "Started"})
        except KeyError:
            return jsonify({"error": "not found"}), 404

    @app.post("/api/jobs/<job_id>/reset")
    def reset_job_offset(job_id):
        body = request.json or {}
        table = body.get("table")
        jobs = engine.get_jobs()
        if job_id not in jobs:
            return jsonify({"error": "not found"}), 404
        tables = [table] if table else jobs[job_id].get("tables", [])
        for t in tables:
            engine.reset_offset(job_id, t)
        return jsonify({"ok": True, "reset": tables})

    @app.put("/api/jobs/<job_id>/enabled")
    def set_job_enabled(job_id):
        body = request.json or {}
        enabled = bool(body.get("enabled", True))
        store.set_job_enabled(job_id, enabled)
        return jsonify({"ok": True, "enabled": enabled})

    # ── Runs ──────────────────────────────────────────────────────────────────

    @app.get("/api/runs")
    def list_runs():
        job_id = request.args.get("job_id")
        limit  = int(request.args.get("limit", 100))
        return jsonify(store.get_runs(job_id, limit))

    @app.get("/api/jobs/<job_id>/runs")
    def job_runs(job_id):
        return jsonify(store.get_runs(job_id, limit=50))

    # ── Pipeline Overview ─────────────────────────────────────────────────────

    @app.get("/api/pipeline-overview")
    def pipeline_overview():
        target = store.get_target()
        target_host = target.get("host", "")
        target_catalog = target.get("catalog", "")
        target_schema  = target.get("schema", "")
        target_mode    = target.get("mode", "a")
        result = []
        for job_id, job_cfg in engine.get_jobs().items():
            runs = store.get_runs(job_id, limit=10)
            total = len(runs)
            succeeded = sum(1 for r in runs if r.get("status") == "success")
            last = runs[0] if runs else None
            result.append({
                "id": job_id,
                "name": job_cfg.get("name", job_id),
                "source_type": job_cfg.get("source_type", "unknown"),
                "source_label": _source_label(job_cfg),
                "tables": job_cfg.get("tables", []),
                "load_mode": job_cfg.get("load_mode", "incremental"),
                "schedule": job_cfg.get("schedule"),
                "enabled": job_cfg.get("enabled", True),
                "target_host": target_host,
                "target_catalog": target_catalog,
                "target_schema": target_schema,
                "target_mode": target_mode,
                "target_table": _target_table(job_cfg, target_catalog, target_schema),
                "last_run": last,
                "success_rate": round(succeeded / total, 2) if total else None,
                "total_runs": total,
            })
        return jsonify(result)

    # ── Health Summary ────────────────────────────────────────────────────────

    @app.get("/api/health/summary")
    def health_summary():
        jobs = engine.get_jobs()
        total_jobs = len(jobs)
        healthy = degraded = failing = never_run = 0
        job_health = []
        for job_id, job_cfg in jobs.items():
            runs = store.get_runs(job_id, limit=20)
            last = runs[0] if runs else None
            total = len(runs)
            succeeded = sum(1 for r in runs if r.get("status") == "success")
            rate = succeeded / total if total else None
            if not runs:
                status = "never_run"; never_run += 1
            elif last.get("status") == "error":
                status = "failing"; failing += 1
            elif rate is not None and rate >= 0.9:
                status = "healthy"; healthy += 1
            else:
                status = "degraded"; degraded += 1
            total_rows = sum(r.get("rows_written") or 0 for r in runs)
            durations  = [r.get("duration_s") for r in runs if r.get("duration_s")]
            avg_dur    = round(sum(durations) / len(durations), 1) if durations else None
            job_health.append({
                "id": job_id,
                "name": job_cfg.get("name", job_id),
                "source_type": job_cfg.get("source_type"),
                "schedule": job_cfg.get("schedule"),
                "enabled": job_cfg.get("enabled", True),
                "health": status,
                "success_rate": round(rate, 2) if rate is not None else None,
                "total_runs": total,
                "total_rows": total_rows,
                "avg_duration_s": avg_dur,
                "last_run": last,
                "recent_errors": [r.get("error") for r in runs if r.get("status") == "error"][:3],
            })
        return jsonify({
            "total_jobs": total_jobs,
            "healthy": healthy,
            "degraded": degraded,
            "failing": failing,
            "never_run": never_run,
            "jobs": job_health,
        })

    # ── Target (Dremio connection) ────────────────────────────────────────────

    @app.get("/api/target")
    def get_target():
        saved = store.get_target()
        # Redact secrets
        redacted = {**saved}
        if redacted.get("password") and not redacted["password"].startswith("${"):
            redacted["password"] = "***"
        if redacted.get("pat") and not redacted["pat"].startswith("${"):
            redacted["pat"] = "***"
        if redacted.get("token") and not redacted["token"].startswith("${"):
            redacted["token"] = "***"
        return jsonify(redacted)

    @app.put("/api/target")
    def save_target():
        body = request.json or {}
        existing = store.get_target()
        # Don't overwrite with placeholder ***
        for field in ("password", "pat", "token", "aws_secret_access_key"):
            if body.get(field) in ("***", None, ""):
                body[field] = existing.get(field, "")
        store.save_target(body)
        # Update live engine target config
        cfg["target"] = body
        return jsonify({"ok": True})

    @app.post("/api/target/test")
    def test_target():
        saved = store.get_target()
        try:
            from core.dremio_sink import DremioSink
            sink = DremioSink(saved)
            sink.connect()
            result = sink._sql("SELECT 1 AS ok")
            sink.close()
            return jsonify({"ok": True, "message": "Connected to Dremio"})
        except Exception as exc:
            return jsonify({"ok": False, "message": str(exc)})

    @app.get("/api/target/namespaces")
    def target_namespaces():
        saved = store.get_target()
        try:
            from core.dremio_sink import DremioSink
            sink = DremioSink(saved)
            sink.connect()
            result = sink._sql("SHOW SCHEMAS")
            sink.close()
            schemas = []
            if result and "rows" in result:
                for row in result["rows"]:
                    schemas.append(list(row.values())[0] if row else "")
            return jsonify(sorted(set(s for s in schemas if s)))
        except Exception as exc:
            return jsonify([])

    # ── Settings / Secrets ────────────────────────────────────────────────────

    @app.get("/api/settings/secrets")
    def get_secrets():
        raw = store.get_setting("vault_config")
        if raw:
            vault = json.loads(raw)
        else:
            vault = {"url": "", "auth_method": "token", "token": "", "role_id": "",
                     "secret_id": "", "namespace": "", "mount": "secret"}
        vault["token"]     = "***" if vault.get("token") else ""
        vault["secret_id"] = "***" if vault.get("secret_id") else ""
        return jsonify(vault)

    @app.put("/api/settings/secrets")
    def save_secrets():
        body = request.json or {}
        raw = store.get_setting("vault_config")
        existing = json.loads(raw) if raw else {}
        for k in ("url", "auth_method", "token", "role_id", "secret_id", "namespace", "mount"):
            v = body.get(k)
            if v is not None and v != "***":
                existing[k] = v
        store.set_setting("vault_config", json.dumps(existing))
        return jsonify({"ok": True})

    @app.post("/api/settings/secrets/test")
    def test_secrets():
        raw = store.get_setting("vault_config")
        vault_cfg = json.loads(raw) if raw else {}
        body = request.json or {}
        for k in ("url", "auth_method", "token", "role_id", "secret_id", "namespace", "mount"):
            v = body.get(k)
            if v and v != "***":
                vault_cfg[k] = v
        if not vault_cfg.get("url"):
            return jsonify({"ok": False, "message": "No Vault URL configured"})
        try:
            from core.secrets import VaultClient
            VaultClient(vault_cfg)
            return jsonify({"ok": True, "message": f"Connected to Vault at {vault_cfg['url']}"})
        except Exception as exc:
            return jsonify({"ok": False, "message": str(exc)})

    # ── Notifications ─────────────────────────────────────────────────────────

    NOTIF_KEYS = [
        "notify_email_enabled", "notify_email_smtp_host", "notify_email_smtp_port",
        "notify_email_smtp_user", "notify_email_smtp_pass", "notify_email_from",
        "notify_email_to", "notify_slack_enabled", "notify_slack_webhook_url",
    ]

    def _load_notif_settings() -> dict:
        raw = store.get_setting("notification_config")
        settings = json.loads(raw) if raw else {}
        # redact secrets for GET
        return settings

    # Push current settings into engine at startup
    from core.engine import set_notification_settings
    set_notification_settings(_load_notif_settings())

    @app.get("/api/settings/notifications")
    def get_notifications():
        s = _load_notif_settings()
        redacted = dict(s)
        if redacted.get("notify_email_smtp_pass"):
            redacted["notify_email_smtp_pass"] = "***"
        if redacted.get("notify_slack_webhook_url") and not redacted["notify_slack_webhook_url"].startswith("${"):
            pass  # webhook URL is not really secret, show it
        return jsonify(redacted)

    @app.put("/api/settings/notifications")
    def save_notifications():
        body = request.json or {}
        raw = store.get_setting("notification_config")
        existing = json.loads(raw) if raw else {}
        for k in NOTIF_KEYS:
            v = body.get(k)
            if v is None:
                continue
            if k == "notify_email_smtp_pass" and v == "***":
                continue
            existing[k] = v
        store.set_setting("notification_config", json.dumps(existing))
        set_notification_settings(existing)
        return jsonify({"ok": True})

    @app.post("/api/settings/notifications/test")
    def test_notifications():
        raw = store.get_setting("notification_config")
        settings = json.loads(raw) if raw else {}
        body = request.json or {}
        for k in NOTIF_KEYS:
            if k in body and body[k] != "***":
                settings[k] = body[k]
        from core.notifier import _send_sync
        try:
            _send_sync("test-job", "ok", "This is a test notification from Dremio Load.", settings)
            return jsonify({"ok": True, "message": "Test notification sent"})
        except Exception as exc:
            return jsonify({"ok": False, "message": str(exc)})

    # ── COPY INTO (Dremio-native) ─────────────────────────────────────────────

    @app.post("/api/copy-into/preview")
    def preview_copy_into():
        body = request.json or {}
        from core.copy_into import build_copy_into_sql
        try:
            sql = build_copy_into_sql(
                target_table=body.get("target_table", ""),
                source_location=body.get("source_location", ""),
                file_format=body.get("file_format", "parquet"),
                format_options=body.get("format_options"),
                pattern=body.get("pattern"),
            )
            return jsonify({"sql": sql})
        except Exception as exc:
            return jsonify({"error": str(exc)}), 400

    @app.post("/api/copy-into/run")
    def run_copy_into():
        body = request.json or {}
        from core.copy_into import CopyIntoJob
        import threading, uuid
        job_id = body.get("id") or str(uuid.uuid4())[:8]
        target = store.get_target()
        job = CopyIntoJob(job_id, body, target)

        def _run():
            result = job.run()
            logger.info("COPY INTO result: %s", result)

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        return jsonify({"ok": True, "job_id": job_id, "message": "COPY INTO started"})

    # ── Dremio Catalog Explorer ───────────────────────────────────────────────

    def _dremio_sink():
        from core.dremio_sink import DremioSink
        saved = store.get_target()
        sink = DremioSink(saved)
        sink.connect()
        return sink

    @app.get("/api/dremio/namespaces")
    def dremio_namespaces():
        try:
            sink = _dremio_sink()
            result = sink._sql("SHOW SCHEMAS")
            sink.close()
            schemas = []
            if result and "rows" in result:
                for row in result["rows"]:
                    v = list(row.values())[0] if row else ""
                    if v and not v.startswith("INFORMATION_SCHEMA") and v != "sys":
                        schemas.append(v)
            return jsonify(sorted(set(schemas)))
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    @app.get("/api/dremio/tables")
    def dremio_tables():
        ns = request.args.get("ns", "")
        if not ns:
            return jsonify([])
        try:
            sink = _dremio_sink()
            result = sink._sql(f'SHOW TABLES IN "{ns}"')
            sink.close()
            tables = []
            if result and "rows" in result:
                for row in result["rows"]:
                    vals = list(row.values())
                    tbl = vals[1] if len(vals) > 1 else (vals[0] if vals else "")
                    if tbl:
                        tables.append(tbl)
            return jsonify(sorted(set(tables)))
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    @app.get("/api/dremio/preview")
    def dremio_preview():
        table = request.args.get("table", "")
        limit = int(request.args.get("limit", 50))
        if not table:
            return jsonify({"error": "table required"}), 400
        try:
            sink = _dremio_sink()
            parts = table.split(".")
            quoted = ".".join(f'"{p}"' for p in parts)
            result = sink._sql(f"SELECT * FROM {quoted} LIMIT {limit}")
            sink.close()
            if not result:
                return jsonify({"columns": [], "rows": []})
            columns = [c["name"] for c in result.get("schema", {}).get("fields", [])]
            if not columns and result.get("rows"):
                columns = list(result["rows"][0].keys())
            return jsonify({"table": table, "columns": columns, "rows": result.get("rows", [])})
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    # ── Source Table Explorer ─────────────────────────────────────────────────

    def _source_list_tables(job_cfg: Dict) -> list:
        src_type = job_cfg.get("source_type", "")
        host = job_cfg.get("host", "localhost")
        port = job_cfg.get("port")
        user = job_cfg.get("username") or job_cfg.get("user", "")
        password = job_cfg.get("password", "")
        database = job_cfg.get("database", "")

        if src_type == "postgres":
            import psycopg2
            conn = psycopg2.connect(host=host, port=port or 5432, user=user, password=password, dbname=database)
            cur = conn.cursor()
            cur.execute("SELECT table_schema || '.' || table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema NOT IN ('pg_catalog','information_schema') ORDER BY 1")
            tables = [r[0] for r in cur.fetchall()]
            conn.close(); return tables

        elif src_type == "mysql":
            import pymysql
            conn = pymysql.connect(host=host, port=port or 3306, user=user, password=password, database=database)
            cur = conn.cursor()
            cur.execute("SHOW TABLES")
            tables = [r[0] for r in cur.fetchall()]
            conn.close(); return tables

        elif src_type == "sqlserver":
            import pyodbc
            cs = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host},{port or 1433};DATABASE={database};UID={user};PWD={password}"
            conn = pyodbc.connect(cs)
            cur = conn.cursor()
            cur.execute("SELECT TABLE_SCHEMA+'.'+TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' ORDER BY 1")
            tables = [r[0] for r in cur.fetchall()]
            conn.close(); return tables

        elif src_type == "oracle":
            import cx_Oracle
            dsn = cx_Oracle.makedsn(host, port or 1521, service_name=job_cfg.get("service_name", database))
            conn = cx_Oracle.connect(user=user, password=password, dsn=dsn)
            cur = conn.cursor()
            cur.execute("SELECT owner||'.'||table_name FROM all_tables WHERE owner NOT IN ('SYS','SYSTEM') ORDER BY 1")
            tables = [r[0] for r in cur.fetchall()]
            conn.close(); return tables

        elif src_type == "mongodb":
            from pymongo import MongoClient
            uri = job_cfg.get("connection_string") or f"mongodb://{user}:{password}@{host}:{port or 27017}/{database}"
            client = MongoClient(uri, serverSelectionTimeoutMS=10000)
            db = client[database]
            tables = sorted(db.list_collection_names())
            client.close(); return tables

        else:
            # File/cloud sources — return configured tables
            return job_cfg.get("tables") or []

    def _source_preview_table(job_cfg: Dict, table: str, limit: int = 50):
        src_type = job_cfg.get("source_type", "")
        host = job_cfg.get("host", "localhost")
        port = job_cfg.get("port")
        user = job_cfg.get("username") or job_cfg.get("user", "")
        password = job_cfg.get("password", "")
        database = job_cfg.get("database", "")

        if src_type == "postgres":
            import psycopg2, psycopg2.extras
            conn = psycopg2.connect(host=host, port=port or 5432, user=user, password=password, dbname=database)
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(f"SELECT * FROM {table} LIMIT %s", (limit,))
            rows = [dict(r) for r in cur.fetchall()]
            columns = list(rows[0].keys()) if rows else []
            conn.close(); return columns, rows

        elif src_type == "mysql":
            import pymysql
            conn = pymysql.connect(host=host, port=port or 3306, user=user, password=password, database=database, cursorclass=pymysql.cursors.DictCursor)
            cur = conn.cursor()
            cur.execute(f"SELECT * FROM {table} LIMIT %s", (limit,))
            rows = cur.fetchall()
            columns = list(rows[0].keys()) if rows else []
            conn.close(); return columns, rows

        elif src_type == "mongodb":
            from pymongo import MongoClient
            uri = job_cfg.get("connection_string") or f"mongodb://{user}:{password}@{host}:{port or 27017}/{database}"
            client = MongoClient(uri, serverSelectionTimeoutMS=10000)
            db = client[database]
            docs = list(db[table].find({}, {'_id': 0}).limit(limit))
            columns = list(docs[0].keys()) if docs else []
            rows = [{k: str(v) for k, v in doc.items()} for doc in docs]
            client.close(); return columns, rows

        else:
            raise ValueError(f"Preview not supported for source type: {src_type}")

    @app.get("/api/source/tables")
    def source_tables():
        job_id = request.args.get("job_id", "")
        jobs = engine.get_jobs()
        if job_id not in jobs:
            return jsonify({"error": "Job not found"}), 404
        try:
            return jsonify(_source_list_tables(jobs[job_id]))
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    @app.get("/api/source/preview")
    def source_preview():
        job_id = request.args.get("job_id", "")
        table = request.args.get("table", "")
        limit = int(request.args.get("limit", 50))
        jobs = engine.get_jobs()
        if job_id not in jobs:
            return jsonify({"error": "Job not found"}), 404
        try:
            columns, rows = _source_preview_table(jobs[job_id], table, limit)
            return jsonify({"table": table, "columns": columns, "rows": rows})
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    # ── Agent Settings ────────────────────────────────────────────────────────

    AGENT_KEYS = ["agent_enabled", "agent_model", "anthropic_api_key"]

    def _load_agent_settings() -> dict:
        raw = store.get_setting("agent_config")
        return json.loads(raw) if raw else {"agent_enabled": False, "agent_model": "claude-opus-4-7", "anthropic_api_key": ""}

    @app.get("/api/settings/agent")
    def get_agent_settings():
        s = _load_agent_settings()
        redacted = dict(s)
        if redacted.get("anthropic_api_key"):
            redacted["anthropic_api_key"] = "***"
        return jsonify(redacted)

    @app.put("/api/settings/agent")
    def save_agent_settings():
        body = request.json or {}
        existing = _load_agent_settings()
        for k in AGENT_KEYS:
            v = body.get(k)
            if v is None:
                continue
            if k == "anthropic_api_key" and v == "***":
                continue
            existing[k] = v
        store.set_setting("agent_config", json.dumps(existing))
        return jsonify({"ok": True})

    # ── Agent Chat ────────────────────────────────────────────────────────────

    _AGENT_TOOLS = [
        {
            "name": "list_jobs",
            "description": "List all configured data load jobs. Returns job names, source types, schedules, and last run status.",
            "input_schema": {"type": "object", "properties": {}, "required": []},
        },
        {
            "name": "get_job",
            "description": "Get full configuration for a specific job by its ID.",
            "input_schema": {
                "type": "object",
                "properties": {"job_id": {"type": "string", "description": "The job ID to look up"}},
                "required": ["job_id"],
            },
        },
        {
            "name": "create_job",
            "description": "Create a new data load job. Requires name, source_type, and connection details.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "source_type": {"type": "string", "description": "s3, gcs, azure_blob, postgres, mysql, sqlserver, oracle, mongodb, snowflake"},
                    "load_mode": {"type": "string", "description": "incremental, full, or ctas"},
                    "schedule": {"type": "string", "description": "Cron expression, e.g. 0 */6 * * *"},
                    "tables": {"type": "array", "items": {"type": "string"}},
                    "connection": {"type": "object", "description": "Source connection parameters (host, port, database, username, password, bucket, etc.)"},
                },
                "required": ["name", "source_type"],
            },
        },
        {
            "name": "trigger_job",
            "description": "Trigger an immediate run of a job.",
            "input_schema": {
                "type": "object",
                "properties": {"job_id": {"type": "string"}},
                "required": ["job_id"],
            },
        },
        {
            "name": "get_health_summary",
            "description": "Get health and run statistics for all jobs.",
            "input_schema": {"type": "object", "properties": {}, "required": []},
        },
        {
            "name": "list_dremio_namespaces",
            "description": "List available schemas/namespaces in the Dremio target.",
            "input_schema": {"type": "object", "properties": {}, "required": []},
        },
        {
            "name": "list_dremio_tables",
            "description": "List tables in a specific Dremio namespace/schema.",
            "input_schema": {
                "type": "object",
                "properties": {"namespace": {"type": "string", "description": "The namespace/schema to list tables in"}},
                "required": ["namespace"],
            },
        },
        {
            "name": "get_target_info",
            "description": "Get the Dremio target connection information (credentials are redacted).",
            "input_schema": {"type": "object", "properties": {}, "required": []},
        },
    ]

    def _run_agent_tool(name: str, tool_input: dict) -> str:
        try:
            if name == "list_jobs":
                db_jobs = {j["id"]: j for j in store.get_jobs()}
                result = []
                for jid, jcfg in engine.get_jobs().items():
                    db = db_jobs.get(jid, {})
                    runs = store.get_runs(jid, limit=1)
                    last = runs[0] if runs else None
                    result.append({
                        "id": jid, "name": jcfg.get("name", jid),
                        "source_type": jcfg.get("source_type"),
                        "load_mode": jcfg.get("load_mode", "incremental"),
                        "schedule": jcfg.get("schedule"),
                        "enabled": bool(db.get("enabled", 1)),
                        "last_status": last.get("status") if last else None,
                        "last_run_at": last.get("started_at") if last else None,
                    })
                return json.dumps(result)

            elif name == "get_job":
                jid = tool_input["job_id"]
                jobs = engine.get_jobs()
                if jid not in jobs:
                    return json.dumps({"error": f"Job '{jid}' not found"})
                cfg_copy = dict(jobs[jid])
                # redact passwords
                if "connection" in cfg_copy:
                    conn = dict(cfg_copy["connection"])
                    for k in ("password", "aws_secret_access_key", "client_secret", "credentials_file"):
                        if conn.get(k):
                            conn[k] = "***"
                    cfg_copy["connection"] = conn
                return json.dumps(cfg_copy)

            elif name == "create_job":
                job_id = tool_input.get("name", "").lower().replace(" ", "_")
                body = {
                    "id": job_id,
                    "name": tool_input.get("name"),
                    "source_type": tool_input.get("source_type"),
                    "load_mode": tool_input.get("load_mode", "incremental"),
                    "schedule": tool_input.get("schedule"),
                    "tables": tool_input.get("tables", []),
                    "connection": tool_input.get("connection", {}),
                }
                engine.add_job(job_id, body)
                store.upsert_job(job_id, body["name"], body)
                return json.dumps({"ok": True, "job_id": job_id, "message": f"Job '{body['name']}' created successfully"})

            elif name == "trigger_job":
                jid = tool_input["job_id"]
                try:
                    t = engine.trigger(jid)
                    if t is None:
                        return json.dumps({"ok": False, "message": "Job is already running"})
                    return json.dumps({"ok": True, "message": f"Job '{jid}' triggered successfully"})
                except KeyError:
                    return json.dumps({"error": f"Job '{jid}' not found"})

            elif name == "get_health_summary":
                jobs = engine.get_jobs()
                summary = {"total_jobs": len(jobs), "healthy": 0, "degraded": 0, "failing": 0, "never_run": 0, "jobs": []}
                for jid, jcfg in jobs.items():
                    runs = store.get_runs(jid, limit=10)
                    last = runs[0] if runs else None
                    total = len(runs)
                    succeeded = sum(1 for r in runs if r.get("status") == "success")
                    rate = succeeded / total if total else None
                    if not runs:
                        status = "never_run"; summary["never_run"] += 1
                    elif last.get("status") == "error":
                        status = "failing"; summary["failing"] += 1
                    elif rate is not None and rate >= 0.9:
                        status = "healthy"; summary["healthy"] += 1
                    else:
                        status = "degraded"; summary["degraded"] += 1
                    summary["jobs"].append({"id": jid, "name": jcfg.get("name", jid), "health": status,
                                             "success_rate": round(rate, 2) if rate is not None else None,
                                             "last_status": last.get("status") if last else None})
                return json.dumps(summary)

            elif name == "list_dremio_namespaces":
                try:
                    sink = _dremio_sink()
                    result = sink._sql("SHOW SCHEMAS")
                    sink.close()
                    schemas = []
                    if result and "rows" in result:
                        for row in result["rows"]:
                            v = list(row.values())[0] if row else ""
                            if v and not v.startswith("INFORMATION_SCHEMA") and v != "sys":
                                schemas.append(v)
                    return json.dumps(sorted(set(schemas)))
                except Exception as exc:
                    return json.dumps({"error": str(exc)})

            elif name == "list_dremio_tables":
                ns = tool_input.get("namespace", "")
                try:
                    sink = _dremio_sink()
                    result = sink._sql(f'SHOW TABLES IN "{ns}"')
                    sink.close()
                    tables = []
                    if result and "rows" in result:
                        for row in result["rows"]:
                            vals = list(row.values())
                            tbl = vals[1] if len(vals) > 1 else (vals[0] if vals else "")
                            if tbl:
                                tables.append(tbl)
                    return json.dumps(sorted(set(tables)))
                except Exception as exc:
                    return json.dumps({"error": str(exc)})

            elif name == "get_target_info":
                t = store.get_target()
                redacted = {k: ("***" if k in ("password", "pat", "token") and t.get(k) else v) for k, v in t.items()}
                return json.dumps(redacted)

            else:
                return json.dumps({"error": f"Unknown tool: {name}"})

        except Exception as exc:
            return json.dumps({"error": str(exc)})

    _AGENT_SYSTEM = """You are the Dremio Load Assistant — a friendly, helpful AI that guides users through loading data into their Dremio lakehouse.

You have access to tools that let you:
- See what load jobs are configured and their health
- View the Dremio target connection and available schemas/tables
- Create new load jobs
- Trigger job runs

Guidelines:
- Be concise and friendly. Avoid technical jargon unless the user is clearly technical.
- Before creating a job or triggering a run, always confirm the key details with the user.
- When listing information, use clear formatting (bullet points, short tables).
- If something fails, explain what went wrong in plain language and suggest what to try.
- When a user wants to load data, guide them step by step: source type → connection details → tables → schedule → create.
- Source types: s3 (Amazon S3), gcs (Google Cloud Storage), azure_blob (Azure Blob), postgres, mysql, sqlserver, oracle, mongodb, snowflake."""

    @app.post("/api/agent/chat")
    def agent_chat():
        agent_cfg = _load_agent_settings()
        if not agent_cfg.get("agent_enabled"):
            return jsonify({"error": "AI Agent is not enabled. Enable it in Settings → AI Agent."}), 403

        api_key = agent_cfg.get("anthropic_api_key", "")
        if not api_key:
            return jsonify({"error": "No Anthropic API key configured. Add it in Settings → AI Agent."}), 400

        body = request.json or {}
        messages = body.get("messages", [])
        if not messages:
            return jsonify({"error": "messages required"}), 400

        try:
            import anthropic
            client = anthropic.Anthropic(api_key=api_key)
            model = agent_cfg.get("agent_model", "claude-opus-4-7")

            # Agentic loop — run until no more tool calls
            MAX_ITERATIONS = 8
            for _ in range(MAX_ITERATIONS):
                response = client.messages.create(
                    model=model,
                    max_tokens=4096,
                    system=_AGENT_SYSTEM,
                    tools=_AGENT_TOOLS,
                    messages=messages,
                )

                # Append assistant turn
                assistant_content = [c.model_dump() if hasattr(c, "model_dump") else dict(c) for c in response.content]
                messages = messages + [{"role": "assistant", "content": assistant_content}]

                if response.stop_reason != "tool_use":
                    break

                # Execute tools and build user tool_result turn
                tool_results = []
                for block in response.content:
                    if block.type == "tool_use":
                        result_str = _run_agent_tool(block.name, block.input)
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": result_str,
                        })

                if tool_results:
                    messages = messages + [{"role": "user", "content": tool_results}]

            return jsonify({"messages": messages})

        except Exception as exc:
            logger.error("Agent chat error: %s", exc, exc_info=True)
            return jsonify({"error": str(exc)}), 500

    # ── Schedule ──────────────────────────────────────────────────────────────

    @app.get("/api/schedule")
    def get_schedule():
        from croniter import croniter
        from datetime import datetime as _dt, timezone as _tz
        now = _dt.now(_tz.utc)
        result = []
        for job_id, job_cfg in engine.get_jobs().items():
            db_jobs = {j["id"]: j for j in store.get_jobs()}
            db = db_jobs.get(job_id, {})
            cron = job_cfg.get("schedule")
            next_run = prev_run = None
            if cron:
                try:
                    it = croniter(cron, now)
                    next_run = it.get_next(_dt).isoformat()
                    it2 = croniter(cron, now)
                    prev_run = it2.get_prev(_dt).isoformat()
                except Exception:
                    pass
            runs = store.get_runs(job_id, limit=1)
            last = runs[0] if runs else None
            result.append({
                "id": job_id,
                "name": job_cfg.get("name", job_id),
                "source_type": job_cfg.get("source_type"),
                "schedule": cron,
                "enabled": bool(db.get("enabled", 1)),
                "next_run": next_run,
                "prev_run": prev_run,
                "load_mode": job_cfg.get("load_mode", "incremental"),
                "last_status": last.get("status") if last else None,
                "last_run_at": last.get("started_at") if last else None,
                "running": job_id in engine._running and engine._running[job_id].is_alive(),
            })
        # Sort by next_run ascending
        result.sort(key=lambda x: x.get("next_run") or "9999")
        return jsonify(result)

    @app.put("/api/schedule/<job_id>")
    def update_schedule(job_id):
        body = request.json or {}
        jobs = engine.get_jobs()
        if job_id not in jobs:
            return jsonify({"error": "not found"}), 404
        job_cfg = dict(jobs[job_id])
        if "schedule" in body:
            job_cfg["schedule"] = body["schedule"]
        if "enabled" in body:
            store.set_job_enabled(job_id, bool(body["enabled"]))
        engine.add_job(job_id, job_cfg)
        store.upsert_job(job_id, job_cfg.get("name", job_id), job_cfg)
        return jsonify({"ok": True})

    # ── Google Ads OAuth ──────────────────────────────────────────────────────

    _gads_pending: dict = {}   # state -> {client_id, client_secret}
    _gads_done: dict    = {}   # state -> {refresh_token, email}

    GADS_REDIRECT = "http://localhost:7071/api/oauth/google-ads/callback"
    GADS_SCOPE    = "https://www.googleapis.com/auth/adwords"

    @app.post("/api/oauth/google-ads/start")
    def google_ads_oauth_start():
        body = request.json or {}
        client_id     = body.get("client_id", "").strip()
        client_secret = body.get("client_secret", "").strip()
        if not client_id or not client_secret:
            return jsonify({"error": "client_id and client_secret are required"}), 400
        state = _secrets.token_urlsafe(16)
        _gads_pending[state] = {"client_id": client_id, "client_secret": client_secret}
        params = urllib.parse.urlencode({
            "client_id":     client_id,
            "response_type": "code",
            "scope":         GADS_SCOPE,
            "redirect_uri":  GADS_REDIRECT,
            "access_type":   "offline",
            "prompt":        "consent",
            "state":         state,
        })
        return jsonify({"auth_url": f"https://accounts.google.com/o/oauth2/auth?{params}", "state": state})

    @app.get("/api/oauth/google-ads/callback")
    def google_ads_oauth_callback():
        code  = request.args.get("code", "")
        state = request.args.get("state", "")
        error = request.args.get("error", "")

        def _html(title, color, msg, close=False):
            script = "<script>setTimeout(()=>window.close(),2000)</script>" if close else ""
            return f"""<html><head><title>{title}</title></head>
<body style="font-family:-apple-system,sans-serif;text-align:center;padding:60px;background:#0f172a;color:#e2e8f0">
<div style="font-size:48px;margin-bottom:16px">{"✓" if close else "✗"}</div>
<h2 style="color:{color};margin:0 0 8px">{title}</h2>
<p style="color:#94a3b8">{msg}</p>{script}
</body></html>"""

        if error or not code or state not in _gads_pending:
            return _html("Authorization failed", "#ef4444", error or "Invalid state. Please try again."), 400

        ctx = _gads_pending.pop(state)
        token_body = urllib.parse.urlencode({
            "code":          code,
            "client_id":     ctx["client_id"],
            "client_secret": ctx["client_secret"],
            "redirect_uri":  GADS_REDIRECT,
            "grant_type":    "authorization_code",
        }).encode()

        try:
            req = urllib.request.Request("https://oauth2.googleapis.com/token", data=token_body, method="POST")
            with urllib.request.urlopen(req) as resp:
                data = json.loads(resp.read())
        except Exception as exc:
            return _html("Token exchange failed", "#ef4444", str(exc)), 500

        refresh_token = data.get("refresh_token", "")
        email = ""
        try:
            id_token = data.get("id_token", "")
            if id_token:
                segment = id_token.split(".")[1]
                segment += "=" * (4 - len(segment) % 4)
                email = json.loads(base64.b64decode(segment)).get("email", "")
        except Exception:
            pass

        _gads_done[state] = {"refresh_token": refresh_token, "email": email}
        return _html("Connected to Google Ads!", "#34d399",
                     f"Authorized as {email or 'your Google account'}. You can close this window.", close=True)

    @app.get("/api/oauth/google-ads/result/<state>")
    def google_ads_oauth_result(state):
        if state in _gads_done:
            return jsonify(_gads_done.pop(state))
        return jsonify({"pending": True})

    # ── Static SPA ────────────────────────────────────────────────────────────

    @app.get("/")
    @app.get("/<path:path>")
    def spa(path=""):
        dist = FRONTEND_DIST
        if os.path.isdir(dist):
            target = os.path.join(dist, path)
            if path and os.path.isfile(target):
                return send_from_directory(dist, path)
            return send_from_directory(dist, "index.html")
        return "<h2>Dremio Load UI</h2><p>Frontend not built yet.</p>", 200

    return app
