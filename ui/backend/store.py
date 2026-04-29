"""SQLite persistence for jobs, runs, and settings."""
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


class LoadStore:
    def __init__(self, db_path: str = "./load_ui.db"):
        self._path = db_path
        self._lock = threading.Lock()
        self._init()

    def _conn(self):
        conn = sqlite3.connect(self._path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init(self):
        with self._conn() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    config_json TEXT NOT NULL,
                    enabled INTEGER DEFAULT 1,
                    created_at TEXT,
                    updated_at TEXT
                );
                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    table_name TEXT,
                    status TEXT,
                    rows INTEGER DEFAULT 0,
                    error TEXT,
                    started_at TEXT,
                    finished_at TEXT,
                    duration_s REAL
                );
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );
            """)

    # ── Jobs ───────────────────────────────────────────────────────────────────

    def upsert_job(self, job_id: str, name: str, config: dict, enabled: bool = True):
        now = datetime.now(timezone.utc).isoformat()
        with self._lock, self._conn() as db:
            existing = db.execute("SELECT id FROM jobs WHERE id=?", (job_id,)).fetchone()
            if existing:
                db.execute(
                    "UPDATE jobs SET name=?, config_json=?, enabled=?, updated_at=? WHERE id=?",
                    (name, json.dumps(config), int(enabled), now, job_id)
                )
            else:
                db.execute(
                    "INSERT INTO jobs (id, name, config_json, enabled, created_at, updated_at) VALUES (?,?,?,?,?,?)",
                    (job_id, name, json.dumps(config), int(enabled), now, now)
                )

    def get_jobs(self) -> List[Dict]:
        with self._conn() as db:
            rows = db.execute("SELECT * FROM jobs ORDER BY created_at").fetchall()
        return [dict(r) for r in rows]

    def get_job(self, job_id: str) -> Optional[Dict]:
        with self._conn() as db:
            row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return dict(row) if row else None

    def delete_job(self, job_id: str):
        with self._lock, self._conn() as db:
            db.execute("DELETE FROM jobs WHERE id=?", (job_id,))

    def set_job_enabled(self, job_id: str, enabled: bool):
        now = datetime.now(timezone.utc).isoformat()
        with self._lock, self._conn() as db:
            db.execute("UPDATE jobs SET enabled=?, updated_at=? WHERE id=?",
                       (int(enabled), now, job_id))

    # ── Runs ───────────────────────────────────────────────────────────────────

    def save_run(self, run) -> str:
        import uuid
        run_id = str(uuid.uuid4())
        with self._lock, self._conn() as db:
            db.execute(
                "INSERT INTO runs (id, job_id, table_name, status, rows, error, started_at, finished_at, duration_s) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (run_id, run.job_id, run.table, run.status, run.rows, run.error,
                 run.started.isoformat() if run.started else None,
                 run.finished.isoformat() if run.finished else None,
                 run.duration_s)
            )
        return run_id

    def get_runs(self, job_id: str = None, limit: int = 200) -> List[Dict]:
        with self._conn() as db:
            if job_id:
                rows = db.execute(
                    "SELECT * FROM runs WHERE job_id=? ORDER BY started_at DESC LIMIT ?",
                    (job_id, limit)
                ).fetchall()
            else:
                rows = db.execute(
                    "SELECT * FROM runs ORDER BY started_at DESC LIMIT ?", (limit,)
                ).fetchall()
        return [dict(r) for r in rows]

    # ── Settings ───────────────────────────────────────────────────────────────

    def get_setting(self, key: str) -> Optional[str]:
        with self._conn() as db:
            row = db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else None

    def set_setting(self, key: str, value: str):
        with self._lock, self._conn() as db:
            db.execute(
                "INSERT INTO settings (key, value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value)
            )

    def get_target(self) -> Dict:
        raw = self.get_setting("target_config")
        if raw:
            return json.loads(raw)
        return {}

    def save_target(self, cfg: Dict):
        self.set_setting("target_config", json.dumps(cfg))
