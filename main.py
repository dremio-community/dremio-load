"""
Dremio Load — entry point.
Loads config, starts the load engine and scheduler, launches the UI server.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import threading

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def load_config(path: str) -> dict:
    import yaml
    from core.secrets import build_resolver
    with open(path) as f:
        raw = yaml.safe_load(f)
    resolver = build_resolver(raw)
    return resolver.walk(raw)


def main():
    parser = argparse.ArgumentParser(description="Dremio Load — batch ingestion engine")
    parser.add_argument("--config", default="config.yml", help="Path to config YAML")
    parser.add_argument("--port",   default=int(os.getenv("UI_PORT", "7071")), type=int)
    parser.add_argument("--no-ui",  action="store_true", help="Run engine only (no web UI)")
    args = parser.parse_args()

    if not os.path.exists(args.config):
        logger.error("Config file not found: %s", args.config)
        sys.exit(1)

    cfg = load_config(args.config)

    from core.engine import LoadEngine
    engine = LoadEngine(cfg)

    # Register all jobs from config
    for job_cfg in cfg.get("jobs", []):
        engine.add_job(job_cfg["id"], job_cfg)

    # Start cron scheduler
    from ui.backend.scheduler import LoadScheduler
    scheduler = LoadScheduler(engine)
    sched_thread = threading.Thread(target=scheduler.run, daemon=True, name="scheduler")
    sched_thread.start()
    logger.info("Scheduler started")

    if args.no_ui:
        logger.info("Engine running (no UI). Press Ctrl+C to stop.")
        try:
            while True:
                import time; time.sleep(60)
        except KeyboardInterrupt:
            pass
        return

    # Start Flask UI
    from ui.backend.app import create_app
    app = create_app(engine, cfg)
    logger.info("UI starting on http://0.0.0.0:%d", args.port)
    app.run(host="0.0.0.0", port=args.port, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
