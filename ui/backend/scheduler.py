"""Cron-based scheduler — triggers load jobs on their configured schedule."""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from croniter import croniter

logger = logging.getLogger(__name__)


class LoadScheduler:
    def __init__(self, engine):
        self._engine = engine
        self._stop   = False

    def run(self):
        while not self._stop:
            now = datetime.now(timezone.utc)
            for job_id, job_cfg in self._engine.get_jobs().items():
                cron = job_cfg.get("schedule")
                if not cron:
                    continue
                try:
                    it = croniter(cron, now)
                    prev = it.get_prev(datetime)
                    # Fire if the previous slot was within the last 60 seconds
                    if (now - prev).total_seconds() <= 60:
                        logger.info("[scheduler] Triggering job %s", job_id)
                        self._engine.trigger(job_id)
                except Exception as exc:
                    logger.warning("[scheduler] Error checking job %s: %s", job_id, exc)
            time.sleep(60)

    def stop(self):
        self._stop = True
