"""dl context — full state dump for AI agents."""
from __future__ import annotations

from typing import Optional

import typer

from ..client import DLClient
from .. import output

app = typer.Typer(help="Context for AI agents")


@app.command("dump")
def dump(
    url: Optional[str] = typer.Option(None, envvar="DL_URL"),
):
    """Dump complete Dremio Load state for use by an AI agent.

    Fetches jobs, schedules, health summary, run history, and target
    info in parallel and returns a single JSON object with everything
    an agent needs to understand the current state.
    """
    client = DLClient(url)
    jobs, schedule, health, runs, target = client.get_parallel([
        "/api/jobs",
        "/api/schedule",
        "/api/health/summary",
        "/api/runs?limit=20",
        "/api/target",
    ])

    # Build schedule index for enrichment
    sched_by_id = {s["id"]: s for s in (schedule or [])}

    # Enrich jobs with schedule and health inline
    health_by_id = {j["id"]: j for j in (health or {}).get("jobs", [])}
    enriched_jobs = []
    for job in (jobs or []):
        jid = job["id"]
        enriched = dict(job)
        s = sched_by_id.get(jid, {})
        enriched["next_run"] = s.get("next_run")
        enriched["prev_run"] = s.get("prev_run")
        h = health_by_id.get(jid, {})
        enriched["health"] = h.get("health")
        enriched["success_rate"] = h.get("success_rate")
        enriched["total_runs"] = h.get("total_runs", 0)
        enriched["avg_duration_s"] = h.get("avg_duration_s")
        enriched_jobs.append(enriched)

    ctx = {
        "service": "dremio-load",
        "url": client.base,
        "jobs": enriched_jobs,
        "health_summary": {
            "total_jobs": (health or {}).get("total_jobs", 0),
            "healthy": (health or {}).get("healthy", 0),
            "degraded": (health or {}).get("degraded", 0),
            "failing": (health or {}).get("failing", 0),
            "never_run": (health or {}).get("never_run", 0),
        },
        "recent_runs": runs or [],
        "target": target or {},
    }
    output.out(ctx)
