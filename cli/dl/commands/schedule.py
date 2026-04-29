"""dl schedule — view and edit job schedules."""
from __future__ import annotations

from typing import Optional

import typer

from ..client import DLClient
from .. import output

app = typer.Typer(help="Manage job schedules")


@app.command("list")
def list_schedules(
    url: Optional[str] = typer.Option(None, envvar="DL_URL"),
):
    """List all jobs with their cron schedules and next run times."""
    data = DLClient(url).get("/api/schedule")
    output.out(data)


@app.command("set")
def set_schedule(
    job_id: str = typer.Argument(..., help="Job ID"),
    cron: str = typer.Argument(..., help="Cron expression e.g. '0 * * * *'"),
    url: Optional[str] = typer.Option(None, envvar="DL_URL"),
):
    """Set or update a job's cron schedule."""
    DLClient(url).put(f"/api/schedule/{job_id}", {"schedule": cron})
    output.success(f"Schedule for '{job_id}' set to: {cron}")


@app.command("clear")
def clear_schedule(
    job_id: str = typer.Argument(..., help="Job ID"),
    url: Optional[str] = typer.Option(None, envvar="DL_URL"),
):
    """Remove a job's schedule (make it manual-only)."""
    DLClient(url).put(f"/api/schedule/{job_id}", {"schedule": None})
    output.success(f"Schedule cleared for '{job_id}'")
