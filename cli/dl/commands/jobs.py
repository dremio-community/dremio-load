"""dl jobs — list, trigger, reset, enable/disable load jobs."""
from __future__ import annotations

import typer
from typing import Optional

from ..client import DLClient
from .. import output

app = typer.Typer(help="Manage load jobs")


@app.command("list")
def list_jobs(
    url: Optional[str] = typer.Option(None, envvar="DL_URL"),
):
    """List all configured load jobs."""
    data = DLClient(url).get("/api/jobs")
    output.out(data)


@app.command("get")
def get_job(
    job_id: str = typer.Argument(..., help="Job ID"),
    url: Optional[str] = typer.Option(None, envvar="DL_URL"),
):
    """Get full config for a job."""
    data = DLClient(url).get(f"/api/jobs/{job_id}")
    output.out(data)


@app.command("run")
def run_job(
    job_id: str = typer.Argument(..., help="Job ID to trigger"),
    url: Optional[str] = typer.Option(None, envvar="DL_URL"),
):
    """Trigger an immediate run of a job."""
    data = DLClient(url).post(f"/api/jobs/{job_id}/run")
    if data and not data.get("ok"):
        output.error(data.get("message", "Failed to start job"))
    output.success(f"Job '{job_id}' started")


@app.command("enable")
def enable_job(
    job_id: str = typer.Argument(..., help="Job ID"),
    url: Optional[str] = typer.Option(None, envvar="DL_URL"),
):
    """Enable a job."""
    DLClient(url).put(f"/api/jobs/{job_id}/enabled", {"enabled": True})
    output.success(f"Job '{job_id}' enabled")


@app.command("disable")
def disable_job(
    job_id: str = typer.Argument(..., help="Job ID"),
    url: Optional[str] = typer.Option(None, envvar="DL_URL"),
):
    """Disable a job (skips scheduled runs)."""
    DLClient(url).put(f"/api/jobs/{job_id}/enabled", {"enabled": False})
    output.success(f"Job '{job_id}' disabled")


@app.command("reset")
def reset_job(
    job_id: str = typer.Argument(..., help="Job ID"),
    table: Optional[str] = typer.Option(None, "--table", "-t", help="Specific table to reset (default: all)"),
    url: Optional[str] = typer.Option(None, envvar="DL_URL"),
):
    """Reset offset so next run does a full reload."""
    body = {"table": table} if table else {}
    data = DLClient(url).post(f"/api/jobs/{job_id}/reset", body)
    output.success(f"Offset reset for '{job_id}'" + (f" / {table}" if table else " (all tables)"))


@app.command("delete")
def delete_job(
    job_id: str = typer.Argument(..., help="Job ID"),
    url: Optional[str] = typer.Option(None, envvar="DL_URL"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
):
    """Delete a job."""
    if not yes:
        typer.confirm(f"Delete job '{job_id}'?", abort=True)
    DLClient(url).delete(f"/api/jobs/{job_id}")
    output.success(f"Job '{job_id}' deleted")
