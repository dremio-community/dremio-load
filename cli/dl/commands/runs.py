"""dl runs — view job run history."""
from __future__ import annotations

from typing import Optional

import typer

from ..client import DLClient
from .. import output

app = typer.Typer(help="View run history")


@app.command("list")
def list_runs(
    job_id: Optional[str] = typer.Option(None, "--job", "-j", help="Filter by job ID"),
    limit: int = typer.Option(50, "--limit", "-n", help="Max runs to return"),
    url: Optional[str] = typer.Option(None, envvar="DL_URL"),
):
    """List recent job runs."""
    data = DLClient(url).get("/api/runs", job_id=job_id, limit=limit)
    output.out(data)
