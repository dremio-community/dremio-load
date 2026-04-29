"""dl health — job health summary."""
from __future__ import annotations

from typing import Optional

import typer

from ..client import DLClient
from .. import output

app = typer.Typer(help="Health and status")


@app.command()
def health(
    url: Optional[str] = typer.Option(None, envvar="DL_URL"),
):
    """Show health summary for all jobs."""
    data = DLClient(url).get("/api/health/summary")
    output.out(data)
