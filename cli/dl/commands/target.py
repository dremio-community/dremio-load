"""dl target — view and update the Dremio target connection."""
from __future__ import annotations

from typing import Optional

import typer

from ..client import DLClient
from .. import output

app = typer.Typer(help="Manage Dremio target connection")


@app.command("get")
def get_target(
    url: Optional[str] = typer.Option(None, envvar="DL_URL"),
):
    """Show the current Dremio target configuration (secrets redacted)."""
    data = DLClient(url).get("/api/target")
    output.out(data)


@app.command("test")
def test_target(
    url: Optional[str] = typer.Option(None, envvar="DL_URL"),
):
    """Test the Dremio target connection."""
    data = DLClient(url).post("/api/target/test")
    if data and data.get("ok"):
        output.success(data.get("message", "Connected"))
    else:
        output.error(data.get("message", "Connection failed") if data else "Connection failed")


@app.command("namespaces")
def list_namespaces(
    url: Optional[str] = typer.Option(None, envvar="DL_URL"),
):
    """List available schemas/namespaces in the Dremio target."""
    data = DLClient(url).get("/api/dremio/namespaces")
    output.out(data)


@app.command("tables")
def list_tables(
    namespace: str = typer.Argument(..., help="Namespace/schema to list tables in"),
    url: Optional[str] = typer.Option(None, envvar="DL_URL"),
):
    """List tables in a Dremio namespace."""
    data = DLClient(url).get("/api/dremio/tables", ns=namespace)
    output.out(data)
