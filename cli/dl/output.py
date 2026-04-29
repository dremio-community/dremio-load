"""Output formatting — JSON by default, rich tables with --pretty."""
from __future__ import annotations

import json
import sys
from typing import Any

from rich.console import Console
from rich.table import Table

console = Console()
err_console = Console(stderr=True)

_pretty = False


def set_pretty(value: bool) -> None:
    global _pretty
    _pretty = value


def out(data: Any) -> None:
    if _pretty and isinstance(data, list) and data and isinstance(data[0], dict):
        _print_table(data)
    elif _pretty and isinstance(data, dict):
        _print_dict(data)
    else:
        print(json.dumps(data, indent=2, default=str))


def success(msg: str) -> None:
    if _pretty:
        console.print(f"[green]✓[/green] {msg}")
    else:
        print(json.dumps({"status": "ok", "message": msg}))


def error(msg: str) -> None:
    if _pretty:
        err_console.print(f"[red]✗[/red] {msg}")
    else:
        print(json.dumps({"status": "error", "message": msg}), file=sys.stderr)
    sys.exit(1)


def _print_table(rows: list[dict]) -> None:
    if not rows:
        console.print("[dim]No results[/dim]")
        return
    t = Table(show_header=True, header_style="bold cyan")
    cols = list(rows[0].keys())
    for c in cols:
        t.add_column(str(c))
    for row in rows:
        t.add_row(*[str(row.get(c, "")) for c in cols])
    console.print(t)


def _print_dict(d: dict) -> None:
    t = Table(show_header=False, box=None, padding=(0, 1))
    t.add_column("key", style="bold cyan")
    t.add_column("value")
    for k, v in d.items():
        t.add_row(str(k), str(v) if not isinstance(v, (dict, list)) else json.dumps(v, default=str))
    console.print(t)
