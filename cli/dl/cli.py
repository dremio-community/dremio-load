"""dl — Dremio Load CLI.

Agent-first CLI for controlling the Dremio Load data ingestion engine.
JSON output by default; use --pretty for human-readable tables.

Configuration:
  DL_URL env var or --url flag (default: http://localhost:7071)

Quick start:
  dl jobs list                    # list all jobs
  dl jobs run <job-id>            # trigger a job now
  dl health                       # show health summary
  dl schedule list                # show schedules + next run times
  dl context dump                 # full state dump (for AI agents)
"""
from __future__ import annotations

from typing import Optional

import typer

from . import output
from .commands import jobs, runs, health, schedule, target, context

app = typer.Typer(
    name="dl",
    help="Dremio Load CLI — agent-first control of your data ingestion engine",
    no_args_is_help=True,
    add_completion=False,
)

# ── Global options ─────────────────────────────────────────────────────────────

_pretty_opt = typer.Option(False, "--pretty", "-p", help="Human-readable output (tables)", is_eager=True)


@app.callback()
def main(pretty: bool = _pretty_opt):
    output.set_pretty(pretty)


# ── Subcommand groups ──────────────────────────────────────────────────────────

app.add_typer(jobs.app,     name="jobs",     help="List, trigger, reset, enable/disable jobs")
app.add_typer(runs.app,     name="runs",     help="View run history")
app.add_typer(schedule.app, name="schedule", help="View and edit cron schedules")
app.add_typer(target.app,   name="target",   help="Dremio target connection")
app.add_typer(context.app,  name="context",  help="Full state dump for AI agents")


# ── Top-level shortcuts ────────────────────────────────────────────────────────

@app.command("health")
def health_cmd(
    url: Optional[str] = typer.Option(None, envvar="DL_URL"),
    pretty: bool = _pretty_opt,
):
    """Show health summary for all jobs."""
    output.set_pretty(pretty)
    from .commands.health import health as _health
    _health(url=url)


@app.command("run")
def run_cmd(
    job_id: str = typer.Argument(..., help="Job ID to trigger"),
    url: Optional[str] = typer.Option(None, envvar="DL_URL"),
):
    """Trigger an immediate run of a job (shortcut for `dl jobs run`)."""
    from .commands.jobs import run_job
    run_job(job_id=job_id, url=url)


# ── Config ─────────────────────────────────────────────────────────────────────

_config_app = typer.Typer(help="CLI configuration")
app.add_typer(_config_app, name="config")


@_config_app.command("set")
def config_set(
    url: str = typer.Option(..., "--url", help="Dremio Load URL"),
):
    """Save the Dremio Load URL to ~/.config/dl/config.json."""
    from .config import save
    save(url)
    output.success(f"Config saved: url={url}")


@_config_app.command("get")
def config_get():
    """Show current CLI config."""
    from .config import get_url, _CONFIG_PATH
    output.out({"url": get_url(), "config_file": str(_CONFIG_PATH)})


@_config_app.command("clear")
def config_clear():
    """Remove saved CLI config."""
    from .config import clear
    clear()
    output.success("Config cleared")


if __name__ == "__main__":
    app()
