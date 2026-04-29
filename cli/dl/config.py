"""Config and auth for the dl CLI.

Priority (highest first):
  1. CLI flag  --url
  2. DL_URL env var
  3. ~/.config/dl/config.json  (written by `dl config set`)
  4. Default: http://localhost:7071
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

_CONFIG_PATH = Path.home() / ".config" / "dl" / "config.json"
_DEFAULT_URL = "http://localhost:7071"


def _load_file() -> dict:
    if _CONFIG_PATH.exists():
        try:
            return json.loads(_CONFIG_PATH.read_text())
        except Exception:
            return {}
    return {}


def get_url(override: Optional[str] = None) -> str:
    if override:
        return override.rstrip("/")
    return (
        os.environ.get("DL_URL")
        or _load_file().get("url")
        or _DEFAULT_URL
    ).rstrip("/")


def save(url: str) -> None:
    _CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    _CONFIG_PATH.write_text(json.dumps({"url": url}, indent=2))
    _CONFIG_PATH.chmod(0o600)


def clear() -> None:
    if _CONFIG_PATH.exists():
        _CONFIG_PATH.unlink()
