"""Thin httpx wrapper around the Dremio Load REST API."""
from __future__ import annotations

import sys
from typing import Any, Optional

import httpx

from .config import get_url

DEFAULT_TIMEOUT = 30.0


class DLClient:
    def __init__(self, url: Optional[str] = None):
        self.base = get_url(url)

    def _headers(self) -> dict:
        return {"Content-Type": "application/json", "Accept": "application/json"}

    def _handle(self, r: httpx.Response) -> Any:
        if r.status_code == 204:
            return None
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError:
            try:
                detail = r.json().get("error", r.text)
            except Exception:
                detail = r.text
            print(f"Error {r.status_code}: {detail}", file=sys.stderr)
            sys.exit(1)
        if not r.content:
            return None
        return r.json()

    def get(self, path: str, **params) -> Any:
        with httpx.Client(timeout=DEFAULT_TIMEOUT) as c:
            r = c.get(f"{self.base}{path}", headers=self._headers(), params={k: v for k, v in params.items() if v is not None})
        return self._handle(r)

    def post(self, path: str, body: Any = None) -> Any:
        with httpx.Client(timeout=DEFAULT_TIMEOUT) as c:
            r = c.post(f"{self.base}{path}", headers=self._headers(), json=body)
        return self._handle(r)

    def put(self, path: str, body: Any = None) -> Any:
        with httpx.Client(timeout=DEFAULT_TIMEOUT) as c:
            r = c.put(f"{self.base}{path}", headers=self._headers(), json=body)
        return self._handle(r)

    def delete(self, path: str) -> Any:
        with httpx.Client(timeout=DEFAULT_TIMEOUT) as c:
            r = c.delete(f"{self.base}{path}", headers=self._headers())
        return self._handle(r)

    def get_parallel(self, paths: list[str]) -> list[Any]:
        import asyncio

        async def _fetch_all():
            async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as c:
                tasks = [c.get(f"{self.base}{p}", headers=self._headers()) for p in paths]
                responses = await asyncio.gather(*tasks, return_exceptions=True)
            results = []
            for r in responses:
                if isinstance(r, Exception):
                    results.append(None)
                elif r.status_code in (200, 201):
                    try:
                        results.append(r.json())
                    except Exception:
                        results.append(None)
                else:
                    results.append(None)
            return results

        return asyncio.run(_fetch_all())
