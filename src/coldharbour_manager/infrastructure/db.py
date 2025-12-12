"""Provide async Postgres helpers for account manager components.

Centralise asyncpg connection handling and expose lightweight
fetch/execute helpers. Include DSN normalisation that accepts
SQLAlchemy URLs so existing config keys can be reused.
"""

from __future__ import annotations

import asyncio
from typing import Any, Iterable, Optional

import asyncpg
from sqlalchemy.engine import make_url


def _url_from_sqlalchemy(sa_url: str) -> str:
    """Build a Postgres URL without a driver suffix from
    an SQLAlchemy URL."""

    url = make_url(sa_url)
    driver = (url.drivername or "postgresql").split("+")[0]
    user = url.username or ""
    password = url.password or ""
    host = url.host or "localhost"
    port = f":{url.port}" if url.port else ""
    auth = ""
    if user and password:
        auth = f"{user}:{password}@"
    elif user:
        auth = f"{user}@"
    return f"{driver}://{auth}{host}{port}/{url.database or ''}"


def _url_from_libpq(dsn: str) -> str:
    """Convert a libpq-style DSN to a Postgres URL."""

    parts = {}
    for tok in dsn.split():
        if "=" in tok:
            k, v = tok.split("=", 1)
            parts[k.strip()] = v.strip()
    driver = "postgresql"
    user = parts.get("user", "")
    password = parts.get("password", "")
    host = parts.get("host", "localhost")
    port = f":{parts['port']}" if parts.get("port") else ""
    db = parts.get("dbname", "")
    auth = ""
    if user and password:
        auth = f"{user}:{password}@"
    elif user:
        auth = f"{user}@"
    return f"{driver}://{auth}{host}{port}/{db}"


class AsyncAccountRepository:
    """Wrap an asyncpg pool with repository convenience helpers."""

    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool
        self._closed = False
        self._close_lock = asyncio.Lock()

    @classmethod
    async def create(
        cls,
        conn_string: str,
        *,
        min_size: int = 1,
        max_size: int = 10,
    ) -> "AsyncAccountRepository":
        """Create a pool from an SQLAlchemy URL or libpq DSN."""
        if "://" in conn_string:
            dsn = _url_from_sqlalchemy(conn_string)
        elif "host=" in conn_string:
            dsn = _url_from_libpq(conn_string)
        else:
            dsn = conn_string
        pool = await asyncpg.create_pool(
            dsn=dsn,
            min_size=max(1, min_size),
            max_size=max(min_size, max_size),
        )
        return cls(pool)

    async def close(self) -> None:
        """Close the underlying pool once."""
        async with self._close_lock:
            if self._closed:
                return
            self._closed = True
            await self.pool.close()

    async def fetch(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        """Run SELECT SQL and return rows as dicts."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, *args)
        return [dict(r) for r in rows]

    async def fetchrow(self, sql: str, *args: Any) -> Optional[dict[str, Any]]:
        """Return a single row as a dict or ``None``."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(sql, *args)
        return dict(row) if row else None

    async def execute(self, sql: str, *args: Any) -> str:
        """Execute a statement and return the asyncpg status string."""
        async with self.pool.acquire() as conn:
            return await conn.execute(sql, *args)

    async def executemany(
        self,
        sql: str,
        params: Iterable[Iterable[Any]],
    ) -> None:
        """Run executemany with a sequence of parameter sets."""
        async with self.pool.acquire() as conn:
            await conn.executemany(sql, list(params))
