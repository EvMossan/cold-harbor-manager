"""Local DB fetch helpers shared by AccountManager helpers."""

from __future__ import annotations

from typing import Dict, Optional, TYPE_CHECKING

from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import make_url

if TYPE_CHECKING:
    from cold_harbour.services.account_manager.runtime import AccountManager


def _parse_dsn_to_url(dsn: str) -> Optional[str]:
    """Convert `key=value` pairs into a SQLAlchemy URL if possible."""
    if "://" in dsn:
        return None
    parts: Dict[str, str] = {}
    for token in dsn.strip().split():
        if "=" not in token:
            continue
        key, _, val = token.partition("=")
        parts[key.lower()] = val
    if "dbname" not in parts and "database" not in parts:
        return None
    user_info = ""
    user = parts.get("user")
    password = parts.get("password")
    if user or password:
        user_info = quote_plus(user or "")
        if password:
            user_info += f":{quote_plus(password)}"
        if user_info:
            user_info += "@"
    host = parts.get("host", "localhost")
    port = parts.get("port")
    host_port = host
    if port:
        host_port = f"{host_port}:{port}"
    dbname = parts.get("dbname") or parts.get("database")
    path = f"/{quote_plus(dbname)}"
    return f"postgresql://{user_info}{host_port}{path}"


def _resolve_pg_conn_string(mgr: "AccountManager") -> str:
    """Return the connection string used for local-history read_sql."""
    conn = mgr._pg_conn_string or mgr.c.POSTGRESQL_LIVE_SQLALCHEMY
    if not conn:
        raise ValueError("no Postgres connection string configured")
    conv = _parse_dsn_to_url(conn)
    if conv:
        conn = conv
    try:
        url = make_url(conn)
    except Exception:
        return conn
    # Log effective target without exposing the password.
    host = url.host or "localhost"
    port = url.port or 5432
    user = url.username or ""
    db = url.database or ""
    mgr._pg_conn_host = host  # type: ignore[attr-defined]
    mgr._pg_conn_port = port  # type: ignore[attr-defined]
    mgr._pg_conn_user = user  # type: ignore[attr-defined]
    mgr._pg_conn_db = db  # type: ignore[attr-defined]
    try:
        mgr.log.debug(
            "Local history DB target: user=%s host=%s port=%s db=%s",
            user,
            host,
            port,
            db,
        )
    except Exception:
        pass
    driver = url.drivername.split("+", 1)[0]
    if driver != url.drivername:
        url = url.set(drivername=driver)
    return url.render_as_string(hide_password=False)
