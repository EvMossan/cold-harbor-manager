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


def _fetch_orders_from_db(
    mgr: "AccountManager", days_back: int = 365
) -> pd.DataFrame:
    """Read orders from the local ingester history table."""
    cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=days_back)
    sql = text(
        f"""
        SELECT
            id,
            client_order_id,
            symbol,
            side,
            type as order_type,
            status,
            qty,
            filled_qty,
            filled_avg_price,
            limit_price,
            stop_price,
            created_at,
            updated_at,
            submitted_at,
            filled_at,
            raw_json->>'parent_order_id' as parent_id,
            raw_json->'legs' as legs
        FROM {mgr.tbl_raw_orders}
        WHERE created_at >= :cutoff
        ORDER BY created_at DESC
        """
    )

    params = {"cutoff": cutoff}
    try:
        conn_str = _resolve_pg_conn_string(mgr)
        df = pd.read_sql(sql, conn_str, params=params)
    except Exception as exc:
        mgr.log.error(
            "Failed to fetch orders from DB (conn=%s@%s:%s/db=%s): %s",
            getattr(mgr, "_pg_conn_user", "unknown"),
            getattr(mgr, "_pg_conn_host", "unknown"),
            getattr(mgr, "_pg_conn_port", "unknown"),
            getattr(mgr, "_pg_conn_db", "unknown"),
            exc,
        )
        return pd.DataFrame()

    for col in (
        "created_at",
        "updated_at",
        "filled_at",
        "submitted_at",
    ):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    for col in (
        "qty",
        "filled_qty",
        "limit_price",
        "stop_price",
        "filled_avg_price",
    ):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "parent_id" not in df.columns:
        df["parent_id"] = pd.NA

    if "id" in df.columns:
        df = df.drop_duplicates(subset=["id"]).reset_index(drop=True)

    return df


def _fetch_activities_from_db(
    mgr: "AccountManager", activity_type: Optional[str] = None
) -> pd.DataFrame:
    """Read activities from the ingester raw_activities table."""
    query = f"""
        SELECT
            id,
            activity_type,
            transaction_time as exec_time,
            symbol,
            side,
            qty,
            price,
            net_amount,
            order_id
        FROM {mgr.tbl_raw_activities}
    """
    params: Dict[str, str] = {}
    if activity_type:
        query += " WHERE activity_type = :atype"
        params["atype"] = activity_type
    query += " ORDER BY transaction_time DESC"

    try:
        df = pd.read_sql(
            text(query), _resolve_pg_conn_string(mgr), params=params
        )
    except Exception as exc:
        mgr.log.error("Failed to fetch activities from DB: %s", exc)
        return pd.DataFrame()

    if "exec_time" in df.columns:
        df["exec_time"] = pd.to_datetime(
            df["exec_time"], utc=True, errors="coerce"
        )
    return df
