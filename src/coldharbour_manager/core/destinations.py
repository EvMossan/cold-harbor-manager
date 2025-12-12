from __future__ import annotations

import os
import re
from typing import Any, Dict, List

DESTINATIONS: List[Dict[str, Any]] = [
    {
        "name": "live",
        "allow_trading": True,
        "base_url": os.getenv("ALPACA_BASE_URL_LIVE", "https://api.alpaca.markets"),
        "key_id": os.getenv("ALPACA_API_KEY_LIVE"),
        "secret_key": os.getenv("ALPACA_SECRET_KEY_LIVE"),
        "risk_factor": 0.1,
        "dry_run": False,
        "orders_table": "placed_orders_live",
        "trained_classifier_path": (
            "/home/tradingbot/proxima/airflow_docer/trained_models/ensemble/"
            "ensemble_202501.joblib"
        ),
        "trade_stream_enabled": False,
    },
    {
        "name": "cold-harbour_v1.0",
        "allow_trading": True,
        "base_url": os.getenv("ALPACA_BASE_URL_PAPER", "https://paper-api.alpaca.markets"),
        "key_id": os.getenv("ALPACA_API_KEY_JOHNYSSAN"),
        "secret_key": os.getenv("ALPACA_SECRET_KEY_JOHNYSSAN"),
        "risk_factor": 1.0,
        "dry_run": False,
        "orders_table": "placed_orders_1",
        "trained_classifier_path": (
            "/home/tradingbot/proxima/airflow_docer/trained_models/ensemble/"
            "ensemble_202501.joblib"
        ),
        "trade_stream_enabled": False,
    },
    {
        "name": "cold-harbour_v2.1",
        "allow_trading": True,
        "base_url": os.getenv("ALPACA_BASE_URL_PAPER", "https://paper-api.alpaca.markets"),
        "key_id": os.getenv("ALPACA_API_KEY_cold-harbour_v2.1"),
        "secret_key": os.getenv("ALPACA_SECRET_KEY_cold-harbour_v2.1"),
        "risk_factor": 1.0,
        "dry_run": False,
        "orders_table": "placed_orders_ch21",
        "trained_classifier_path": (
            "/home/tradingbot/proxima/airflow_docer/trained_models/ensemble/"
            "ensemble_202501.joblib"
        ),
        "trade_stream_enabled": False,
    },
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SAFE_RE = re.compile(r"[^a-z0-9_]")


def sanitize_identifier(raw: str) -> str:
    """Return a safe Postgres identifier derived from ``raw``.

    Convert to lowercase, replace non-alphanumeric/underscore chars with
    single underscores, strip leading/trailing underscores, and prefix
    digits with ``d_``. Empty results fall back to ``dest``.
    """
    s = raw.lower()
    s = _SAFE_RE.sub("_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        s = "dest"
    if s[0].isdigit():
        s = f"d_{s}"
    return s


def _is_paper(dest: Dict[str, Any]) -> bool:
    """Return true when the destination URL refers to paper trading."""
    base = (dest.get("base_url") or "").lower()
    if "paper" in base:
        return True
    # Fallback: explicit name 'live' → treat as live
    return False


def _schema_qualify(name: str, schema: str | None) -> str:
    if schema and "." not in name:
        return f"{schema}.{name}"
    return name


def slug_for(dest: Dict[str, Any]) -> str:
    """Return a stable slug based on the destination name."""
    return sanitize_identifier(str(dest.get("name", "dest")))


def account_tables_for(
    dest: Dict[str, Any], *, schema: str = "accounts"
) -> Dict[str, str]:
    """Return schema-qualified open and closed table names."""
    if _is_paper(dest):
        open_tbl = _schema_qualify("open_trades_paper", schema)
        closed_tbl = _schema_qualify("closed_trades_paper", schema)
    else:
        open_tbl = _schema_qualify("open_trades_live", schema)
        closed_tbl = _schema_qualify("closed_trades_live", schema)
    return {"open": open_tbl, "closed": closed_tbl}


def equity_table_for(
    dest: Dict[str, Any], *, schema: str = "accounts"
) -> str:
    """Return the schema-qualified equity table for the destination."""
    suffix = sanitize_identifier(str(dest.get("name", "dest")))
    tbl = f"equity_full_{suffix}"
    return _schema_qualify(tbl, schema)


def account_table_names(
    dest: Dict[str, Any], *, schema: str = "accounts"
) -> Dict[str, str]:
    """Return schema-qualified tables keyed by account role."""
    slug = slug_for(dest)
    open_tbl = _schema_qualify(f"open_trades_{slug}", schema)
    closed_tbl = _schema_qualify(f"closed_trades_{slug}", schema)
    equity_tbl = equity_table_for(dest, schema=schema)
    metrics_tbl = _schema_qualify(f"account_metrics_{slug}", schema)
    schedule_tbl = _schema_qualify("market_schedule", schema)
    return {
        "open": open_tbl,
        "closed": closed_tbl,
        "equity": equity_tbl,
        "metrics": metrics_tbl,
        "schedule": schedule_tbl,
    }


def notify_channels_for(dest: Dict[str, Any]) -> Dict[str, str]:
    """Return namespaced SSE channel names for the destination."""
    slug = slug_for(dest)
    return {
        "pos": f"pos_channel_{slug}",
        "closed": f"closed_channel_{slug}",
    }


def _pg_conn_url_from_env() -> str:
    """Return the Postgres SQLAlchemy URL from the environment.

    Prefer ``POSTGRESQL_LIVE_SQLALCHEMY`` and fall back to
    ``POSTGRESQL_LIVE_CONN_STRING``. Raise if neither is set.
    """
    dsn = os.getenv("POSTGRESQL_LIVE_SQLALCHEMY")
    if not dsn:
        dsn = os.getenv("POSTGRESQL_LIVE_CONN_STRING")
    if not dsn:
        raise RuntimeError(
            "Postgres connection string is not set. Define "
            "POSTGRESQL_LIVE_SQLALCHEMY or POSTGRESQL_LIVE_CONN_STRING."
        )
    return dsn

