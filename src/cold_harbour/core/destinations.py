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
        "initial_deposit": 0.0,
        "trained_classifier_path": (
            "/home/tradingbot/proxima/airflow_docer/trained_models/ensemble/"
            "ensemble_202501.joblib"
        ),
    },
    # {
    #     "name": "cold-harbour_v1.0",
    #     "allow_trading": True,
    #     "base_url": os.getenv("ALPACA_BASE_URL_PAPER", "https://paper-api.alpaca.markets"),
    #     "key_id": os.getenv("ALPACA_API_KEY_JOHNYSSAN"),
    #     "secret_key": os.getenv("ALPACA_SECRET_KEY_JOHNYSSAN"),
    #     "risk_factor": 1.0,
    #     "dry_run": False,
    #     "orders_table": "placed_orders_1",
    #     "trained_classifier_path": (
    #         "/home/tradingbot/proxima/airflow_docer/trained_models/ensemble/"
    #         "ensemble_202501.joblib"
    #     ),
    # },
    # {
    #     "name": "cold-harbour_v2.1",
    #     "allow_trading": True,
    #     "base_url": os.getenv("ALPACA_BASE_URL_PAPER", "https://paper-api.alpaca.markets"),
    #     "key_id": os.getenv("ALPACA_API_KEY_cold-harbour_v2.1"),
    #     "secret_key": os.getenv("ALPACA_SECRET_KEY_cold-harbour_v2.1"),
    #     "risk_factor": 1.0,
    #     "dry_run": False,
    #     "orders_table": "placed_orders_ch21",
    #     "trained_classifier_path": (
    #         "/home/tradingbot/proxima/airflow_docer/trained_models/ensemble/"
    #         "ensemble_202501.joblib"
    #     ),
    # },
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SAFE_RE = re.compile(r"[^a-z0-9_]")


def sanitize_identifier(raw: str) -> str:
    """Return a safe, unquoted Postgres identifier from an arbitrary name.

    Rules:
    - lower-case
    - replace any character outside ``[a-z0-9_]`` with ``_``
    - collapse repeated underscores
    - strip leading/trailing underscores
    - if the first character is a digit, prefix ``d_``
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
    """Heuristic: paper if base URL includes 'paper'; otherwise live."""
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
    """Return a safe slug for the destination based on its ``name``.

    The slug is used to derive per-account table names and notify channels.
    It is lower-case, alphanumeric/underscore only, and stable.
    """
    return sanitize_identifier(str(dest.get("name", "dest")))


def account_tables_for(
    dest: Dict[str, Any], *, schema: str = "accounts"
) -> Dict[str, str]:
    """Return open/closed table names for this destination's account type."""
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
    """Return per-destination equity table name (schema-qualified)."""
    suffix = sanitize_identifier(str(dest.get("name", "dest")))
    tbl = f"equity_full_{suffix}"
    return _schema_qualify(tbl, schema)


def account_table_names(
    dest: Dict[str, Any], *, schema: str = "accounts"
) -> Dict[str, str]:
    """Return per-destination table names using the destination slug.

    This generates fully schema-qualified names:
    - open:   ``accounts.open_trades_<slug>``
    - closed: ``accounts.closed_trades_<slug>``
    - equity: ``accounts.equity_full_<slug>``
    - metrics: ``accounts.account_metrics_<slug>``
    - schedule: shared ``accounts.market_schedule`` for all accounts
    """
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
    """Return ``pos`` and ``closed`` channel names for this destination.

    Channels are namespaced by destination slug to isolate SSE streams when
    running multiple accounts in a single process/container.
    """
    slug = slug_for(dest)
    return {
        "pos": f"pos_channel_{slug}",
        "closed": f"closed_channel_{slug}",
    }


def _pg_conn_url_from_env() -> str:
    """Resolve Postgres SQLAlchemy URL from environment.

    Accepts either ``POSTGRESQL_LIVE_SQLALCHEMY`` or
    ``POSTGRESQL_LIVE_CONN_STRING`` for compatibility.
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


def build_equity_cfg(
    dest: Dict[str, Any], *, schema: str | None = None
) -> Dict[str, Any]:
    """Build cfg dict for ``rebuild_equity_series`` for this destination.

    Keys:
    - CONN_STRING_POSTGRESQL
    - API_KEY, SECRET_KEY, ALPACA_BASE_URL
    - TABLE_ACCOUNT_CLOSED, TABLE_ACCOUNT_POSITIONS
    - TABLE_ACCOUNT_EQUITY_FULL
    - EQUITY_INIT_CASH (optional): initial equity override
    """
    schema = schema or os.getenv("ACCOUNT_SCHEMA", "accounts")
    # Use per-destination tables derived from slug (one set per account)
    tables = account_table_names(dest, schema=schema)
    cfg = {
        "CONN_STRING_POSTGRESQL": _pg_conn_url_from_env(),
        "API_KEY": dest.get("key_id"),
        "SECRET_KEY": dest.get("secret_key"),
        "ALPACA_BASE_URL": dest.get("base_url")
        or os.getenv(
            "ALPACA_BASE_URL_PAPER", "https://paper-api.alpaca.markets"
        ),
        "TABLE_ACCOUNT_CLOSED": tables["closed"],
        "TABLE_ACCOUNT_POSITIONS": tables["open"],
        "TABLE_ACCOUNT_EQUITY_FULL": tables["equity"],
        "TABLE_MARKET_SCHEDULE": tables.get("schedule"),
    }

    # Optional initial equity override. Prefer per-destination value,
    # then EQUITY_INIT_CASH_<slug>, then EQUITY_INIT_CASH.
    try:
        slug = slug_for(dest)
    except Exception:
        slug = ""
    init_cash: float | None
    init_cash = dest.get("initial_deposit")  # type: ignore[assignment]
    if init_cash is None:
        env_key = f"EQUITY_INIT_CASH_{slug}" if slug else None
        raw = (os.getenv(env_key or "") or os.getenv("EQUITY_INIT_CASH"))
        if raw:
            try:
                init_cash = float(raw)
            except Exception:
                init_cash = None
    if init_cash is not None:
        cfg["EQUITY_INIT_CASH"] = float(init_cash)

    missing = [k for k, v in cfg.items() if v in (None, "")]
    if missing:
        raise RuntimeError(
            "Missing keys for destination cfg: " + ", ".join(missing)
        )
    return cfg
