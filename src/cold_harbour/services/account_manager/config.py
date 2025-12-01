"""Configuration helpers for the AccountManager runtime."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from sqlalchemy.engine import make_url


@dataclass
class _Config:
    """Typed view of the runtime config dict for the manager."""

    API_KEY: str
    SECRET_KEY: str
    ALPACA_BASE_URL: str = "https://paper-api.alpaca.markets"

    POSTGRESQL_LIVE_SQLALCHEMY: str = ""
    CONN_STRING_POSTGRESQL: Optional[str] = None
    # passed through unchanged

    TABLE_ACCOUNT_POSITIONS: str = "account_open_positions"
    TABLE_ACCOUNT_CLOSED: str = "account_closed"
    TABLE_ACCOUNT_EQUITY_FULL: str = "account_equity_full"
    TABLE_MARKET_SCHEDULE: str = "market_schedule"

    MIN_STOP_GAP: float = 0.01
    HEARTBEAT_SEC: int = 30
    SNAPSHOT_SEC: int = 60
    FLUSH_INTERVAL_S: float = 60.0
    CLOSED_SYNC_SEC: int = 60
    EQUITY_UPDATE_INTERVAL_S: int = 60
    UI_PUSH_PCT_THRESHOLD: float = 0.01
    ZMQ_SIP_STREAM_ENDPOINT: str = "tcp://127.0.0.1:5558"
    # UI streaming knobs
    UI_SNAPSHOT_SEC: int = 30
    UI_BATCH_MS: int = 200
    UI_PUSH_MIN_INTERVAL_S: float = 2.0
    CLOSED_LIMIT: int = 300

    MARKET_PRE_OPEN_TIME: str = "04:00"
    MARKET_POLL_SEC: int = 300
    MARKET_REFRESH_SEC: int = 21600

    POS_CHANNEL: str = "pos_channel"
    CLOSED_CHANNEL: str = "closed_channel"

    ACCOUNT_SCHEMA: Optional[str] = "accounts"

    DISABLE_SESSION_SLEEP: bool = True

    LOG_LEVEL: Optional[str] = None

    TRADE_CONDITIONS_EXCLUDE: tuple[str, ...] = (
        "",
        "D",
        "M",
        "E",
    )


DB_COLS: tuple[str, ...] = (
    "parent_id",
    "symbol",
    "filled_at",
    "qty",
    "avg_fill",
    "avg_px_symbol",
    "sl_child",
    "sl_px",
    "tp_child",
    "tp_px",
    "mkt_px",
    "moved_flag",
    "buy_value",
    "holding_days",
    "mkt_value",
    "profit_loss",
    "profit_loss_lot",
    "tp_sl_reach_pct",
    "updated_at",
)


def _build_pg_dsn_from_sqlalchemy_url(sa_url: str) -> str:
    """Make a libpq DSN from an SQLAlchemy URL (unchanged logic)."""
    url = make_url(sa_url)
    return (
        f"host={url.host} "
        f"port={url.port or 5432} "
        f"dbname={url.database} "
        f"user={url.username} "
        f"password={url.password}"
    )
