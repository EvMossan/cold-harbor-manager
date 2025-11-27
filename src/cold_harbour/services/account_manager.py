"""
Async account/position manager for Alpaca backed by Postgres.

Keeps the DB in sync with Alpaca and emits NOTIFY for UI consumers.
Public API: ``AccountManager(cfg)``, ``run()``, ``close()``.
Runtime tasks: price listener, DB worker, snapshot, closed sync, equity,
trade stream.

NOTIFY payloads:
- pos_channel upsert → ``{"row": {<DB_COLS>}}``
- pos_channel delete → ``{"op":"delete","parent_id":"..."}``
- closed_channel refresh → ``'refresh'``
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import signal
import contextlib
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone, timedelta, time as dtime
from decimal import Decimal
from typing import Any, Awaitable, Callable, Dict, Optional

import msgpack
import numpy as np
import pandas as pd
import zmq.asyncio
from alpaca_trade_api.rest import REST
from sqlalchemy.engine import make_url
from zoneinfo import ZoneInfo

from cold_harbour.core.equity import (
    rebuild_equity_series_async,
    update_today_row_async,
)
from cold_harbour.core.account_utils import (
    fetch_all_orders,
    build_open_positions_df,
    closed_trades_fifo_from_orders,
    trading_session_date,
)
from cold_harbour.core.market_schedule import (
    SessionWindow,
    next_session_after_async,
    session_for_timestamp_async,
    sync_schedule_async,
)
from cold_harbour.infrastructure.db import AsyncAccountRepository

# ──────────────────────────────────────────────────────────────────────
#  Config and constants
# ──────────────────────────────────────────────────────────────────────


@dataclass
class _Config:
    """Lightweight, typed view of the runtime config dict.

    All defaults mirror the previous implementation's behavior. No new
    required keys are introduced; we only read those used by the runtime.
    """

    API_KEY: str
    SECRET_KEY: str
    ALPACA_BASE_URL: str = "https://paper-api.alpaca.markets"

    POSTGRESQL_LIVE_SQLALCHEMY: str = ""
    CONN_STRING_POSTGRESQL: Optional[str] = None  # passed through unchanged

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

    # Notification channels (per-account isolation when running multi-account)
    POS_CHANNEL: str = "pos_channel"
    CLOSED_CHANNEL: str = "closed_channel"

    # Optional schema for account tables (e.g., 'accounts')
    ACCOUNT_SCHEMA: Optional[str] = "accounts"

    DISABLE_SESSION_SLEEP: bool = False

    # Optional explicit logging level for this manager
    LOG_LEVEL: Optional[str] = None

    # Trade tick conditions to exclude in the ZMQ price stream. These are
    # SIP/TAQ condition codes (strings). When a trade packet carries any of
    # these codes, the tick is ignored by the price listener. The default is
    TRADE_CONDITIONS_EXCLUDE: tuple[str, ...] = ("", "D", "M", "E")


# Persisted DB columns – the single source of truth for upserts/NOTIFY payloads
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


# ──────────────────────────────────────────────────────────────────────
#  Utility helpers
# ──────────────────────────────────────────────────────────────────────


def _utcnow() -> datetime:
    """Return an aware UTC datetime."""
    return datetime.now(timezone.utc)


def _fmt_ts_alpaca(dt: datetime) -> str:
    """Return Alpaca-friendly UTC timestamp like 2025-07-20T05:58:11Z."""
    return (
        dt.astimezone(timezone.utc)
        .replace(tzinfo=None)
        .isoformat(timespec="seconds")
        + "Z"
    )


def _json_safe(v: Any) -> Any:
    """Convert DB / numpy / datetime values to JSON-safe primitives."""
    if isinstance(v, (datetime, date, pd.Timestamp)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    return v


def _parse_bool(value: Any, default: bool = False) -> bool:
    """Return a boolean from common string/int representations."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


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


def _is_flat(qty: float) -> bool:
    """Near-zero test used in order handlers."""
    return abs(qty) < 1e-12


# ──────────────────────────────────────────────────────────────────────
#  AccountManager
# ──────────────────────────────────────────────────────────────────────


class AccountManager:
    """Realtime account-state synchronizer for Alpaca.

    Public surface and runtime semantics are preserved exactly. See
    the module docstring for a summary of invariants.
    """

    DEFAULT_TABLE = "account_open_positions"

    # ──────────────────────────────────────────────────────────────────────
    #  Initialization
    # ──────────────────────────────────────────────────────────────────────

    def __init__(self, cfg: Dict[str, Any]):
        """Wire up connections, runtime knobs, and in-memory state."""
        self.cfg: Dict[str, Any] = dict(cfg)  # original dict stays available

        heartbeat_raw = cfg.get("HEARTBEAT_SEC")
        if heartbeat_raw is not None:
            heartbeat_sec = max(1, int(heartbeat_raw))
            heartbeat_via_new_key = True
        else:
            heartbeat_sec = max(
                1,
                int(
                    cfg.get(
                        "EQUITY_UPDATE_INTERVAL_S",
                        _Config.HEARTBEAT_SEC,
                    )
                ),
            )
            heartbeat_via_new_key = False

        snapshot_sec = max(1, int(cfg.get("SNAPSHOT_SEC", heartbeat_sec)))
        closed_sync_sec = max(
            1, int(cfg.get("CLOSED_SYNC_SEC", heartbeat_sec))
        )
        if heartbeat_via_new_key:
            equity_interval_sec = heartbeat_sec
        else:
            equity_interval_sec = max(
                1, int(cfg.get("EQUITY_UPDATE_INTERVAL_S", heartbeat_sec))
            )
        ui_snapshot_sec = max(
            1, int(cfg.get("UI_SNAPSHOT_SEC", heartbeat_sec))
        )

        self.c = _Config(
            API_KEY=cfg["API_KEY"],
            SECRET_KEY=cfg["SECRET_KEY"],
            ALPACA_BASE_URL=cfg.get(
                "ALPACA_BASE_URL", _Config.ALPACA_BASE_URL
            ),
            POSTGRESQL_LIVE_SQLALCHEMY=cfg["POSTGRESQL_LIVE_SQLALCHEMY"],
            CONN_STRING_POSTGRESQL=cfg.get("CONN_STRING_POSTGRESQL"),
            TABLE_ACCOUNT_POSITIONS=cfg.get(
                "TABLE_ACCOUNT_POSITIONS", _Config.TABLE_ACCOUNT_POSITIONS
            ),
            TABLE_ACCOUNT_CLOSED=cfg.get(
                "TABLE_ACCOUNT_CLOSED", _Config.TABLE_ACCOUNT_CLOSED
            ),
            TABLE_ACCOUNT_EQUITY_FULL=cfg.get(
                "TABLE_ACCOUNT_EQUITY_FULL", _Config.TABLE_ACCOUNT_EQUITY_FULL
            ),
            MIN_STOP_GAP=float(cfg.get("MIN_STOP_GAP", _Config.MIN_STOP_GAP)),
            HEARTBEAT_SEC=heartbeat_sec,
            SNAPSHOT_SEC=snapshot_sec,
            FLUSH_INTERVAL_S=float(
                cfg.get("FLUSH_INTERVAL_S", _Config.FLUSH_INTERVAL_S)
            ),
            CLOSED_SYNC_SEC=closed_sync_sec,
            EQUITY_UPDATE_INTERVAL_S=equity_interval_sec,
            UI_PUSH_PCT_THRESHOLD=float(
                cfg.get("UI_PUSH_PCT_THRESHOLD", _Config.UI_PUSH_PCT_THRESHOLD)
            ),
            ZMQ_SIP_STREAM_ENDPOINT=cfg.get(
                "ZMQ_SIP_STREAM_ENDPOINT", _Config.ZMQ_SIP_STREAM_ENDPOINT
            ),
            POS_CHANNEL=cfg.get("POS_CHANNEL", _Config.POS_CHANNEL),
            CLOSED_CHANNEL=cfg.get("CLOSED_CHANNEL", _Config.CLOSED_CHANNEL),
            ACCOUNT_SCHEMA=cfg.get("ACCOUNT_SCHEMA", _Config.ACCOUNT_SCHEMA),
            LOG_LEVEL=cfg.get("LOG_LEVEL"),
            UI_SNAPSHOT_SEC=ui_snapshot_sec,
            DISABLE_SESSION_SLEEP=_parse_bool(
                cfg.get(
                    "DISABLE_SESSION_SLEEP",
                    _Config.DISABLE_SESSION_SLEEP,
                )
            ),
        )

        # Temporary logger; replaced with a per-account logger once
        # tables/channels are initialised below.
        self.log = logging.getLogger("AccountMgr")
        self.disable_session_sleep = bool(self.c.DISABLE_SESSION_SLEEP)
        self.cfg["DISABLE_SESSION_SLEEP"] = self.disable_session_sleep

        # Postgres (asyncpg via repository; initialised lazily in init()).
        raw_conn = (
            cfg.get("POSTGRESQL_LIVE_SQLALCHEMY")
            or cfg.get("CONN_STRING_POSTGRESQL")
            or os.getenv("POSTGRESQL_LIVE_SQLALCHEMY")
            or os.getenv("POSTGRESQL_LIVE_CONN_STRING")
            or ""
        )
        self._pg_conn_string = raw_conn.strip()
        self.repo: Optional[AsyncAccountRepository] = None
        ts_conn = (
            cfg.get("TIMESCALE_LIVE_SQLALCHEMY")
            or cfg.get("TIMESCALE_LIVE_CONN_STRING")
            or os.getenv("TIMESCALE_LIVE_SQLALCHEMY")
            or os.getenv("TIMESCALE_LIVE_CONN_STRING")
            or ""
        )
        self._ts_conn_string = ts_conn.strip()
        self.ts_repo: Optional[AsyncAccountRepository] = None

        # Keep the URL available in self.cfg for downstream helpers.
        self.cfg["CONN_STRING_POSTGRESQL"] = (
            self._pg_conn_string or self.c.POSTGRESQL_LIVE_SQLALCHEMY
        )

        # Alpaca REST client (unchanged).
        self.rest = REST(
            self.c.API_KEY, self.c.SECRET_KEY, self.c.ALPACA_BASE_URL
        )

        # Effective table names (allow runtime overrides).
        def _qualify(schema: Optional[str], name: str) -> str:
            if not name:
                return name
            if "." in name:
                return name  # already schema-qualified
            return f"{schema}.{name}" if schema else name

        schema = (self.c.ACCOUNT_SCHEMA or "").strip() or None
        self.tbl_live = _qualify(schema, self.c.TABLE_ACCOUNT_POSITIONS)
        self.tbl_closed = _qualify(schema, self.c.TABLE_ACCOUNT_CLOSED)
        self.tbl_equity = _qualify(schema, self.c.TABLE_ACCOUNT_EQUITY_FULL)
        # Derive intraday equity table alongside full equity

        def _derive_intraday(full_name: str) -> str:
            try:
                if not full_name:
                    return _qualify(schema, "account_equity_intraday")
                if "." in full_name:
                    sch, base = full_name.split(".", 1)
                else:
                    sch, base = None, full_name
                if base.startswith("equity_full_"):
                    base2 = base.replace("equity_full_", "equity_intraday_", 1)
                else:
                    base2 = f"{base}_intraday"
                return f"{sch}.{base2}" if sch else base2
            except Exception:
                return _qualify(schema, "account_equity_intraday")

        self.tbl_equity_intraday = _derive_intraday(self.tbl_equity)

        # Derive cash-flows table alongside equity (accounts.cash_flows_<slug>)
        def _derive_cash_flows(full_name: str) -> str:
            try:
                if not full_name:
                    return _qualify(schema, "cash_flows")
                if "." in full_name:
                    sch, base = full_name.split(".", 1)
                else:
                    sch, base = None, full_name
                if base.startswith("equity_full_"):
                    base2 = base.replace("equity_full_", "cash_flows_", 1)
                else:
                    base2 = (
                        f"cash_flows_{base}"
                        if not base.startswith("cash_flows_")
                        else base
                    )
                return f"{sch}.{base2}" if sch else base2
            except Exception:
                return _qualify(schema, "cash_flows")

        self.tbl_cash_flows = _derive_cash_flows(self.tbl_equity)
        self.tbl_market_schedule = _qualify(
            schema, self.c.TABLE_MARKET_SCHEDULE
        )

        # Per-account NOTIFY channels.
        self.pos_channel = self.c.POS_CHANNEL
        self.closed_channel = self.c.CLOSED_CHANNEL

        # ---------------- Logging: per-account label + local timestamps
        # Derive a human-friendly label to disambiguate logs when multiple
        # accounts/managers run in the same process.
        label = str(self.cfg.get("ACCOUNT_LABEL") or "").strip()
        if not label:
            # Try to infer from channel name like "pos_channel_<slug>"
            ch = str(self.pos_channel or "")
            if ch.startswith("pos_channel_") and len(ch) > len("pos_channel_"):
                label = ch.split("pos_channel_", 1)[1]
        if not label:
            # Fallback to table suffix after the last underscore, e.g.
            # accounts.account_open_positions_<slug> → <slug>
            t = str(self.tbl_live or "")
            base = t.split(".")[-1]
            if "_" in base:
                label = base.rsplit("_", 1)[-1]
        self.account_label = label or "default"

        # Configure a dedicated logger per account. We attach a local-time
        # formatter and disable propagation to avoid double-printing when the
        # root logger already has handlers (e.g. under Airflow).
        log_name = f"AccountMgr[{self.account_label}]"
        self.log = logging.getLogger(log_name)
        if not self.log.handlers:
            handler = logging.StreamHandler(stream=sys.stdout)
            fmt = "%(asctime)s %(levelname)s %(name)s: %(message)s"
            datefmt = "%Y-%m-%d %H:%M:%S"
            formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)
            # Use local time for timestamps as requested
            formatter.converter = time.localtime  # type: ignore[attr-defined]
            handler.setFormatter(formatter)
            self.log.addHandler(handler)

            # Resolve logging level from cfg/env; default to DEBUG to make
            # troubleshooting easier unless explicitly overridden.
            def _parse_level(v: Optional[str]) -> int:
                try:
                    if not v:
                        return logging.DEBUG
                    name = str(v).strip().upper()
                    return getattr(logging, name, logging.DEBUG)
                except Exception:
                    return logging.DEBUG

            lvl_name = (
                self.c.LOG_LEVEL
                or os.getenv("ACCOUNT_LOG_LEVEL")
                or os.getenv("LOG_LEVEL")
                or "DEBUG"
            )
            self.log.setLevel(_parse_level(lvl_name))
            # Let the logger level control filtering; keep handler open
            handler.setLevel(logging.NOTSET)
            # Prevent propagation to root to avoid duplicates
            self.log.propagate = False

        if self.disable_session_sleep:
            self.log.info("Session sleep logic disabled by configuration.")

        # Timing knobs (preserved).
        self.HEARTBEAT_SEC = self.c.HEARTBEAT_SEC
        self.SNAPSHOT_SEC = self.c.SNAPSHOT_SEC
        self.FLUSH_INTERVAL_S = self.c.FLUSH_INTERVAL_S
        self.CLOSED_SYNC_SEC = self.c.CLOSED_SYNC_SEC
        self.EQUITY_INTRADAY_SEC = self.c.EQUITY_UPDATE_INTERVAL_S
        self.UI_PUSH_PCT_THRESHOLD = self.c.UI_PUSH_PCT_THRESHOLD
        self.UI_SNAPSHOT_SEC = self.c.UI_SNAPSHOT_SEC
        self.UI_BATCH_MS = self.c.UI_BATCH_MS
        self.UI_PUSH_MIN_INTERVAL_S = self.c.UI_PUSH_MIN_INTERVAL_S
        self.CLOSED_LIMIT = self.c.CLOSED_LIMIT

        def _parse_time(raw: str) -> dtime:
            try:
                hh, mm = str(raw).split(":", 1)
                return dtime(hour=int(hh), minute=int(mm))
            except Exception:
                return dtime(hour=4)

        self._market_pre_open_time: dtime = _parse_time(
            self.c.MARKET_PRE_OPEN_TIME
        )
        self.MARKET_POLL_SEC = max(30, int(self.c.MARKET_POLL_SEC))
        self.MARKET_REFRESH_SEC = max(300, int(self.c.MARKET_REFRESH_SEC))
        self.MARKET_LOOKBACK_DAYS = 7
        self.MARKET_FORWARD_DAYS = 21

        # In-memory state.
        self.state: Dict[str, Dict[str, Any]] = {}  # parent_id → row
        self.sym2pid: Dict[str, set[str]] = {}
        self._snap_lock = asyncio.Lock()
        self._pending_legs: Dict[str, Dict[str, Any]] = {}
        self._db_lock = asyncio.Lock()
        # Per‑symbol snapshot refresh in‑flight guard
        self._sym_refresh_inflight: set[str] = set()
        # Outgoing UI batch placeholders (future use)
        self._ui_out_rows: list[dict] = []
        self._ui_out_dels: list[str] = []
        self._active_tasks: list[asyncio.Task] = []
        self._active_session: SessionWindow | None = None
        # Pending realised PnL captured from trade updates before DB sync
        self._pending_realised: list[tuple[date, float]] = []

        # Exclusion set for SIP trade condition codes used by the ZMQ
        # price listener. Accepts list/tuple/set or CSV/space-separated
        # string via cfg["TRADE_CONDITIONS_EXCLUDE"]. Defaults to the
        # unified set used by the notebook resampler.
        def _norm_excl(v: Any) -> set[str]:
            try:
                if isinstance(v, (list, tuple, set)):
                    return {str(x).strip() for x in v}
                if isinstance(v, str):
                    toks = v.replace(",", " ").split()
                    return {t.strip() for t in toks}
            except Exception:
                pass
            return set(self.c.TRADE_CONDITIONS_EXCLUDE)

        self._cond_exclude: set[str] = _norm_excl(
            self.cfg.get("TRADE_CONDITIONS_EXCLUDE")
        )

        # Snapshot readiness flag to gate per-symbol refreshes until the
        # initial snapshot completes. This prevents early trade updates
        # (legs without parents) from triggering symbol refresh against
        # an uninitialised open-positions view.
        self._snapshot_ready: bool = False
        self._snapshot_last_complete: datetime | None = None
        self._last_price_tick: datetime | None = None
        self._supervisor_last_tick: datetime | None = None

    async def init(self) -> None:
        """Initialise async Postgres repository and ensure tables exist."""
        if self.repo is not None:
            return
        conn_raw = self._pg_conn_string
        if not conn_raw:
            raise ValueError(
                "Database connection string is empty; expected "
                "POSTGRESQL_LIVE_SQLALCHEMY or CONN_STRING_POSTGRESQL."
            )
        self.repo = await AsyncAccountRepository.create(
            conn_raw
        )
        await self._ensure_tables()
        if self._ts_conn_string:
            self.ts_repo = await AsyncAccountRepository.create(
                self._ts_conn_string
            )

    async def _db_fetch(self, sql: str, *args: Any) -> list[dict]:
        assert self.repo is not None
        return await self.repo.fetch(sql, *args)

    async def _db_fetchrow(self, sql: str, *args: Any) -> Optional[dict]:
        assert self.repo is not None
        return await self.repo.fetchrow(sql, *args)

    async def _db_execute(self, sql: str, *args: Any) -> str:
        assert self.repo is not None
        try:
            return await self.repo.execute(sql, *args)
        except Exception:
            self.log.exception("DB execute failed: %s", sql)
            raise

    async def _ts_fetch(self, sql: str, *args: Any) -> list[dict]:
        """Fetch rows from Timescale if configured; fall back to Postgres."""
        if self.ts_repo is not None:
            return await self.ts_repo.fetch(sql, *args)
        return await self._db_fetch(sql, *args)

    async def _ts_fetchrow(self, sql: str, *args: Any) -> Optional[dict]:
        """Fetch a single row from Timescale if configured; else fall back."""
        if self.ts_repo is not None:
            return await self.ts_repo.fetchrow(sql, *args)
        return await self._db_fetchrow(sql, *args)

    async def _type_exists(self, name: str) -> bool:
        """Return True if a composite/enum type exists for ``name``."""
        schema, typ = (None, name)
        if "." in name:
            schema, typ = name.split(".", 1)
        schema = schema or "public"
        row = await self._db_fetchrow(
            """
            SELECT 1
              FROM pg_catalog.pg_type t
              JOIN pg_catalog.pg_namespace n
                ON n.oid = t.typnamespace
             WHERE n.nspname = $1
               AND t.typname = $2
             LIMIT 1;
            """,
            schema,
            typ,
        )
        return bool(row)

    async def _drop_type(self, name: str) -> None:
        """Drop a composite/enum type if it exists (no CASCADE)."""
        schema, typ = (None, name)
        if "." in name:
            schema, typ = name.split(".", 1)
        schema = schema or "public"
        await self._db_execute(f"DROP TYPE IF EXISTS {schema}.{typ};")

    async def _db_executemany(
        self,
        sql: str,
        params: list[tuple[Any, ...]],
    ) -> None:
        assert self.repo is not None
        await self.repo.executemany(sql, params)

    async def _relation_exists(self, name: str) -> bool:
        """Return True if a table/view/partition exists for ``name``."""
        schema, table = (None, name)
        if "." in name:
            schema, table = name.split(".", 1)
        schema = schema or "public"
        row = await self._db_fetchrow(
            """
            SELECT 1
              FROM pg_catalog.pg_class c
              JOIN pg_catalog.pg_namespace n
                ON n.oid = c.relnamespace
             WHERE n.nspname = $1
               AND c.relname = $2
               AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
             LIMIT 1;
            """,
            schema,
            table,
        )
        return bool(row)

    async def _sleep_until_boundary(
        self,
        interval: float,
        offset: float = 0.0,
    ) -> None:
        """Sleep until the next aligned boundary for ``interval`` seconds."""
        interval_sec = max(0.001, float(interval))
        normalized_offset = (
            (offset % interval_sec) + interval_sec
        ) % interval_sec
        now = time.time()
        phase = (now - normalized_offset) % interval_sec
        remaining = interval_sec - phase if phase > 1e-6 else interval_sec
        await asyncio.sleep(remaining)

    # ──────────────────────────────────────────────────────────────────────
    #  Intraday helpers (Timescale engine)
    # ──────────────────────────────────────────────────────────────────────

    # ──────────────────────────────────────────────────────────────────────
    #  Intraday session helpers
    # ──────────────────────────────────────────────────────────────────────

    def _session_start_utc(
        self, now_ts: Optional[datetime] = None
    ) -> tuple[pd.Timestamp, date]:
        """Return the 04:00 New York start for the active trading session."""
        tz_ny = ZoneInfo("America/New_York")
        now_ts = now_ts or _utcnow()
        session_date = trading_session_date(now_ts)
        start_local = datetime.combine(
            session_date, dtime(hour=4), tzinfo=tz_ny
        )
        start_utc = pd.to_datetime(start_local, utc=True)
        return start_utc, session_date

    async def _ensure_intraday_table(self) -> None:
        """Create the intraday equity table if missing.

        Lightweight DDL used for self-healing when users drop tables.
        """
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.tbl_equity_intraday} (
            ts                timestamptz PRIMARY KEY,
            deposit           real,
            cumulative_return real,
            drawdown          real,
            realised_return   real,
            updated_at        timestamptz DEFAULT now()
        );
        """
        await self._db_execute(ddl)
        await self._db_execute(
            f"""
            ALTER TABLE {self.tbl_equity_intraday}
            ADD COLUMN IF NOT EXISTS drawdown real
            """
        )

    # ──────────────────────────────────────────────────────────────────────
    #  Market schedule helpers
    # ──────────────────────────────────────────────────────────────────────

    async def _refresh_market_schedule(self) -> int:
        """Synchronise the market schedule table with the Alpaca calendar."""

        try:
            assert self.repo is not None
            count = await sync_schedule_async(
                self.repo,
                self.tbl_market_schedule,
                self.rest,
                lookback_days=self.MARKET_LOOKBACK_DAYS,
                forward_days=self.MARKET_FORWARD_DAYS,
                pre_open_time=self._market_pre_open_time,
                retain_days=180,
            )
            if count:
                self.log.debug("market schedule refreshed: %d rows", count)
            return count
        except Exception as exc:
            self.log.warning("market schedule refresh failed: %s", exc)
            return 0

    async def _schedule_current(
        self,
        ts: Optional[datetime] = None,
    ) -> Optional[SessionWindow]:
        ts = ts or _utcnow()
        return await session_for_timestamp_async(
            self.repo,  # type: ignore[arg-type]
            self.tbl_market_schedule,
            ts,
        )

    async def _schedule_next(
        self,
        ts: Optional[datetime] = None,
    ) -> Optional[SessionWindow]:
        ts = ts or _utcnow()
        return await next_session_after_async(
            self.repo,  # type: ignore[arg-type]
            self.tbl_market_schedule,
            ts,
        )

    async def _needs_recovery(self) -> bool:
        """Return True when core tables are empty and require bootstrap."""

        try:
            row = await self._db_fetchrow(
                f"""
                SELECT
                    (SELECT COUNT(*) FROM {self.tbl_equity})   AS eqc,
                    (SELECT COUNT(*) FROM {self.tbl_live})     AS livec,
                    (SELECT COUNT(*) FROM {self.tbl_closed})   AS closedc
                """
            )
            if not row:
                return True
            equity_rows = row.get("eqc", 0) or 0
            live_rows = row.get("livec", 0) or 0
            closed_rows = row.get("closedc", 0) or 0
        except Exception as exc:
            self.log.debug("recovery check failed: %s", exc)
            return True

        if equity_rows == 0:
            return True

        # Equity present; still recover if both supporting tables disappeared
        return live_rows == 0 and closed_rows == 0

    async def _market_schedule_worker(self) -> None:
        """Keep the market_schedule table in sync with the Alpaca calendar."""

        interval = self.MARKET_REFRESH_SEC
        first = True
        while True:
            if first:
                first = False
            else:
                await asyncio.sleep(interval)
            try:
                await self._refresh_market_schedule()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.debug("market_schedule_worker note: %s", exc)

    async def _bootstrap_for_session(self) -> None:
        """Run the bootstrap sequence before a session starts."""

        async with self._snap_lock:
            self._snapshot_ready = False
            try:
                await self._bootstrap_cash_flows()
            except Exception as exc:
                self.log.debug("bootstrap flows note: %s", exc)

            try:
                await self._sync_closed_trades()
            except Exception as exc:
                self.log.debug("bootstrap closed trades note: %s", exc)

            try:
                await self._initial_snapshot()
            except Exception as exc:
                self.log.debug("bootstrap snapshot note: %s", exc)

            try:
                await self._rebuild_equity_full()
            except Exception as exc:
                self.log.debug("bootstrap equity rebuild note: %s", exc)

            try:
                await self._equity_intraday_backfill()
            except Exception as exc:
                self.log.debug("startup intraday backfill note: %s", exc)

    async def _deactivate_session(self) -> None:
        """Cancel active worker tasks and reset session state."""

        if not self._active_tasks:
            self._active_session = None
            return

        self.log.info(
            "Deactivating session; stopping %d tasks", len(self._active_tasks)
        )
        for task in self._active_tasks:
            task.cancel()
        with contextlib.suppress(Exception):
            await asyncio.gather(*self._active_tasks, return_exceptions=True)
        self._active_tasks.clear()
        self._active_session = None
        self._snapshot_ready = False

    async def _activate_session(self, session: SessionWindow) -> None:
        """Ensure workers are running for the provided session."""

        current = self._active_session
        if current and current.session_date == session.session_date:
            return

        await self._deactivate_session()
        self.log.info(
            "Activating session %s (open %s, close %s)",
            session.session_date,
            session.open_utc.isoformat(),
            session.close_utc.isoformat(),
        )

        await self._bootstrap_for_session()

        tasks = [
            asyncio.create_task(self._price_listener(), name="price_listener"),
            asyncio.create_task(self._db_worker(), name="db_worker"),
            asyncio.create_task(self._snapshot_loop(), name="snapshot_loop"),
            asyncio.create_task(
                self._closed_trades_worker(), name="closed_trades_worker"
            ),
            asyncio.create_task(self._equity_worker(), name="equity_worker"),
            asyncio.create_task(
                self._equity_intraday_worker(), name="equity_intraday"
            ),
            asyncio.create_task(self._cash_flows_worker(), name="cash_flows"),
            asyncio.create_task(
                self._ui_heartbeat_worker(), name="ui_heartbeat"
            ),
            asyncio.create_task(self._run_trade_stream(), name="trade_stream"),
        ]
        self._active_tasks = tasks
        self._active_session = session

    def _spawn_guarded(
        self,
        name: str,
        factory: Callable[[], Awaitable[Any]],
        *,
        stop_event: asyncio.Event,
        backoff: int = 5,
    ) -> asyncio.Task:
        """Run factory in a restart loop so crashes restart the task."""

        delay = max(1, int(backoff))

        async def _runner() -> None:
            while not stop_event.is_set():
                try:
                    await factory()
                    break
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: PERF203 - restart path
                    self.log.exception("%s crashed: %s", name, exc)
                    await asyncio.sleep(delay)

        return asyncio.create_task(_runner(), name=name)

    async def _schedule_supervisor(self, stop_event: asyncio.Event) -> None:
        """Toggle worker sets based on the market schedule table."""

        poll = self.MARKET_POLL_SEC
        disable_sleep = self.disable_session_sleep
        while not stop_event.is_set():
            now = _utcnow()
            self._supervisor_last_tick = now
            next_check = now + timedelta(seconds=poll)
            try:
                session = await self._schedule_current(now)

                if disable_sleep:
                    candidate = (
                        session
                        or self._active_session
                        or await self._schedule_next(now)
                    )
                    if not candidate:
                        await self._refresh_market_schedule()
                        refreshed_now = _utcnow()
                        candidate = await self._schedule_current(
                            refreshed_now
                        ) or await self._schedule_next(refreshed_now)
                    if candidate:
                        await self._activate_session(candidate)
                    else:
                        if await self._needs_recovery():
                            self.log.info(
                                "schedule supervisor: core tables empty – "
                                "running recovery bootstrap"
                            )
                            try:
                                await self._bootstrap_for_session()
                            except Exception as exc:
                                self.log.warning(
                                    "recovery bootstrap failed: %s",
                                    exc,
                                )
                        else:
                            self.log.debug(
                                "schedule supervisor: no session rows; "
                                "retrying"
                            )
                else:
                    if session:
                        await self._activate_session(session)
                        next_check = min(
                            session.post_close_utc,
                            now + timedelta(seconds=poll),
                        )
                    else:
                        await self._deactivate_session()
                        if await self._needs_recovery():
                            self.log.info(
                                "schedule supervisor: core tables empty – "
                                "running recovery bootstrap"
                            )
                            try:
                                await self._bootstrap_for_session()
                            except Exception as exc:
                                self.log.warning(
                                    "recovery bootstrap failed: %s",
                                    exc,
                                )
                            next_check = now + timedelta(seconds=poll)
                        else:
                            next_session = await self._schedule_next(now)
                            if not next_session:
                                await self._refresh_market_schedule()
                                next_session = await self._schedule_next(now)
                            if next_session:
                                next_check = min(
                                    next_session.pre_open_utc,
                                    now + timedelta(seconds=poll),
                                )
                            else:
                                self.log.debug(
                                    "market schedule has no future sessions; "
                                    "retrying"
                                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.exception("schedule supervisor tick failed: %s", exc)

            wait = max(1.0, (next_check - _utcnow()).total_seconds())
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=wait)
            except asyncio.TimeoutError:
                continue
            else:
                break

    # ──────────────────────────────────────────────────────────────────────
    #  Cold‑start bootstrap helpers
    # ──────────────────────────────────────────────────────────────────────

    async def _bootstrap_cash_flows(
        self,
        max_pages: int | None = None,
    ) -> int:
        """Load activities newest→oldest until a page is already present."""

        await self._db_execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.tbl_cash_flows} (
                id          text PRIMARY KEY,
                ts          timestamptz,
                amount      real,
                type        text,
                description text,
                updated_at  timestamptz DEFAULT now()
            );
            """
        )

        async def _page_known(ids: list[str]) -> bool:
            if not ids:
                return True
            placeholders = ", ".join(f"(${i+1})" for i in range(len(ids)))
            sql = f"""
                SELECT v.id
                  FROM (VALUES {placeholders}) AS v(id)
                  JOIN {self.tbl_cash_flows} f
                    ON f.id = v.id
                """
            try:
                rows = await self._db_fetch(sql, *ids)
                return len(rows) == len(ids)
            except Exception:
                return False

        token: Optional[str] = None
        got_total = 0
        pages = 0
        while True:
            if max_pages is not None and pages >= max_pages:
                break
            pages += 1
            try:
                acts = self.rest.get_activities(
                    page_size=100,
                    page_token=token,
                    direction="desc",
                )
            except Exception as exc:
                self.log.debug("flows bootstrap get_activities: %s", exc)
                break
            if not acts:
                break

            rows: list[dict] = []
            ids: list[str] = []
            for a in acts:
                raw = getattr(a, "_raw", None) or getattr(a, "__dict__", {})
                if not isinstance(raw, dict):
                    continue
                aid = str(raw.get("id") or raw.get("activity_id") or "")
                if not aid:
                    continue
                atype = str(raw.get("activity_type") or raw.get("type") or "")
                ts = (
                    raw.get("transaction_time")
                    or raw.get("date")
                    or raw.get("processing_date")
                )
                try:
                    ts_utc = pd.to_datetime(ts, utc=True).to_pydatetime()
                except Exception:
                    continue
                val = (
                    raw.get("net_amount")
                    or raw.get("cash")
                    or raw.get("amount")
                )
                if val is None and str(atype).upper() == "FILL":
                    try:
                        px = float(raw.get("price") or 0.0)
                        qty = float(raw.get("qty") or 0.0)
                        side = str(raw.get("side") or "").lower()
                        if side.startswith("buy"):
                            val = -abs(px * qty)
                        elif side.startswith("sell"):
                            val = abs(px * qty)
                        else:
                            val = px * qty
                    except Exception:
                        val = 0.0
                try:
                    amount = float(val) if val is not None else 0.0
                except Exception:
                    amount = 0.0
                desc = str(raw.get("description") or raw.get("symbol") or "")
                rows.append(
                    {
                        "id": aid,
                        "ts": ts_utc,
                        "amount": amount,
                        "type": atype,
                        "description": desc,
                    }
                )
                ids.append(aid)

            if await _page_known(ids):
                break

            if rows:
                params = [
                    (
                        r["id"],
                        r["ts"],
                        r["amount"],
                        r["type"],
                        r["description"],
                    )
                    for r in rows
                ]
                await self._db_executemany(
                    f"""
                    INSERT INTO {self.tbl_cash_flows}
                        (id, ts, amount, type, description)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (id) DO UPDATE SET
                        ts = EXCLUDED.ts,
                        amount = EXCLUDED.amount,
                        type = EXCLUDED.type,
                        description = EXCLUDED.description,
                        updated_at = now()
                    """,
                    params,
                )
                got_total += len(rows)

            token = getattr(acts, "next_page_token", None)
            if not token:
                try:
                    token = rows[-1]["id"] if rows else None
                except Exception:
                    token = None
            if not token:
                break

        if got_total:
            self.log.info(
                "flows bootstrap upserted %d rows (%d pages)", got_total, pages
            )
        else:
            self.log.info("flows bootstrap: nothing to ingest")
        return got_total

    async def _rebuild_equity_full(self) -> None:
        """Rebuild daily equity using full cash-flows history."""
        cfg = {
            "CONN_STRING_POSTGRESQL": self.c.POSTGRESQL_LIVE_SQLALCHEMY,
            "API_KEY": self.c.API_KEY,
            "SECRET_KEY": self.c.SECRET_KEY,
            "ALPACA_BASE_URL": self.c.ALPACA_BASE_URL,
            "TABLE_ACCOUNT_CLOSED": self.tbl_closed,
            "TABLE_ACCOUNT_POSITIONS": self.tbl_live,
            "TABLE_ACCOUNT_EQUITY_FULL": self.tbl_equity,
            "CASH_FLOW_TYPES": os.getenv(
                "CASH_FLOW_TYPES",
                (
                    "CSD,CSW,JNLC,ACATC,ACATS,FEE,CFEE,DIV,DIVCGL,DIVCGS,"
                    "DIVNRA,DIVROC,DIVTXEX,DIVWH,INT,INTPNL"
                ),
            ),
        }
        try:
            df = await rebuild_equity_series_async(cfg, repo=self.repo)
            self.log.info(
                "equity rebuild: %d days from %s to %s",
                len(df.index) if df is not None else 0,
                (
                    str(df["date"].min())
                    if df is not None and not df.empty
                    else "-"
                ),
                (
                    str(df["date"].max())
                    if df is not None and not df.empty
                    else "-"
                ),
            )
        except Exception as exc:
            self.log.warning("equity rebuild failed: %s", exc)

    # ──────────────────────────────────────────────────────────────────────
    #  Intraday equity backfill (04:00 NY → now)
    # ──────────────────────────────────────────────────────────────────────

    async def _equity_intraday_backfill(self) -> None:
        """Rebuild intraday deposits for the current trading day only."""
        try:
            now_dt = _utcnow()
            start_utc, session_date = self._session_start_utc(now_dt)
            now_ts = pd.Timestamp(now_dt)
            floor_min = now_ts.floor("min")
            last_complete = (floor_min - pd.Timedelta(seconds=5)).floor("min")
            cutoff = last_complete if last_complete >= start_utc else start_utc
            now_pydt = now_ts.to_pydatetime()

            prev_row = await self._db_fetchrow(
                f"""
                SELECT deposit, unrealised_pl, cumulative_return
                  FROM {self.tbl_equity}
                 WHERE date < $1
              ORDER BY date DESC
                 LIMIT 1
                """,
                session_date,
            )
            if not prev_row:
                return

            prev_deposit = float(prev_row.get("deposit") or 0.0)
            prev_unreal = float(prev_row.get("unrealised_pl") or 0.0)
            try:
                prev_cum_ret = float(prev_row.get("cumulative_return") or 0.0)
                if not math.isfinite(prev_cum_ret):
                    prev_cum_ret = 0.0
            except Exception:
                prev_cum_ret = 0.0

            idx = pd.date_range(start_utc, cutoff, freq="min", tz="UTC")
            if idx.empty:
                idx = pd.DatetimeIndex([start_utc])

            closed_rows = await self._db_fetch(
                f"""
                SELECT symbol, qty, side,
                       entry_time AT TIME ZONE 'UTC' AS entry_time,
                       exit_time  AT TIME ZONE 'UTC' AS exit_time,
                       entry_price, pnl_cash
                  FROM {self.tbl_closed}
                 WHERE (exit_time AT TIME ZONE 'America/New_York')::date = $1
                """,
                session_date,
            )
            closed_today = pd.DataFrame(closed_rows)

            open_rows = await self._db_fetch(
                f"""
                SELECT symbol, qty,
                       filled_at AT TIME ZONE 'UTC' AS filled_at,
                       avg_fill AS basis,
                       profit_loss,
                       profit_loss_lot
                  FROM {self.tbl_live}
                 WHERE qty <> 0
                """
            )
            open_df = pd.DataFrame(open_rows)

            try:
                raw_types = os.getenv(
                    "CASH_FLOW_TYPES",
                    (
                        "CSD,CSW,JNLC,ACATC,ACATS,FEE,CFEE,DIV,DIVCGL,"
                        "DIVCGS,DIVNRA,DIVROC,DIVTXEX,DIVWH,INT,INTPNL"
                    ),
                )
                pats = [
                    t.strip().upper()
                    for t in raw_types.split(",")
                    if t.strip()
                ]
                where_types = (
                    " OR ".join([f"type ILIKE '{p}%'" for p in pats])
                    or "FALSE"
                )
                flow_rows = await self._db_fetch(
                    f"""
                    SELECT ts AT TIME ZONE 'UTC' AS ts, amount
                      FROM {self.tbl_cash_flows}
                     WHERE ts >= $1 AND ts <= $2
                       AND ({where_types})
                  ORDER BY ts
                    """,
                    start_utc.to_pydatetime(),
                    now_pydt,
                )
                flows = pd.DataFrame(flow_rows)
            except Exception:
                flows = pd.DataFrame(columns=["ts", "amount"])

            cutoff_ts = cutoff.to_pydatetime()
            if not closed_today.empty:
                closed_today = closed_today[
                    pd.to_datetime(closed_today["exit_time"], utc=True)
                    <= cutoff_ts
                ]

            if closed_today.empty:
                rcum = pd.Series(0.0, index=idx)
            else:
                exit_floor = pd.to_datetime(
                    closed_today["exit_time"], utc=True
                ).dt.floor("min")
                rcum = (
                    closed_today.assign(_m=exit_floor)
                    .groupby("_m")["pnl_cash"]
                    .sum()
                    .reindex(idx, fill_value=0.0)
                    .cumsum()
                )

            syms = sorted(
                set(
                    open_df.get("symbol", pd.Series(dtype=str))
                    .dropna()
                    .unique()
                )
                | set(
                    closed_today.get("symbol", pd.Series(dtype=str))
                    .dropna()
                    .unique()
                )
            )
            px_cols: dict[str, pd.Series] = {}
            if syms:
                for sym in syms:
                    bars_rows = await self._ts_fetch(
                        """
                        SELECT timestamp AT TIME ZONE 'UTC' AS ts, close
                          FROM public.alpaca_bars_1min
                        WHERE symbol = $1
                          AND timestamp >= $2
                          AND timestamp <= $3
                      ORDER BY timestamp
                        """,
                        sym,
                        start_utc.to_pydatetime(),
                        cutoff_ts,
                    )
                    pre_px_row = await self._ts_fetchrow(
                        """
                        SELECT close
                          FROM public.alpaca_bars_1min
                         WHERE symbol = $1 AND timestamp <= $2
                      ORDER BY timestamp DESC
                         LIMIT 1
                        """,
                        sym,
                        start_utc.to_pydatetime(),
                    )
                    pre_px = (
                        float(pre_px_row.get("close"))
                        if pre_px_row and pre_px_row.get("close") is not None
                        else None
                    )
                    bars = pd.DataFrame(bars_rows)
                    if (bars is None or bars.empty) and pre_px is not None:
                        bars = pd.DataFrame(
                            {
                                "ts": [start_utc.to_pydatetime()],
                                "close": [float(pre_px)],
                            }
                        )
                    elif not bars.empty and pre_px is not None:
                        first_ts = pd.to_datetime(bars["ts"].iloc[0], utc=True)
                        if first_ts > start_utc:
                            seed = pd.DataFrame(
                                {
                                    "ts": [start_utc.to_pydatetime()],
                                    "close": [float(pre_px)],
                                }
                            )
                            bars = pd.concat([seed, bars], ignore_index=True)

                    if not bars.empty:
                        series = (
                            bars.assign(
                                ts=pd.to_datetime(bars["ts"], utc=True)
                            )
                            .drop_duplicates("ts")
                            .set_index("ts")["close"]
                            .reindex(idx)
                            .ffill()
                        )
                        px_cols[sym] = series

            upl = pd.Series(0.0, index=idx)
            for _, row in open_df.iterrows():
                sym = str(row.get("symbol") or "")
                if not sym or sym not in px_cols:
                    continue
                qty = float(row.get("qty") or 0.0)
                if qty == 0:
                    continue
                basis = float(row.get("basis") or 0.0)
                filled_at = pd.to_datetime(row.get("filled_at"), utc=True)
                filled_floor = max(filled_at.floor("min"), start_utc)
                diff = px_cols[sym] - basis
                mask = idx >= filled_floor
                upl = upl.add(diff.where(mask, 0.0) * qty, fill_value=0.0)

            if not closed_today.empty:
                for _, row in closed_today.iterrows():
                    sym = str(row.get("symbol") or "")
                    if not sym or sym not in px_cols:
                        continue
                    qty = float(row.get("qty") or 0.0)
                    if qty == 0:
                        continue
                    side = str(row.get("side") or "long").lower()
                    signed = -qty if side.startswith("short") else qty
                    basis = float(row.get("entry_price") or 0.0)
                    entry = pd.to_datetime(row.get("entry_time"), utc=True)
                    exit_ = pd.to_datetime(row.get("exit_time"), utc=True)
                    entry_floor = max(entry.floor("min"), start_utc)
                    exit_floor = min(exit_.floor("min"), cutoff_ts)
                    if entry_floor >= exit_floor:
                        continue
                    diff = px_cols[sym] - basis
                    mask = (idx >= entry_floor) & (idx < exit_floor)
                    upl = upl.add(
                        diff.where(mask, 0.0) * signed, fill_value=0.0
                    )

            if flows is not None and not flows.empty:
                flow_floor = pd.to_datetime(flows["ts"], utc=True).dt.floor(
                    "min"
                )
                fser = (
                    flows.assign(_m=flow_floor)
                    .groupby("_m")["amount"]
                    .sum()
                    .reindex(idx, fill_value=0.0)
                    .cumsum()
                )
            else:
                fser = pd.Series(0.0, index=idx)

            deposit_series = (
                prev_deposit + rcum + (upl - prev_unreal) + fser
            ).astype(float)

            step = max(1, int(self.EQUITY_INTRADAY_SEC))
            tick_idx = pd.date_range(
                start_utc, cutoff, freq=f"{step}s", tz="UTC"
            )
            if tick_idx.empty:
                tick_idx = pd.DatetimeIndex([cutoff])

            dep_ticks = deposit_series.reindex(tick_idx, method="ffill")
            dep_ticks = dep_ticks.ffill().astype(float)
            base_cap = prev_deposit if prev_deposit != 0.0 else 1.0
            daily_ret = dep_ticks / base_cap - 1.0
            cum_ticks = (1.0 + prev_cum_ret) * (1.0 + daily_ret) - 1.0
            peak_ticks = dep_ticks.cummax().replace(0.0, np.nan)
            dd_ticks = (dep_ticks / peak_ticks) - 1.0
            dd_ticks = dd_ticks.fillna(0.0)

            records = [
                {
                    "ts": ts.to_pydatetime(),
                    "dep": float(dep_ticks.iat[i]),
                    "cum": float(cum_ticks.iat[i]),
                    "dd": (
                        float(dd_ticks.iat[i])
                        if math.isfinite(dd_ticks.iat[i])
                        else 0.0
                    ),
                    "rr": 0.0,
                }
                for i, ts in enumerate(tick_idx)
            ]

            await self._ensure_intraday_table()
            params = [
                (r["ts"], r["dep"], r["cum"], r["dd"], r["rr"])
                for r in records
            ]
            await self._db_executemany(
                f"""
                INSERT INTO {self.tbl_equity_intraday}
                    (ts, deposit, cumulative_return, drawdown, realised_return)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (ts) DO UPDATE SET
                    deposit = EXCLUDED.deposit,
                    cumulative_return = EXCLUDED.cumulative_return,
                    drawdown = EXCLUDED.drawdown,
                    realised_return = EXCLUDED.realised_return,
                    updated_at = now()
                """,
                params,
            )

        except Exception as exc:
            self.log.warning("equity_intraday_backfill failed: %s", exc)

    # ──────────────────────────────────────────────────────────────────────
    #  Schema ensure (DDL + helpful indexes)
    # ──────────────────────────────────────────────────────────────────────

    async def _ensure_tables(self) -> None:
        """Create live & closed tables if missing (no-ops otherwise)."""

        def _schema_of(tbl: str) -> Optional[str]:
            return tbl.split(".", 1)[0] if "." in tbl else None

        def _idx_name(tbl: str, suffix: str) -> str:
            return f"idx_{tbl.replace('.', '_')}_{suffix}"

        schema = _schema_of(self.tbl_live) or _schema_of(self.tbl_closed)
        if schema:
            await self._db_execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

        async def _ensure_table(name: str, ddl: str) -> None:
            if await self._relation_exists(name):
                return
            if await self._type_exists(name):
                try:
                    await self._drop_type(name)
                except Exception as exc:  # pragma: no cover
                    self.log.warning("drop type %s failed: %s", name, exc)
                    raise
            await self._db_execute(ddl)

        async with self._db_lock:
            await _ensure_table(
                self.tbl_live,
                f"""
                CREATE TABLE {self.tbl_live} (
                    parent_id        text PRIMARY KEY,
                    symbol           text,
                    filled_at        timestamptz,
                    qty              integer,
                    avg_fill         real,
                    avg_px_symbol    real,
                    sl_child         text,
                    sl_px            real,
                    tp_child         text,
                    tp_px            real,
                    mkt_px           real,
                    moved_flag       text DEFAULT '—',
                    buy_value        real,
                    holding_days     integer,
                    mkt_value        real,
                    profit_loss      real,
                    profit_loss_lot  real,
                    tp_sl_reach_pct  real,
                    updated_at       timestamptz DEFAULT now()
                );
                """,
            )
            await _ensure_table(
                self.tbl_closed,
                f"""
                CREATE TABLE {self.tbl_closed} (
                    entry_lot_id   text,
                    exit_order_id  text,
                    exit_parent_id text,
                    symbol         text,
                    side           text,
                    qty            real,
                    entry_time     timestamptz,
                    entry_price    real,
                    exit_time      timestamptz,
                    exit_price     real,
                    exit_type      text,
                    pnl_cash       real,
                    pnl_pct        real,
                    return_pct     real,
                    duration_sec   real,
                    UNIQUE (entry_lot_id, exit_order_id)
                );
                """,
            )
            await _ensure_table(
                self.tbl_equity_intraday,
                f"""
                CREATE TABLE {self.tbl_equity_intraday} (
                    ts                timestamptz PRIMARY KEY,
                    deposit           real,
                    cumulative_return real,
                    drawdown          real,
                    realised_return   real,
                    updated_at        timestamptz DEFAULT now()
                );
                """,
            )
            await self._db_execute(
                f"""
                ALTER TABLE {self.tbl_equity_intraday}
                ADD COLUMN IF NOT EXISTS drawdown real;
                """
            )
            await _ensure_table(
                self.tbl_cash_flows,
                f"""
                CREATE TABLE {self.tbl_cash_flows} (
                    id          text PRIMARY KEY,
                    ts          timestamptz,
                    amount      real,
                    type        text,
                    description text,
                    updated_at  timestamptz DEFAULT now()
                );
                """,
            )
            await _ensure_table(
                self.tbl_market_schedule,
                f"""
                CREATE TABLE {self.tbl_market_schedule} (
                    session_date    date PRIMARY KEY,
                    pre_open_utc    timestamptz,
                    open_utc        timestamptz,
                    close_utc       timestamptz,
                    post_close_utc  timestamptz,
                    updated_at      timestamptz DEFAULT now()
                );
                """,
            )
            await self._db_execute(
                f"""
                CREATE INDEX IF NOT EXISTS
                    {_idx_name(self.tbl_market_schedule, 'open')}
                ON {self.tbl_market_schedule} (open_utc);
                """
            )
            await self._db_execute(
                f"""
                CREATE INDEX IF NOT EXISTS
                    {_idx_name(self.tbl_live, 'symbol')}
                ON {self.tbl_live} (symbol);
                """
            )
            await self._db_execute(
                f"""
                CREATE INDEX IF NOT EXISTS
                    {_idx_name(self.tbl_closed, 'symbol_time')}
                ON {self.tbl_closed} (symbol, entry_time);
                """
            )

    # ──────────────────────────────────────────────────────────────────────
    #  Tracing & small utilities
    # ──────────────────────────────────────────────────────────────────────

    def _trace(self, msg: str, **kwargs: Any) -> None:
        """Structured debug logging gated by logger level."""
        try:
            payload = json.dumps(kwargs, default=_json_safe)
        except Exception:
            payload = repr(kwargs)
        self.log.debug("TRACE %s | %s", msg, payload)

    def _log_if_changed(
        self, before: Dict[str, Any], after: Dict[str, Any]
    ) -> None:
        """INFO-level diff for watch fields."""
        watch = (
            "qty",
            "avg_fill",
            "avg_px_symbol",
            "sl_px",
            "tp_px",
            "moved_flag",
        )
        diffs = {
            k: (before.get(k), after.get(k))
            for k in watch
            if before.get(k) != after.get(k)
        }
        if diffs:
            self.log.info(
                "UPDATE %-5s %s  %s",
                after.get("symbol"),
                after.get("parent_id"),
                ", ".join(f"{k}:{a}→{b}" for k, (a, b) in diffs.items()),
            )

    # ──────────────────────────────────────────────────────────────────────
    #  Business rules: break-even & KPI refresh
    # ──────────────────────────────────────────────────────────────────────

    def _is_at_break_even(
        self, side: str, entry: float, stop_px: Optional[float]
    ) -> bool:
        """Return True if stop is at/inside break-even, respecting ticks."""
        if stop_px is None:
            return False

        gap = max(self.c.MIN_STOP_GAP, 0.0001 if entry < 1 else 0.01)
        tick = 0.0001 if entry < 1 else 0.01

        # Round legal BE level to tick and decimals as previously implemented.
        legal_be = entry - gap if side.lower() == "long" else entry + gap
        steps = round(legal_be / tick)
        legal_be = round(steps * tick, 4 if entry < 1 else 2)

        if side.lower() == "long":
            return stop_px >= legal_be
        return stop_px <= legal_be

    def _refresh_row_metrics(
        self, row: Dict[str, Any], now_ts: datetime
    ) -> None:
        """Compute KPIs for a row: values, P&L, reach %, holding days."""
        qty = row.get("qty", 0)
        entry_px = row.get("avg_fill", 0.0)
        # Use broker symbol-average price for P/L basis when available.
        basis_px = (
            row.get("avg_px_symbol")
            if row.get("avg_px_symbol") is not None
            else entry_px
        )
        mkt_px = row.get("mkt_px")
        tp_px = row.get("tp_px")
        sl_px = row.get("sl_px")

        if mkt_px is None or qty == 0:
            return

        row["buy_value"] = qty * entry_px
        row["mkt_value"] = qty * mkt_px
        # P/L uses broker WAC basis so UI totals match broker unrealised P/L
        row["profit_loss"] = qty * (mkt_px - basis_px)
        # Per-lot P/L (legacy definition): Market Value − Buy Value
        row["profit_loss_lot"] = row["mkt_value"] - row["buy_value"]

        try:
            # type: ignore[arg-type]
            row["holding_days"] = (now_ts - row["filled_at"]).days
        except Exception:
            row["holding_days"] = None

        # Progress towards TP/SL based on per-lot P/L sign (entry-based)
        target_px = tp_px if row["profit_loss_lot"] >= 0 else sl_px
        if target_px and not math.isclose(target_px, entry_px):
            ratio = (mkt_px - entry_px) / (target_px - entry_px) * 100
            row["tp_sl_reach_pct"] = round(ratio, 2)
        else:
            row["tp_sl_reach_pct"] = None

    # ──────────────────────────────────────────────────────────────────────
    #  Postgres upsert/delete + NOTIFY
    # ──────────────────────────────────────────────────────────────────────

    async def _upsert(
        self, row: Dict[str, Any], *, force_push: bool = False
    ) -> None:
        """Upsert row to live table and optionally NOTIFY the UI.

        Push if:
        - force_push=True, or
        - price moved ≥ UI_PUSH_PCT_THRESHOLD vs last pushed, or
        - structural fields changed: qty, avg_fill, avg_px_symbol, sl_child,
          sl_px, tp_child, tp_px, moved_flag
        """
        cols = DB_COLS
        vals = [row.get(c) for c in cols]
        sets = ",".join(f"{c}=EXCLUDED.{c}" for c in cols if c != "parent_id")

        parent_id = row["parent_id"]
        new_px = row.get("mkt_px")
        prev_state = self.state.get(parent_id)
        last_px = (prev_state or {}).get("_last_pushed_px")

        push_needed = force_push or (
            last_px is None
            or (
                new_px
                and last_px
                and abs(new_px - last_px) / last_px * 100
                >= self.UI_PUSH_PCT_THRESHOLD
            )
        )

        if not push_needed and prev_state:
            for k in (
                "qty",
                "avg_fill",
                "avg_px_symbol",
                "sl_child",
                "sl_px",
                "tp_child",
                "tp_px",
                "moved_flag",
            ):
                if row.get(k) != prev_state.get(k):
                    push_needed = True
                    break

        placeholders = ", ".join(f"${i}" for i in range(1, len(cols) + 1))
        await self._db_execute(
            f"""
            INSERT INTO {self.tbl_live} ({','.join(cols)})
            VALUES ({placeholders})
            ON CONFLICT (parent_id) DO UPDATE SET {sets};
            """,
            *vals,
        )
        if push_needed:
            payload = {c: _json_safe(row.get(c)) for c in cols}
            await self._db_execute(
                "SELECT pg_notify($1, $2);",
                self.pos_channel,
                json.dumps({"row": payload}),
            )
            row["_last_pushed_px"] = new_px
        self.state[parent_id] = row  # cache full row

    async def _delete(
        self,
        parent_id: str,
        *,
        skip_closed: bool = False,
        exit_px: Optional[float] = None,
        exit_type: str = "MANUAL",
    ) -> None:
        """Delete parent and notify UI; optionally write closed row."""
        row = self.state.get(parent_id)

        if row and not skip_closed:
            cp = row.copy()
            cp["qty"] = 0.0
            cp["mkt_px"] = exit_px or cp.get("mkt_px") or cp["avg_fill"]
            self._refresh_row_metrics(cp, _utcnow())
            await self._record_closed_trade_now(cp, exit_type=exit_type)

        await self._db_execute(
            f"DELETE FROM {self.tbl_live} WHERE parent_id = $1;",
            parent_id,
        )
        await self._db_execute(
            "SELECT pg_notify($1, $2);",
            self.pos_channel,
            json.dumps({"op": "delete", "parent_id": parent_id}),
        )

        # In-memory cleanup.
        self.state.pop(parent_id, None)
        sym = row.get("symbol") if row else None
        if sym:
            s = self.sym2pid.get(sym)
            if s:
                s.discard(parent_id)
                if not s:
                    self.sym2pid.pop(sym, None)

    # ──────────────────────────────────────────────────────────────────────
    #  Last trade price helper (optional fallback)
    # ──────────────────────────────────────────────────────────────────────

    def _last_trade_px(self, symbol: str) -> Optional[float]:
        """Get last trade price via REST; return None on any error."""
        try:
            if hasattr(self.rest, "get_latest_trade"):
                return float(self.rest.get_latest_trade(symbol).price)
            return float(self.rest.get_last_trade(symbol).price)
        except Exception:
            return None

    async def _sync_avg_px_symbol(self, symbol: str) -> Optional[float]:
        """Refresh broker WAC basis (avg_px_symbol) for all open parents.

        Fetches current position via REST; if available, updates each in-memory
        row and recomputes P/L so UI totals align without waiting for snapshot.
        Returns the fetched basis (or None on failure).
        """
        try:
            pos = self.rest.get_position(symbol)
            basis = float(getattr(pos, "avg_entry_price", 0.0) or 0.0)
            if basis <= 0:
                return None
        except Exception:
            return None

        now_ts = _utcnow()
        for pid in list(self.sym2pid.get(symbol, set())):
            row = self.state.get(pid)
            if not row:
                continue
            if row.get("avg_px_symbol") != basis:
                old = row.copy()
                row["avg_px_symbol"] = basis
                self._refresh_row_metrics(row, now_ts)
                self._log_if_changed(old, row)
                await self._upsert(row)
        return basis

    def _pull_orders(self, start_iso: str, end_iso: str) -> pd.DataFrame:
        """Unified order retrieval using account_utils.fetch_all_orders.

        Returns a DataFrame where child legs are separate rows and the
        parent link column is guaranteed to be named 'parent_id'.
        """
        df = fetch_all_orders(
            api=self.rest,
            start=start_iso,
            end=end_iso,
            page_size=500,
            show_progress=False,
        )
        if "parent_id" not in df.columns and "parent_order_id" in df.columns:
            df = df.rename(columns={"parent_order_id": "parent_id"})
        if "parent_id" not in df.columns:
            df["parent_id"] = pd.NA
        if "id" in df.columns:
            df = df.drop_duplicates(subset=["id"]).reset_index(drop=True)
        return df

    # ──────────────────────────────────────────────────────────────────────
    #  Closed-trades sync (watermark window, idempotent)
    # ──────────────────────────────────────────────────────────────────────

    async def _sync_closed_trades(self) -> None:
        """Recompute from a fixed safe anchor to ensure lots are intact."""
        start_iso = "2025-07-01T00:00:00Z"
        end_iso = _fmt_ts_alpaca(_utcnow())

        orders_df = self._pull_orders(start_iso, end_iso)
        if orders_df.empty:
            return

        trades = closed_trades_fifo_from_orders(orders_df)
        if trades.empty:
            return

        trades = trades.copy()
        if "symbol" in trades.columns:
            trades["symbol"] = trades["symbol"].astype(str).str.upper()

        for c in ("entry_time", "exit_time"):
            if c in trades.columns:
                trades[c] = pd.to_datetime(
                    trades[c], utc=True, errors="coerce"
                )

        trades["return_pct"] = trades.get("pnl_pct")

        for idc in ("entry_lot_id", "exit_order_id"):
            if idc not in trades.columns:
                trades[idc] = None
            trades[idc] = trades[idc].astype(str)
        if "exit_parent_id" not in trades.columns:
            trades["exit_parent_id"] = None
        trades["exit_parent_id"] = trades["exit_parent_id"].replace("", pd.NA)

        cols = [
            "entry_lot_id",
            "exit_order_id",
            "exit_parent_id",
            "symbol",
            "side",
            "qty",
            "entry_time",
            "entry_price",
            "exit_time",
            "exit_price",
            "exit_type",
            "pnl_cash",
            "pnl_pct",
            "return_pct",
            "duration_sec",
        ]
        trades = trades.reindex(columns=cols)

        def _py(v):
            if isinstance(v, pd.Timestamp):
                return v.to_pydatetime(warn=False)
            try:
                if pd.isna(v):
                    return None
            except Exception:
                pass
            return v

        values = [
            tuple(_py(trades.loc[i, c]) for c in cols) for i in trades.index
        ]
        sets = ", ".join(
            f"{c}=EXCLUDED.{c}"
            for c in cols
            if c not in {"entry_lot_id", "exit_order_id"}
        )

        await self._db_executemany(
            f"""
            INSERT INTO {self.tbl_closed} ({','.join(cols)})
            VALUES ({','.join(f'${i}' for i in range(1, len(cols) + 1))})
            ON CONFLICT (entry_lot_id, exit_order_id)
            DO UPDATE SET {sets};
            """,
            values,
        )
        await self._db_execute(
            "SELECT pg_notify($1, 'refresh');",
            self.closed_channel,
        )

        self._closed_last_sync = _utcnow()

    # ──────────────────────────────────────────────────────────────────────
    #  Snapshot building (full & per-symbol)
    # ──────────────────────────────────────────────────────────────────────

    async def _initial_snapshot(self) -> None:
        """Build a snapshot of all open parents with KPI columns pre-filled."""
        self.log.info("Bootstrap: scanning live positions …")
        pos_live = {p.symbol: p._raw for p in self.rest.list_positions()}
        # Derive side per symbol from broker snapshot qty sign.
        side_map: Dict[str, str] = {}
        for sym, raw in pos_live.items():
            try:
                q = float(raw.get("qty", 0) or 0.0)
            except Exception:
                q = 0.0
            side_map[str(sym).upper()] = "short" if q < 0 else "long"
        if not pos_live:
            self.log.info("No live positions → nothing to bootstrap.")
            self._snapshot_last_complete = _utcnow()
            self._snapshot_ready = True
            return

        symbols = list(pos_live)
        self._trace(
            "snapshot:positions", count=len(pos_live), symbols=symbols[:25]
        )

        orders_df = self._pull_orders(
            "2025-07-01T00:00:00Z",
            _fmt_ts_alpaca(_utcnow()),
        )

        # Restrict to active symbols to preserve performance
        sym_set = {str(s).upper() for s in symbols}
        orders_df = orders_df[
            orders_df["symbol"].astype(str).str.upper().isin(sym_set)
        ].copy()

        # Build authoritative open positions from orders + broker reconcile
        open_df = build_open_positions_df(orders_df, self.rest)

        # Compute deletes for parents that disappeared
        prev_ids = set(self.state.keys())
        new_ids: set[str] = set()

        if open_df is None or open_df.empty:
            for pid in list(prev_ids):
                await self._delete(pid, skip_closed=True)
            self.log.info("Bootstrap stored 0 open parents.")
            self._snapshot_last_complete = _utcnow()
            self._snapshot_ready = True
            return

        now_utc = _utcnow()
        kept = 0

        for _, r in open_df.iterrows():
            try:
                parent_id = (
                    str(r["Parent ID"])
                    if pd.notna(r.get("Parent ID"))
                    else None
                )
                if not parent_id:
                    continue
                symbol = (
                    str(r["Symbol"]).upper()
                    if pd.notna(r.get("Symbol"))
                    else None
                )
                if not symbol:
                    continue

                filled_at_ts = pd.to_datetime(
                    r.get("Buy Date"), utc=True, errors="coerce"
                )
                qty = int(float(r.get("Buy Qty") or 0))
                avg_fill = (
                    float(r.get("Buy Price"))
                    if pd.notna(r.get("Buy Price"))
                    else None
                )
                avg_px_symbol = (
                    float(r.get("_avg_px_symbol"))
                    if "_avg_px_symbol" in r
                    and pd.notna(r.get("_avg_px_symbol"))
                    else avg_fill
                )
                sl_px = (
                    float(r.get("Stop_Loss_Price"))
                    if pd.notna(r.get("Stop_Loss_Price"))
                    else None
                )
                tp_px = (
                    float(r.get("Take_Profit_Price"))
                    if pd.notna(r.get("Take_Profit_Price"))
                    else None
                )
                mkt_px = (
                    float(r.get("Current_Price"))
                    if pd.notna(r.get("Current_Price"))
                    else None
                )
                if mkt_px is None:
                    # Fallback to last trade price if snapshot lacks it
                    mkt_px = self._last_trade_px(symbol)

                row = {
                    "parent_id": parent_id,
                    "symbol": symbol,
                    # Keep side for BE/moved_flag logic; not persisted.
                    "side": side_map.get(symbol, "long"),
                    "filled_at": (
                        filled_at_ts.to_pydatetime()
                        if pd.notna(filled_at_ts)
                        else None
                    ),
                    "qty": qty,
                    "avg_fill": avg_fill,
                    "avg_px_symbol": avg_px_symbol,
                    "sl_child": None,
                    "sl_px": sl_px,
                    "tp_child": None,
                    "tp_px": tp_px,
                    "mkt_px": mkt_px,
                    "moved_flag": "—",
                    "buy_value": None,
                    "holding_days": None,
                    "mkt_value": None,
                    "profit_loss": None,
                    "profit_loss_lot": None,
                    "tp_sl_reach_pct": None,
                    "updated_at": now_utc,
                }

                # Maintain symbol → parent_id index and in-memory cache
                self.sym2pid.setdefault(symbol, set()).add(parent_id)

                # Compute BE flag if entry and stop are known
                try:
                    if (
                        row.get("sl_px") is not None
                        and row.get("avg_fill") is not None
                    ):
                        row["moved_flag"] = (
                            "OK"
                            if self._is_at_break_even(
                                row.get("side", "long"),
                                float(row["avg_fill"]),
                                float(row["sl_px"]),
                            )
                            else "—"
                        )
                except Exception:
                    pass

                # Compute KPIs that depend on mkt_px & entry
                self._refresh_row_metrics(row, now_utc)

                await self._upsert(row, force_push=True)
                kept += 1
                new_ids.add(parent_id)
            except Exception:
                self.log.exception(
                    "Snapshot: failed to upsert parent %s", r.get("Parent ID")
                )

        # Remove rows that disappeared (no closed-write yet)
        gone = prev_ids - new_ids
        for pid in gone:
            await self._delete(pid, skip_closed=True)

        self.log.info("Bootstrap stored %d open parents.", kept)
        self._trace(
            "snapshot:open_df",
            rows=int(open_df.shape[0]),
            kept=kept,
            symbols=len(sym_set),
        )
        # Mark snapshot as ready so symbol refreshes may run safely
        self._snapshot_last_complete = _utcnow()
        self._snapshot_ready = True

    async def _refresh_symbol_snapshot(self, symbol: str) -> None:
        """Rebuild open parents for a single symbol and upsert to DB/UI.

        This uses the same authoritative pipeline as the full snapshot
        but limits work to one symbol. It is safe to call frequently; a
        simple in‑flight guard prevents overlapping refreshes per symbol.
        """
        if not symbol:
            return
        sym = str(symbol).upper()

        # Do not run symbol refresh until initial snapshot has finished.
        if not getattr(self, "_snapshot_ready", False):
            return

        # In‑flight guard to avoid concurrent refreshes for the symbol.
        if sym in self._sym_refresh_inflight:
            return
        self._sym_refresh_inflight.add(sym)
        try:
            orders_df = self._pull_orders(
                "2025-07-01T00:00:00Z", _fmt_ts_alpaca(_utcnow())
            )
            if orders_df.empty:
                # No orders in window → remove any stale rows for symbol.
                for pid in list(self.sym2pid.get(sym, set())):
                    await self._delete(pid, skip_closed=True)
                return

            # Restrict to this symbol
            df_sym = orders_df[
                orders_df["symbol"].astype(str).str.upper() == sym
            ].copy()
            if df_sym.empty:
                for pid in list(self.sym2pid.get(sym, set())):
                    await self._delete(pid, skip_closed=True)
                return

            open_df = build_open_positions_df(df_sym, self.rest)
            now_utc = _utcnow()
            new_ids: set[str] = set()

            # Determine side from broker position sign if available.
            def _side_for_symbol(s: str) -> str:
                try:
                    pos = self.rest.get_position(s)
                    q = float(getattr(pos, "qty", 0.0) or 0.0)
                    return "short" if q < 0 else "long"
                except Exception:
                    return "long"

            if open_df is not None and not open_df.empty:
                for _, r in open_df.iterrows():
                    try:
                        parent_id = str(r.get("Parent ID") or "")
                        if not parent_id:
                            continue
                        filled_at_ts = pd.to_datetime(
                            r.get("Buy Date"), utc=True, errors="coerce"
                        )
                        qty = int(float(r.get("Buy Qty") or 0))
                        avg_fill = (
                            float(r.get("Buy Price"))
                            if pd.notna(r.get("Buy Price"))
                            else None
                        )
                        avg_px_symbol = (
                            float(r.get("_avg_px_symbol"))
                            if (
                                "_avg_px_symbol" in r
                                and pd.notna(r.get("_avg_px_symbol"))
                            )
                            else avg_fill
                        )
                        sl_px = (
                            float(r.get("Stop_Loss_Price"))
                            if pd.notna(r.get("Stop_Loss_Price"))
                            else None
                        )
                        tp_px = (
                            float(r.get("Take_Profit_Price"))
                            if pd.notna(r.get("Take_Profit_Price"))
                            else None
                        )
                        mkt_px = (
                            float(r.get("Current_Price"))
                            if pd.notna(r.get("Current_Price"))
                            else None
                        )
                        if mkt_px is None:
                            mkt_px = self._last_trade_px(sym)

                        row = {
                            "parent_id": parent_id,
                            "symbol": sym,
                            "side": _side_for_symbol(sym),
                            "filled_at": (
                                filled_at_ts.to_pydatetime()
                                if pd.notna(filled_at_ts)
                                else None
                            ),
                            "qty": qty,
                            "avg_fill": avg_fill,
                            "avg_px_symbol": avg_px_symbol,
                            "sl_child": None,
                            "sl_px": sl_px,
                            "tp_child": None,
                            "tp_px": tp_px,
                            "mkt_px": mkt_px,
                            "moved_flag": "—",
                            "buy_value": None,
                            "holding_days": None,
                            "mkt_value": None,
                            "profit_loss": None,
                            "profit_loss_lot": None,
                            "tp_sl_reach_pct": None,
                            "updated_at": now_utc,
                        }
                        # If we have entry price and stop, compute BE flag now
                        try:
                            if (
                                row.get("sl_px") is not None
                                and row.get("avg_fill") is not None
                            ):
                                row["moved_flag"] = (
                                    "OK"
                                    if self._is_at_break_even(
                                        row.get("side", "long"),
                                        float(row["avg_fill"]),
                                        float(row["sl_px"]),
                                    )
                                    else "—"
                                )
                        except Exception:
                            pass
                        self.sym2pid.setdefault(sym, set()).add(parent_id)
                        self._refresh_row_metrics(row, now_utc)
                        await self._upsert(row, force_push=True)
                        new_ids.add(parent_id)
                    except Exception:
                        self.log.exception(
                            "Symbol snapshot: failed to upsert parent %s",
                            parent_id,
                        )

            # Remove rows for this symbol that disappeared
            gone = set(self.sym2pid.get(sym, set())) - new_ids
            for pid in list(gone):
                await self._delete(pid, skip_closed=True)
        except Exception:
            self.log.exception("symbol snapshot failed for %s", sym)
        finally:
            self._sym_refresh_inflight.discard(sym)

    # ──────────────────────────────────────────────────────────────────────
    #  Closed-trade persistence helper used by delete & parent flattens
    # ──────────────────────────────────────────────────────────────────────

    async def _record_closed_trade_now(
        self, row: Dict[str, Any], exit_type: str = "MANUAL"
    ) -> None:
        """Persist a finished position into closed table and notify UI."""
        if not row:
            return

        symbol = str(row["symbol"]).upper()
        side = row.get("side", "long")
        qty_entry = abs(row.get("entry_qty") or row.get("qty") or 0)
        if not qty_entry:
            return

        entry_ts = row["filled_at"]
        entry_px = row["avg_fill"]

        exit_ts = _utcnow()
        exit_px = row.get("mkt_px") or entry_px
        pnl_cash = (
            (exit_px - entry_px) * qty_entry
            if side.lower() == "long"
            else (entry_px - exit_px) * qty_entry
        )
        ret_pct = (
            (pnl_cash / (entry_px * qty_entry) * 100) if qty_entry else None
        )
        duration = (exit_ts - entry_ts).total_seconds() if entry_ts else None

        cols = (
            "symbol",
            "side",
            "qty",
            "entry_time",
            "entry_price",
            "exit_time",
            "exit_price",
            "exit_type",
            "pnl_cash",
            "return_pct",
            "duration_sec",
        )
        vals = (
            symbol,
            side,
            qty_entry,
            entry_ts,
            entry_px,
            exit_ts,
            exit_px,
            exit_type,
            pnl_cash,
            ret_pct,
            duration,
        )

        placeholders = ", ".join(f"${i}" for i in range(1, len(cols) + 1))
        await self._db_execute(
            f"""
            INSERT INTO {self.tbl_closed} ({','.join(cols)})
            VALUES ({placeholders})
            ON CONFLICT DO NOTHING;
            """,
            *vals,
        )
        await self._db_execute(
            "SELECT pg_notify($1, 'refresh');",
            self.closed_channel,
        )
        try:
            ny = ZoneInfo("America/New_York")
            sess_date = exit_ts.astimezone(ny).date()
            self._pending_realised.append((sess_date, float(pnl_cash or 0.0)))
            # prune list to recent sessions
            cutoff = sess_date - timedelta(days=2)
            self._pending_realised = [
                (d, v) for d, v in self._pending_realised if d >= cutoff
            ]
        except Exception:
            pass

    # ──────────────────────────────────────────────────────────────────────
    #  Price listener (ZMQ ticks)
    # ──────────────────────────────────────────────────────────────────────

    async def _price_listener(self) -> None:
        """Listen to ZMQ tick stream and update open parents.

        * Big enough move → recompute KPIs and push immediately.
        * Small move → mark _dirty_px for batch flushing by _db_worker().
        """
        ctx = zmq.asyncio.Context.instance()
        sock = ctx.socket(zmq.SUB)
        endpoint = self.c.ZMQ_SIP_STREAM_ENDPOINT
        sock.connect(endpoint)
        sock.setsockopt(zmq.SUBSCRIBE, b"")
        self.log.info("ZMQ price stream connected %s", endpoint)

        thr = self.UI_PUSH_PCT_THRESHOLD

        while True:
            try:
                raw = await sock.recv()
                data = msgpack.unpackb(raw, raw=False)
                kind = next(iter(data))
                if kind != "trade":
                    continue
                pkt = data[kind]
                sym = pkt.get("symbol") or pkt.get("S")
                px = pkt.get("price") or pkt.get("p")
                # Optional SIP conditions filtering (same spirit as
                # notebook resampling). Skip ticks whose conditions have
                # any code in the configured exclusion set.
                conds_raw = pkt.get("conditions") or pkt.get("c")
                conds: list[str]
                if isinstance(conds_raw, (list, tuple, set)):
                    conds = [
                        str(x).strip() for x in conds_raw if x is not None
                    ]
                elif isinstance(conds_raw, str):
                    conds = [
                        s.strip()
                        for s in conds_raw.split("-")
                        if s is not None
                    ]
                else:
                    conds = []

                if self._cond_exclude and (
                    (not conds and "" in self._cond_exclude)
                    or any(c in self._cond_exclude for c in conds)
                ):
                    continue
                if sym is None or px is None:
                    continue

                sym = str(sym).upper()
                parent_ids = self.sym2pid.get(sym, set())
                if not parent_ids:
                    continue

                ts_utc = _utcnow()
                self._last_price_tick = ts_utc
                px_f = float(px)

                for pid in list(parent_ids):
                    row = self.state.get(pid)
                    if not row:
                        continue

                    last_push = row.get("_last_pushed_px")
                    pct_move = (
                        (abs(px_f - last_push) / last_push * 100)
                        if last_push
                        else 100.0
                    )

                    row["mkt_px"] = px_f
                    row["updated_at"] = ts_utc

                    if pct_move >= thr:
                        self._refresh_row_metrics(row, ts_utc)
                        await self._upsert(row)
                    else:
                        row["_dirty_px"] = True
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.warning("price listener error: %s", exc)
                await asyncio.sleep(0.1)

    # ──────────────────────────────────────────────────────────────────────
    #  DB worker (flush small moves)
    # ──────────────────────────────────────────────────────────────────────

    async def _db_worker(self) -> None:
        """Flush rows whose in-memory dict has a `_dirty_px` flag.

        First flush happens immediately; then the task sleeps exactly
        FLUSH_INTERVAL_S seconds between scans.
        """
        # Immediate first pass
        await asyncio.sleep(0)

        while True:
            try:
                now_ts = _utcnow()
                dirty_ids = [
                    pid for pid, r in self.state.items() if r.get("_dirty_px")
                ]
                for pid in dirty_ids:
                    row = self.state.get(pid)
                    if not row:
                        continue
                    row.pop("_dirty_px", None)
                    self._refresh_row_metrics(row, now_ts)
                    await self._upsert(row)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.warning("db_worker error: %s", exc)
            finally:
                await asyncio.sleep(self.FLUSH_INTERVAL_S)

    # ──────────────────────────────────────────────────────────────────────
    #  Snapshot loop
    # ──────────────────────────────────────────────────────────────────────

    async def _snapshot_loop(self) -> None:
        """Periodically rebuild the in-memory/DB picture from scratch."""
        while True:
            await self._sleep_until_boundary(self.SNAPSHOT_SEC)
            async with self._snap_lock:
                try:
                    await self._initial_snapshot()
                except Exception:
                    self.log.exception("snapshot refresh failed")

    # ──────────────────────────────────────────────────────────────────────
    #  Closed-trades worker
    # ──────────────────────────────────────────────────────────────────────

    async def _closed_trades_worker(self) -> None:
        """Periodic incremental sync of closed trades."""
        while True:
            try:
                await self._sync_closed_trades()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.warning("closed_trades_worker error: %s", exc)
            finally:
                await self._sleep_until_boundary(self.CLOSED_SYNC_SEC)

    # ──────────────────────────────────────────────────────────────────────
    #  Equity worker (optional; if tables present)
    # ──────────────────────────────────────────────────────────────────────

    async def _equity_worker(self) -> None:
        """Keep the equity series up to date if the environment is set up."""
        while True:
            try:
                await update_today_row_async(self.cfg, repo=self.repo)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.debug("equity_worker note: %s", exc)
            finally:
                await self._sleep_until_boundary(self.EQUITY_INTRADAY_SEC)

    async def _cash_flows_worker(self) -> None:
        """Ingest account activities and persist cash flows.

        Behavior:
        - Bootstrap: page newest→oldest without time filters and upsert
          into ``cash_flows`` (idempotent). This fully backfills after a
          drop.
        - Incremental: page from newest and stop when a whole page is
          already present. This avoids relying on timestamps, which for
          non‑trade activities can be date‑only.

        We store ALL activities and write their cash impact into the
        single ``amount`` column (positive = cash in, negative = out).
        Downstream equity logic filters to external flows by ``type``
        using ``CASH_FLOW_TYPES``.
        """
        while True:
            try:
                await self._ensure_intraday_table()
                got = await self._bootstrap_cash_flows(max_pages=3)
                if got:
                    self.log.info("cash_flows ingested %d activities", got)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.debug("cash_flows_worker note: %s", exc)
            finally:
                await asyncio.sleep(300)

    async def _equity_intraday_worker(self) -> None:
        """Append an intraday equity point on the configured cadence."""

        while True:
            try:
                await self._equity_intraday_backfill()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.debug("equity_intraday_worker note: %s", exc)
            finally:
                await self._sleep_until_boundary(
                    self.EQUITY_INTRADAY_SEC, offset=5.0
                )

    # ──────────────────────────────────────────────────────────────────────
    #  Trade-update handling (Alpaca trade_updates)
    # ──────────────────────────────────────────────────────────────────────

    async def _on_trade_update(self, data: Any) -> None:
        """Handle every Alpaca trade_updates message."""
        event = str(getattr(data, "event", "")).lower()
        order = getattr(data, "order", None)

        # Extract a dict view of the order that works across SDK variants.
        odict: Dict[str, Any]
        if order is None:
            odict = {}
        else:
            odict = (
                getattr(order, "model_dump", None)()
                if hasattr(order, "model_dump")
                else getattr(order, "_raw", {})
            )

        oid = str(getattr(order, "id", "") or odict.get("id") or "")
        parent = str(odict.get("parent_order_id") or "") or None

        # Normalise symbol to uppercase for all internal maps
        symbol_raw = odict.get("symbol")
        symbol = str(symbol_raw).upper() if symbol_raw else None

        otype = odict.get("type") or odict.get("order_type")
        intent = str(odict.get("position_intent", "")).lower()
        now_ts = _utcnow()

        is_bracket_parent = (
            odict.get("order_class") == "bracket"
            and parent is None
            and intent.endswith("_to_open")
        )

        self._trace(
            "trade_update:begin",
            event=event,
            oid=oid,
            parent=parent,
            symbol=symbol,
            otype=otype,
            intent=intent,
            is_bracket_parent=is_bracket_parent,
            stream_fields={
                "position_qty": _json_safe(
                    getattr(data, "position_qty", None)
                ),
                "price": _json_safe(getattr(data, "price", None)),
                "qty": _json_safe(getattr(data, "qty", None)),
                "status": getattr(order, "status", None),
            },
        )

        # Always trigger per-symbol snapshot refresh (deterministic truth).
        if symbol:
            try:
                asyncio.create_task(self._refresh_symbol_snapshot(symbol))
            except Exception:
                pass

        # SIMPLE ORDERS (crypto, manual, test orders)
        if (
            symbol
            and not is_bracket_parent
            and parent is None
            and odict.get("order_class") != "bracket"
        ):
            # first fill → create row or flatten
            if oid not in self.state and event in {"fill", "partial_fill"}:
                # If position is flat after this fill, consider it a close.
                try:
                    pos_qty = float(getattr(data, "position_qty", 0) or 0)
                except Exception:
                    pos_qty = 0.0
                if _is_flat(pos_qty):
                    last_px = float(
                        getattr(data, "price", 0)
                        or odict.get("filled_avg_price")
                        or 0
                    )
                    for pid in list(self.sym2pid.get(symbol, set())):
                        await self._delete(
                            pid,
                            skip_closed=False,
                            exit_px=last_px,
                            exit_type="MANUAL",
                        )
                    if symbol in self.sym2pid:
                        self.sym2pid[symbol].clear()
                    else:
                        # No in-memory row; force immediate closed sync
                        try:
                            await asyncio.to_thread(self._sync_closed_trades)
                        except Exception:
                            pass
                    return

                # Normalise qty & prices
                fill_qty = int(
                    float(
                        odict.get("filled_qty") or getattr(data, "qty", 0) or 0
                    )
                )
                fill_price = float(
                    odict.get("filled_avg_price")
                    or getattr(data, "price", 0)
                    or 0
                )

                # Broker position WAC basis, fallback to fill price
                try:
                    pos = self.rest.get_position(symbol)
                    avg_px_symbol = float(
                        getattr(pos, "avg_entry_price", fill_price)
                    )
                except Exception:
                    avg_px_symbol = fill_price

                # Parse filled_at if available
                filled_at_ts = pd.to_datetime(
                    odict.get("filled_at") or odict.get("submitted_at"),
                    utc=True,
                    errors="coerce",
                )
                filled_at = (
                    filled_at_ts.to_pydatetime()
                    if pd.notna(filled_at_ts)
                    else None
                )

                row = dict(
                    parent_id=oid,
                    symbol=symbol,
                    side=(
                        "long"
                        if str(getattr(order, "side", "buy")).lower() == "buy"
                        else "short"
                    ),
                    filled_at=filled_at,
                    qty=fill_qty,
                    avg_fill=fill_price,
                    avg_px_symbol=avg_px_symbol,
                    entry_qty=fill_qty,  # in-memory only
                    sl_child=None,
                    sl_px=None,
                    tp_child=None,
                    tp_px=None,
                    mkt_px=fill_price,
                    moved_flag="—",
                    updated_at=now_ts,
                )
                self._trace(
                    "trade_update:simple_first_fill:create_row", row=row
                )
                self._refresh_row_metrics(row, now_ts)
                await self._upsert(row, force_push=True)
                self.sym2pid.setdefault(symbol, set()).add(oid)
                return

            # subsequent fills
            if oid in self.state and event in {"fill", "partial_fill"}:
                row = self.state[oid]
                old = row.copy()
                row["qty"] = int(
                    float(
                        odict.get("filled_qty")
                        or getattr(data, "qty", 0)
                        or row["qty"]
                    )
                )
                row["avg_fill"] = float(
                    odict.get("filled_avg_price")
                    or getattr(data, "price", 0)
                    or row["avg_fill"]
                )
                row["updated_at"] = now_ts
                # Refresh broker WAC basis so P/L aligns immediately
                try:
                    await self._sync_avg_px_symbol(symbol)
                except Exception:
                    pass
                self._refresh_row_metrics(row, now_ts)
                self._trace(
                    "trade_update:simple_update", before=old, after=row
                )
                self._log_if_changed(old, row)
                await self._upsert(row, force_push=True)

                if _is_flat(row["qty"]):
                    await self._delete(oid)
                return

        # CHILD LEG BEFORE PARENT → cache and exit
        if (parent is None and intent.endswith("_to_close")) or (
            parent and parent not in self.state
        ):
            key = parent or oid
            if otype in {"stop", "stop_limit"}:
                self._pending_legs.setdefault(key, {}).update(
                    {
                        "sl_child": oid,
                        "sl_px": float(
                            odict.get("stop_price")
                            or odict.get("limit_price")
                            or 0
                        ),
                    }
                )
            elif otype == "limit":
                self._pending_legs.setdefault(key, {}).update(
                    {
                        "tp_child": oid,
                        "tp_px": float(odict.get("limit_price") or 0),
                    }
                )

            self._trace(
                "trade_update:cache_leg_before_parent",
                key=key,
                cached=self._pending_legs.get(key, {}),
            )
            return

        # FIRST FILL OF A BRACKET PARENT
        if (
            is_bracket_parent
            and oid not in self.state
            and event in {"partial_fill", "fill"}
        ):
            fill_qty = int(
                float(odict.get("filled_qty") or getattr(data, "qty", 0) or 0)
            )
            fill_price = float(
                odict.get("filled_avg_price") or getattr(data, "price", 0) or 0
            )
            # Broker WAC basis, fallback to fill
            try:
                pos = self.rest.get_position(symbol)
                avg_px_symbol = float(
                    getattr(pos, "avg_entry_price", fill_price)
                )
            except Exception:
                avg_px_symbol = fill_price

            filled_at_ts = pd.to_datetime(
                odict.get("filled_at") or odict.get("submitted_at"),
                utc=True,
                errors="coerce",
            )
            filled_at = (
                filled_at_ts.to_pydatetime()
                if pd.notna(filled_at_ts)
                else None
            )

            row = dict(
                parent_id=oid,
                symbol=symbol,
                side=(
                    "long"
                    if str(getattr(order, "side", "buy")).lower() == "buy"
                    else "short"
                ),
                filled_at=filled_at,
                qty=fill_qty,
                avg_fill=fill_price,
                avg_px_symbol=avg_px_symbol,
                entry_qty=fill_qty,  # in-memory only
                sl_child=None,
                sl_px=None,
                tp_child=None,
                tp_px=None,
                mkt_px=fill_price,
                moved_flag="—",
                updated_at=now_ts,
            )

            # Merge cached SL/TP (by parent id only).
            if oid in self._pending_legs:
                row.update(self._pending_legs.pop(oid))

            if row.get("sl_px") is not None:
                row["moved_flag"] = (
                    "OK"
                    if self._is_at_break_even(
                        row.get("side", "long"),
                        row["avg_fill"],
                        row["sl_px"],
                    )
                    else "—"
                )

            self._trace("trade_update:bracket_first_fill:create_row", row=row)
            self._refresh_row_metrics(row, now_ts)
            await self._upsert(row, force_push=True)
            self.sym2pid.setdefault(symbol, set()).add(oid)
            return

        # CHILD-LEG UPDATE (parent known)
        if parent and parent in self.state:
            row = self.state[parent]
            old = row.copy()
            # Merge from any child-oid keyed cache
            if oid in self._pending_legs:
                row.update(self._pending_legs.pop(oid))
            self._trace(
                "trade_update:child_leg_update:before",
                event=event,
                otype=otype,
                before=old,
            )

            if event in {"new", "accepted", "pending_new", "replaced", "held"}:
                if otype in {"stop", "stop_limit"}:
                    row["sl_child"] = oid
                    row["sl_px"] = float(
                        odict.get("stop_price")
                        or odict.get("limit_price")
                        or row.get("sl_px")
                    )
                elif otype == "limit":
                    row["tp_child"] = oid
                    if odict.get("limit_price") is not None:
                        row["tp_px"] = float(odict["limit_price"])

                if row.get("sl_px") is not None:
                    row["moved_flag"] = (
                        "OK"
                        if self._is_at_break_even(
                            row.get("side", "long"),
                            row["avg_fill"],
                            row["sl_px"],
                        )
                        else "—"
                    )

            if event in {"fill", "canceled", "expired"}:
                if event == "fill":
                    if oid == row.get("sl_child"):
                        row["_exit_type"] = "SL"
                    elif oid == row.get("tp_child"):
                        row["_exit_type"] = "TP"
                if oid == row.get("sl_child"):
                    row.update(
                        {"sl_child": None, "sl_px": None, "moved_flag": "—"}
                    )
                elif oid == row.get("tp_child"):
                    row.update({"tp_child": None, "tp_px": None})

            row["updated_at"] = now_ts
            self._trace("trade_update:child_leg_update:after", after=row)

            if not row.get("sl_child") and not row.get("tp_child"):
                self._trace(
                    "trade_update:both_legs_gone_closing",
                    parent=parent,
                    snapshot=row,
                )
                await self._record_closed_trade_now(
                    row,
                    exit_type=row.pop("_exit_type", "MANUAL"),
                )
                await self._delete(parent, skip_closed=True)
            else:
                self._refresh_row_metrics(row, now_ts)
                self._log_if_changed(old, row)
                await self._upsert(row, force_push=True)
            return

        # BRACKET-PARENT self-events
        if oid in self.state:
            row = self.state[oid]
            old = row.copy()

            if event in {"partial_fill", "fill"}:
                row["qty"] = int(
                    float(
                        odict.get("filled_qty")
                        or getattr(data, "qty", 0)
                        or row["qty"]
                    )
                )
                row["avg_fill"] = float(
                    odict.get("filled_avg_price")
                    or getattr(data, "price", 0)
                    or row["avg_fill"]
                )
                row["moved_flag"] = (
                    "OK"
                    if self._is_at_break_even(
                        row.get("side", "long"),
                        row["avg_fill"],
                        row.get("sl_px"),
                    )
                    else "—"
                )
                row["updated_at"] = now_ts
                # Refresh broker WAC basis so P/L aligns immediately
                try:
                    if symbol:
                        await self._sync_avg_px_symbol(symbol)
                except Exception:
                    pass
                self._refresh_row_metrics(row, now_ts)
                self._log_if_changed(old, row)
                await self._upsert(row, force_push=True)

                if _is_flat(row["qty"]):
                    await self._record_closed_trade_now(row)
                    await self._delete(oid, skip_closed=True)
                return

            if event in {"canceled", "expired", "done_for_day"}:
                await self._delete(oid)
                return

    # ──────────────────────────────────────────────────────────────────────
    #  Orchestration
    # ──────────────────────────────────────────────────────────────────────

    async def run(
        self,
        stop_event: Optional[asyncio.Event] = None,
        install_signal_handlers: bool = True,
    ) -> None:
        """Start background workers and the trade-updates stream.

        If ``stop_event`` is provided, the manager waits on it for
        shutdown. When running many managers in one process, pass a
        shared event and set ``install_signal_handlers=False`` so only
        the orchestrator installs signal handlers.
        """
        await self.init()
        await self._refresh_market_schedule()

        # Graceful shutdown: either use provided event or install handlers.
        loop = asyncio.get_running_loop()
        _stop_evt = stop_event or asyncio.Event()

        def _stop(*_args: Any) -> None:
            if not _stop_evt.is_set():
                self.log.info("Shutdown signal received; cancelling tasks …")
                _stop_evt.set()

        if install_signal_handlers:
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(sig, _stop)
                except NotImplementedError:
                    # On some systems signal handlers may be unavailable.
                    pass

        background = [
            self._spawn_guarded(
                "market_schedule",
                lambda: self._market_schedule_worker(),
                stop_event=_stop_evt,
                backoff=self.MARKET_REFRESH_SEC,
            ),
            self._spawn_guarded(
                "schedule_supervisor",
                lambda: self._schedule_supervisor(_stop_evt),
                stop_event=_stop_evt,
                backoff=self.MARKET_POLL_SEC,
            ),
        ]

        try:
            await _stop_evt.wait()
        finally:
            _stop_evt.set()
            for task in background:
                task.cancel()
            with contextlib.suppress(Exception):
                await asyncio.gather(*background, return_exceptions=True)
            with contextlib.suppress(Exception):
                await self._deactivate_session()

    async def _run_trade_stream(self) -> None:
        """Run trade_updates websocket; fall back to legacy Stream."""
        try:
            # Prefer modern SDK
            from alpaca.trading.stream import TradingStream  # type: ignore

            # Build a proper websocket URL for trade_updates from the REST base
            ws_url = self.c.ALPACA_BASE_URL.rstrip("/")
            if ws_url.startswith("http"):
                ws_url = ws_url.replace("http", "ws", 1)
            if not ws_url.endswith("/stream"):
                ws_url = f"{ws_url}/stream"

            stream = TradingStream(
                self.c.API_KEY,
                self.c.SECRET_KEY,
                paper=("paper" in self.c.ALPACA_BASE_URL),
                url_override=ws_url,
            )
            stream.subscribe_trade_updates(self._on_trade_update)
            await stream.run()
        except asyncio.CancelledError:
            raise
        except Exception as primary_exc:
            self.log.debug(
                "TradingStream unavailable/failed (%s), falling back …",
                primary_exc,
            )
            try:
                # Legacy SDK fallback
                from alpaca_trade_api.stream import Stream  # type: ignore
                from alpaca_trade_api.common import URL  # type: ignore

                stream = Stream(
                    self.c.API_KEY,
                    self.c.SECRET_KEY,
                    base_url=URL(self.c.ALPACA_BASE_URL),
                )
                stream.subscribe_trade_updates(self._on_trade_update)
                # Private runner is an async coroutine in recent versions.
                await stream._run_forever()  # type: ignore[attr-defined]
            except asyncio.CancelledError:
                raise
            except Exception as secondary_exc:
                self.log.exception(
                    "Both TradingStream and fallback Stream failed: %s",
                    secondary_exc,
                )

    # Public convenience
    def close(self) -> None:
        try:
            if self.repo:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(self.repo.close())
                else:
                    loop.run_until_complete(self.repo.close())
            if self.ts_repo:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(self.ts_repo.close())
                else:
                    loop.run_until_complete(self.ts_repo.close())
        except Exception:
            pass

    # ──────────────────────────────────────────────────────────────────────
    #  UI heartbeat worker (lightweight)
    # ──────────────────────────────────────────────────────────────────────

    async def _ui_heartbeat_worker(self) -> None:
        """Periodically notify UI to refresh cached snapshots.

        We emit a minimal heartbeat message on the positions channel.
        The web layer uses ETag-based cached endpoints to fetch only if
        data changed, so this remains cheap.
        """
        while True:
            try:
                payload = json.dumps(
                    {
                        "op": "heartbeat",
                        "version": _utcnow().isoformat(),
                    }
                )
                await self._db_execute(
                    "SELECT pg_notify($1, $2);",
                    self.pos_channel,
                    payload,
                )
            except Exception as exc:
                self.log.debug("ui_heartbeat note: %s", exc)
            finally:
                await self._sleep_until_boundary(self.UI_SNAPSHOT_SEC)
