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
import contextlib
import signal
import os
import sys
import time
from datetime import date, datetime, time as dtime, timedelta
from typing import Any, Awaitable, Callable, Dict, Optional

from alpaca_trade_api.rest import REST

import pandas as pd

from coldharbour_manager.core.destinations import sanitize_identifier
from coldharbour_manager.services.account_manager import db as db_helpers
from coldharbour_manager.services.account_manager import equity as equity_helpers
from coldharbour_manager.services.account_manager import market
from coldharbour_manager.services.account_manager import snapshot
from coldharbour_manager.services.account_manager import state as state_helpers
from coldharbour_manager.services.account_manager import trades
from coldharbour_manager.services.account_manager import workers as runtime_workers
from coldharbour_manager.services.account_manager.core_logic.market_schedule import (
    SessionWindow,
)
from coldharbour_manager.infrastructure.db import AsyncAccountRepository
from coldharbour_manager.services.account_manager.config import _Config
from coldharbour_manager.services.account_manager.utils import (
    _json_safe,
    _parse_bool,
    _utcnow,
)

class AccountLoggerAdapter(logging.LoggerAdapter):
    """Adapter that prefixes AccountManager logs with account/module info."""

    def __init__(
        self,
        logger: logging.Logger,
        account_label: str,
        default_module: str = "account_mgr",
    ):
        super().__init__(logger, {"account_label": account_label})
        self._account_label = account_label
        self._module = default_module

    def process(self, msg, kwargs):
        prefix = f"[{self._account_label}] {self._module}"
        return f"{prefix} {msg}", kwargs

    def with_module(self, module: str) -> "AccountLoggerAdapter":
        """Return a short-lived adapter that reports `<module>` in the prefix."""
        return AccountLoggerAdapter(self.logger, self._account_label, default_module=module)

#  AccountManager
# ──────────────────────────────────────────────────────────────────────


POS_CHANNEL_PREFIX = "pos_channel_"
CLOSED_CHANNEL_PREFIX = "closed_channel_"


class AccountManager:
    """Realtime account-state synchronizer with managed background workers.

    Attributes:
        cfg: Original configuration dictionary supplied by the caller.
        c: Normalized, typed configuration (_Config) that backs runtime knobs.
        log: Logger that tags account labels and module names.
        rest: Alpaca REST client used for fills/orders market calls.
        repo: AsyncAccountRepository for the live Postgres schema.
        ts_repo: Optional repository pointing at Timescale.
        tbl_live: Qualified table name for open trades.
        tbl_closed: Qualified table name for closed trades.
        tbl_equity, tbl_equity_intraday, tbl_cash_flows, tbl_metrics:
            Tables used for equity/flow updating plus KPIs.
        pos_channel, closed_channel: NOTIFY channels broadcast to SSE clients.
        state, sym2pid: In-memory caches for open positions.
        HEARTBEAT_SEC, SNAPSHOT_SEC, CLOSED_SYNC_SEC, EQUITY_INTRADAY_SEC:
            Execution intervals exposed to background tasks.
        _active_tasks: List of asyncio Tasks spawned by ``run()``.

    The manager spawns several persistent workers:
        * Market schedule polling (`_market_schedule_worker` / `_schedule_supervisor`).
        * Snapshot loop (`_snapshot_loop`).
        * Closed trade refresh (`_closed_sync_worker`).
        * Equity background reporter (`_equity_worker` and intraday updater).
        * Price streamer + order ingestion (`_price_worker` / `_orders_worker`).
        * UI push/flush thread (`_ui_flush_worker`).
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
            TABLE_ACCOUNT_METRICS=cfg.get(
                "TABLE_ACCOUNT_METRICS", _Config.TABLE_ACCOUNT_METRICS
            ),
            ACCOUNT_SLUG=cfg.get("ACCOUNT_SLUG", _Config.ACCOUNT_SLUG),
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
            ENABLE_TRADE_STREAM=_parse_bool(
                cfg.get("ENABLE_TRADE_STREAM", False)
            ),
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
        self._orders_cache = pd.DataFrame()
        self._last_orders_sync_time: Optional[datetime] = None
        self._orders_cache_reset_at: Optional[datetime] = None

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

        def _derive_metrics(
            base_metrics: str, live_table: str, sch: Optional[str]
        ) -> str:
            """Derive per-account metrics table using live suffix."""
            if "." in base_metrics:
                return base_metrics
            # If caller supplied a custom name, respect it verbatim.
            if base_metrics != _Config.TABLE_ACCOUNT_METRICS:
                return _qualify(sch, base_metrics)
            try:
                live_base = (live_table or "").split(".")[-1]
                prefixes = (
                    "open_trades_",
                    "account_open_positions_",
                    "open_trades",
                    "account_open_positions",
                )
                suffix = live_base
                for prefix in prefixes:
                    if suffix.startswith(prefix):
                        suffix = suffix[len(prefix) :]
                        break
                suffix = sanitize_identifier(suffix)
                if suffix:
                    return _qualify(sch, f"{base_metrics}_{suffix}")
            except Exception:
                pass
            return _qualify(sch, base_metrics)

        self.tbl_metrics = _derive_metrics(
            self.c.TABLE_ACCOUNT_METRICS, self.tbl_live, schema
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
            if ch.startswith(POS_CHANNEL_PREFIX) and len(ch) > len(
                POS_CHANNEL_PREFIX
            ):
                label = ch.split(POS_CHANNEL_PREFIX, 1)[1]
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
        base_logger = logging.getLogger(log_name)
        self.log = base_logger
        if not base_logger.handlers:
            handler = logging.StreamHandler(stream=sys.__stdout__)
            fmt = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"
            datefmt = "%Y-%m-%d %H:%M:%S"
            formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)
            formatter.converter = time.localtime  # type: ignore[attr-defined]
            handler.setFormatter(formatter)
            base_logger.addHandler(handler)

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
            base_logger.setLevel(_parse_level(lvl_name))
            handler.setLevel(logging.NOTSET)
            base_logger.propagate = False

        self.log = AccountLoggerAdapter(
            base_logger, self.account_label, default_module="runtime"
        )

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
        return await db_helpers._db_fetch(self.repo, sql, *args)

    async def _db_fetchrow(self, sql: str, *args: Any) -> Optional[dict]:
        return await db_helpers._db_fetchrow(self.repo, sql, *args)

    async def _db_execute(self, sql: str, *args: Any) -> str:
        return await db_helpers._db_execute(
            self.repo, self.log, sql, *args
        )

    async def _ts_fetch(self, sql: str, *args: Any) -> list[dict]:
        """Fetch rows from Timescale if configured; fall back to Postgres."""
        return await db_helpers._ts_fetch(
            self.ts_repo, self.repo, sql, *args
        )

    async def _ts_fetchrow(self, sql: str, *args: Any) -> Optional[dict]:
        """Fetch a single row from Timescale if configured; else fall back."""
        return await db_helpers._ts_fetchrow(
            self.ts_repo, self.repo, sql, *args
        )

    async def _type_exists(self, name: str) -> bool:
        """Return True if a composite/enum type exists for ``name``."""
        return await db_helpers._type_exists(self.repo, name)

    async def _drop_type(self, name: str) -> None:
        """Drop a composite/enum type if it exists (no CASCADE)."""
        await db_helpers._drop_type(self.repo, name, self.log)

    async def _db_executemany(
        self,
        sql: str,
        params: list[tuple[Any, ...]],
    ) -> None:
        await db_helpers._db_executemany(self.repo, sql, params)

    async def _relation_exists(self, name: str) -> bool:
        """Return True if a table/view/partition exists for ``name``."""
        return await db_helpers._relation_exists(self.repo, name)

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
    ) -> tuple[datetime, date]:
        """Return the 04:00 New York start for the active trading session."""
        return market._session_start_utc(self, now_ts)

    async def _ensure_intraday_table(self) -> None:
        """Create the intraday equity table if missing."""
        await db_helpers._ensure_intraday_table(
            self.repo, self.tbl_equity_intraday, self.log
        )

    # ──────────────────────────────────────────────────────────────────────
    #  Market schedule helpers
    # ──────────────────────────────────────────────────────────────────────

    async def _refresh_market_schedule(self) -> int:
        """Synchronise the market schedule table with the Alpaca calendar."""
        return await market._refresh_market_schedule(self)

    async def _schedule_current(
        self,
        ts: Optional[datetime] = None,
    ) -> Optional[SessionWindow]:
        return await market._schedule_current(self, ts)

    async def _schedule_next(
        self,
        ts: Optional[datetime] = None,
    ) -> Optional[SessionWindow]:
        return await market._schedule_next(self, ts)

    async def _needs_recovery(self) -> bool:
        """Return True when core tables are empty and require bootstrap."""
        return await market._needs_recovery(self)

    async def _market_schedule_worker(self) -> None:
        """Keep the market_schedule table in sync with the Alpaca calendar."""
        return await market._market_schedule_worker(self)

    async def _bootstrap_for_session(self) -> None:
        """Run the bootstrap sequence before a session starts."""
        return await market._bootstrap_for_session(self)

    async def _deactivate_session(self) -> None:
        """Cancel active worker tasks and reset session state."""
        return await market._deactivate_session(self)

    async def _activate_session(self, session: SessionWindow) -> None:
        """Ensure workers are running for the provided session."""
        return await market._activate_session(self, session)

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
        return await market._schedule_supervisor(self, stop_event)

    # ──────────────────────────────────────────────────────────────────────
    #  Cold‑start bootstrap helpers
    # ──────────────────────────────────────────────────────────────────────

    async def _bootstrap_cash_flows(
        self,
        max_pages: int | None = None,
    ) -> int:
        """Dispatch to the equity helper for cash flows."""
        return await equity_helpers._bootstrap_cash_flows(
            self, max_pages=max_pages
        )

    async def _rebuild_equity_full(self) -> None:
        """Delegate full equity rebuild to the helper."""
        await equity_helpers._rebuild_equity_full(self)

    # ──────────────────────────────────────────────────────────────────────

    async def _equity_intraday_backfill(self) -> None:
        """Delegate intraday backfill to the helper module."""
        await equity_helpers._equity_intraday_backfill(self)

    # ──────────────────────────────────────────────────────────────────────
    #  Schema ensure (DDL + helpful indexes)
    # ──────────────────────────────────────────────────────────────────────

    async def _ensure_tables(self) -> None:
        """Create live & closed tables if missing (no-ops otherwise)."""
        await db_helpers._ensure_tables(
            self.repo,
            self.tbl_live,
            self.tbl_closed,
            self.tbl_cash_flows,
            self.tbl_equity_intraday,
            self.tbl_market_schedule,
            self.tbl_metrics,
            self._db_lock,
            self.log,
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

    #  Postgres upsert/delete + NOTIFY
    # ──────────────────────────────────────────────────────────────────────

    async def _upsert(
        self, row: Dict[str, Any], *, force_push: bool = False
    ) -> None:
        """Upsert row to live table and optionally notify the UI."""
        await state_helpers.upsert_live_row(
            self, row, force_push=force_push
        )

    async def _delete(
        self,
        parent_id: str,
        *,
        skip_closed: bool = False,
        exit_px: Optional[float] = None,
        exit_type: str = "MANUAL",
    ) -> None:
        """Delete parent row, record closed trade, and notify UI."""
        await state_helpers.delete_parent_row(
            self,
            parent_id,
            skip_closed=skip_closed,
            exit_px=exit_px,
            exit_type=exit_type,
        )

    # ──────────────────────────────────────────────────────────────────────
    #  Order helpers
    # ──────────────────────────────────────────────────────────────────────

    def _maybe_reset_orders_cache(self) -> None:
        """Reset the orders cache before a full snapshot pass."""
        now_ts = _utcnow()
        last_reset = self._orders_cache_reset_at
        if last_reset and now_ts - last_reset < timedelta(hours=24):
            return
        last_sync = self._last_orders_sync_time
        if (
            last_sync
            and now_ts - last_sync < timedelta(minutes=60)
        ):
            self._orders_cache_reset_at = now_ts
            self.log.info("Skipping cache reset; synced recently")
            return
        self._orders_cache = pd.DataFrame()
        self._last_orders_sync_time = None
        self._orders_cache_reset_at = now_ts
        self.log.info("Orders cache reset ahead of snapshot refresh")

    # ──────────────────────────────────────────────────────────────────────
    #  Closed-trades sync (watermark window, idempotent)
    # ──────────────────────────────────────────────────────────────────────

    async def _sync_closed_trades(self) -> None:
        """Delegate closed-trades sync to trades helpers."""
        await trades._sync_closed_trades(self)

    # ──────────────────────────────────────────────────────────────────────
    #  Snapshot building (full & per-symbol)
    # ──────────────────────────────────────────────────────────────────────

    async def _initial_snapshot(self) -> None:
        """Delegate building the bootstrap snapshot to snapshot helpers."""
        await snapshot.initial_snapshot(self)

    async def _refresh_symbol_snapshot(self, symbol: str) -> None:
        """Delegate per-symbol refresh to snapshot helpers."""
        await snapshot.refresh_symbol_snapshot(self, symbol)

    # ──────────────────────────────────────────────────────────────────────
    #  Closed-trade persistence helper used by delete & parent flattens
    # ──────────────────────────────────────────────────────────────────────

    async def _record_closed_trade_now(
        self, row: Dict[str, Any], exit_type: str = "MANUAL"
    ) -> None:
        """Delegate closed trade persistence to trades helpers."""
        await trades._record_closed_trade_now(self, row, exit_type=exit_type)

    # ──────────────────────────────────────────────────────────────────────
    #  Price listener (ZMQ ticks)
    # ──────────────────────────────────────────────────────────────────────

    async def _price_listener(self) -> None:
        """Delegate to workers.price_listener()."""
        await runtime_workers.price_listener(self)

    # ──────────────────────────────────────────────────────────────────────
    #  DB worker (flush small moves)
    # ──────────────────────────────────────────────────────────────────────

    async def _db_worker(self) -> None:
        """Delegate to workers.db_worker()."""
        await runtime_workers.db_worker(self)

    # ──────────────────────────────────────────────────────────────────────
    #  Snapshot loop
    # ──────────────────────────────────────────────────────────────────────

    async def _snapshot_loop(self) -> None:
        """Delegate to workers.snapshot_loop()."""
        await runtime_workers.snapshot_loop(self)

    # ──────────────────────────────────────────────────────────────────────
    #  Closed-trades worker
    # ──────────────────────────────────────────────────────────────────────

    async def _closed_trades_worker(self) -> None:
        """Delegate to workers.closed_trades_worker()."""
        await runtime_workers.closed_trades_worker(self)

    # ──────────────────────────────────────────────────────────────────────
    #  Metrics worker
    # ──────────────────────────────────────────────────────────────────────

    async def _metrics_worker(self) -> None:
        """Delegate to workers.metrics_worker()."""
        await runtime_workers.metrics_worker(self)

    # ──────────────────────────────────────────────────────────────────────
    #  Equity worker (optional; if tables present)
    # ──────────────────────────────────────────────────────────────────────

    async def _equity_worker(self) -> None:
        """Run the equity helper worker loop."""
        await equity_helpers._equity_worker(self)

    async def _cash_flows_worker(self) -> None:
        """Run the cash flows helper worker loop."""
        await equity_helpers._cash_flows_worker(self)

    async def _equity_intraday_worker(self) -> None:
        """Run the intraday helper worker loop."""
        await equity_helpers._equity_intraday_worker(self)

    # ──────────────────────────────────────────────────────────────────────
    #  Trade-update handling (Alpaca trade_updates)
    # ──────────────────────────────────────────────────────────────────────

    async def _on_trade_update(self, data: Any) -> None:
        """Delegate trade updates to trades helpers."""
        await trades._on_trade_update(self, data)

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
            self._spawn_guarded(
                "metrics_worker",
                lambda: self._metrics_worker(),
                stop_event=_stop_evt,
                backoff=5,
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
        """Delegate trade stream startup to trades helpers."""
        await trades._run_trade_stream(self)

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
        """Delegate to workers.ui_heartbeat_worker()."""
        await runtime_workers.ui_heartbeat_worker(self)
