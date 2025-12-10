"""Market schedule and session management helpers."""

from __future__ import annotations

import asyncio
import contextlib
import os
from datetime import date, datetime, timedelta, time as dtime
from typing import Optional, TYPE_CHECKING

import pandas as pd
from zoneinfo import ZoneInfo

from cold_harbour.core.date_utils import trading_session_date
from cold_harbour.services.account_manager.core_logic.account_start import (
    earliest_activity_date_async,
)
from cold_harbour.services.account_manager.core_logic.market_schedule import (
    SessionWindow,
    next_session_after_async,
    session_for_timestamp_async,
    sync_schedule_async,
)
from cold_harbour.services.account_manager.utils import _utcnow

if TYPE_CHECKING:
    from .runtime import AccountManager


def _session_start_utc(
    mgr: "AccountManager",
    now_ts: Optional[datetime] = None,
) -> tuple[pd.Timestamp, date]:
    """Return the 04:00 New York start for the active session."""
    tz_ny = ZoneInfo("America/New_York")
    now_ts = now_ts or _utcnow()
    session_date = trading_session_date(now_ts)
    start_local = datetime.combine(
        session_date, dtime(hour=4), tzinfo=tz_ny
    )
    start_utc = pd.to_datetime(start_local, utc=True)
    return start_utc, session_date


async def _refresh_market_schedule(mgr: "AccountManager") -> int:
    """Synchronise the market schedule table with Alpaca."""

    try:
        assert mgr.repo is not None
        cash_flow_types = os.getenv(
            "CASH_FLOW_TYPES",
            (
                "CSD,CSW,JNLC,ACATC,ACATS,FEE,CFEE,DIV,DIVCGL,DIVCGS,"
                "DIVNRA,DIVROC,DIVTXEX,DIVWH,INT,INTPNL"
            ),
        )
        earliest = await earliest_activity_date_async(
            mgr.repo,
            {
                "closed": mgr.tbl_closed,
                "cash_flows": mgr.tbl_cash_flows,
                "open": mgr.tbl_live,
            },
            cash_flow_types=cash_flow_types,
            fallback_years=5,
        )
        count = await sync_schedule_async(
            mgr.repo,
            mgr.tbl_market_schedule,
            mgr.rest,
            start_date=earliest,
            forward_days=max(mgr.MARKET_FORWARD_DAYS, 90),
            pre_open_time=mgr._market_pre_open_time,
        )
        if count:
            mgr.log.debug("market schedule refreshed: %d rows", count)
        return count
    except Exception as exc:
        mgr.log.warning("market schedule refresh failed: %s", exc)
        return 0


async def _schedule_current(
    mgr: "AccountManager",
    ts: Optional[datetime] = None,
) -> Optional[SessionWindow]:
    ts = ts or _utcnow()
    return await session_for_timestamp_async(
        mgr.repo,  # type: ignore[arg-type]
        mgr.tbl_market_schedule,
        ts,
    )


async def _schedule_next(
    mgr: "AccountManager",
    ts: Optional[datetime] = None,
) -> Optional[SessionWindow]:
    ts = ts or _utcnow()
    return await next_session_after_async(
        mgr.repo,  # type: ignore[arg-type]
        mgr.tbl_market_schedule,
        ts,
    )


async def _needs_recovery(mgr: "AccountManager") -> bool:
    """Return True when core tables are empty and need recovery."""

    try:
        row = await mgr._db_fetchrow(
            f"""
            SELECT
                (SELECT COUNT(*) FROM {mgr.tbl_equity})   AS eqc,
                (SELECT COUNT(*) FROM {mgr.tbl_live})     AS livec,
                (SELECT COUNT(*) FROM {mgr.tbl_closed})   AS closedc
            """
        )
        if not row:
            return True
        equity_rows = row.get("eqc", 0) or 0
        live_rows = row.get("livec", 0) or 0
        closed_rows = row.get("closedc", 0) or 0
    except Exception as exc:
        mgr.log.debug("recovery check failed: %s", exc)
        return True

    if equity_rows == 0:
        return True

    return live_rows == 0 and closed_rows == 0


async def _market_schedule_worker(mgr: "AccountManager") -> None:
    """Keep the market schedule table in sync with Alpaca."""

    interval = mgr.MARKET_REFRESH_SEC
    first = True
    while True:
        if first:
            first = False
        else:
            await asyncio.sleep(interval)
        try:
            await _refresh_market_schedule(mgr)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            mgr.log.debug("market_schedule_worker note: %s", exc)


async def _bootstrap_for_session(mgr: "AccountManager") -> None:
    """Run the bootstrap sequence before a trading session starts."""

    async with mgr._snap_lock:
        mgr._snapshot_ready = False
        try:
            await mgr._bootstrap_cash_flows()
        except Exception as exc:
            mgr.log.debug("bootstrap flows note: %s", exc)

        try:
            await mgr._sync_closed_trades()
        except Exception as exc:
            mgr.log.debug("bootstrap closed trades note: %s", exc)

        try:
            await mgr._initial_snapshot()
        except Exception as exc:
            mgr.log.debug("bootstrap snapshot note: %s", exc)

        try:
            await mgr._rebuild_equity_full()
        except Exception as exc:
            mgr.log.debug("bootstrap equity rebuild note: %s", exc)

        try:
            await mgr._equity_intraday_backfill()
        except Exception as exc:
            mgr.log.debug("startup intraday backfill note: %s", exc)


async def _deactivate_session(mgr: "AccountManager") -> None:
    """Cancel active workers and reset the session state."""

    if not mgr._active_tasks:
        mgr._active_session = None
        return

    mgr.log.info(
        "Deactivating session; stopping %d tasks",
        len(mgr._active_tasks),
    )
    for task in mgr._active_tasks:
        task.cancel()
    with contextlib.suppress(Exception):
        await asyncio.gather(*mgr._active_tasks, return_exceptions=True)
    mgr._active_tasks.clear()
    mgr._active_session = None
    mgr._snapshot_ready = False


async def _activate_session(
    mgr: "AccountManager",
    session: SessionWindow,
) -> None:
    """Ensure workers are running for the provided session."""

    current = mgr._active_session
    if current and current.session_date == session.session_date:
        return

    await _deactivate_session(mgr)
    mgr.log.info(
        "Activating session %s (open %s, close %s)",
        session.session_date,
        session.open_utc.isoformat(),
        session.close_utc.isoformat(),
    )

    await _bootstrap_for_session(mgr)

    tasks = [
        asyncio.create_task(mgr._price_listener(), name="price_listener"),
        asyncio.create_task(mgr._db_worker(), name="db_worker"),
        asyncio.create_task(mgr._snapshot_loop(), name="snapshot_loop"),
        asyncio.create_task(
            mgr._closed_trades_worker(), name="closed_trades_worker"
        ),
        asyncio.create_task(mgr._equity_worker(), name="equity_worker"),
        asyncio.create_task(
            mgr._equity_intraday_worker(), name="equity_intraday"
        ),
        asyncio.create_task(mgr._cash_flows_worker(), name="cash_flows"),
        asyncio.create_task(
            mgr._ui_heartbeat_worker(), name="ui_heartbeat"
        ),
    ]
    if mgr.c.ENABLE_TRADE_STREAM:
        tasks.append(
            asyncio.create_task(mgr._run_trade_stream(), name="trade_stream")
        )
    mgr._active_tasks = tasks
    mgr._active_session = session


async def _schedule_supervisor(
    mgr: "AccountManager",
    stop_event: asyncio.Event,
) -> None:
    """Toggle worker sets based on the market schedule table."""

    poll = mgr.MARKET_POLL_SEC
    disable_sleep = mgr.disable_session_sleep
    while not stop_event.is_set():
        now = _utcnow()
        mgr._supervisor_last_tick = now
        next_check = now + timedelta(seconds=poll)
        try:
            session = await _schedule_current(mgr, now)

            if disable_sleep:
                candidate = (
                    session
                    or mgr._active_session
                    or await _schedule_next(mgr, now)
                )
                if not candidate:
                    await _refresh_market_schedule(mgr)
                    refreshed_now = _utcnow()
                    candidate = (
                        await _schedule_current(mgr, refreshed_now)
                        or await _schedule_next(mgr, refreshed_now)
                    )
                if candidate:
                    await _activate_session(mgr, candidate)
                else:
                    if await _needs_recovery(mgr):
                        mgr.log.info(
                            "schedule supervisor: core tables empty - "
                            "running recovery bootstrap"
                        )
                        try:
                            await _bootstrap_for_session(mgr)
                        except Exception as exc:
                            mgr.log.warning(
                                "recovery bootstrap failed: %s",
                                exc,
                            )
                    else:
                        mgr.log.debug(
                            "schedule supervisor: no session rows; "
                            "retrying"
                        )
            else:
                if session:
                    await _activate_session(mgr, session)
                    next_check = min(
                        session.post_close_utc,
                        now + timedelta(seconds=poll),
                    )
                else:
                    await _deactivate_session(mgr)
                    if await _needs_recovery(mgr):
                        mgr.log.info(
                            "schedule supervisor: core tables empty - "
                            "running recovery bootstrap"
                        )
                        try:
                            await _bootstrap_for_session(mgr)
                        except Exception as exc:
                            mgr.log.warning(
                                "recovery bootstrap failed: %s",
                                exc,
                            )
                        next_check = now + timedelta(seconds=poll)
                    else:
                        next_session = await _schedule_next(mgr, now)
                        if not next_session:
                            await _refresh_market_schedule(mgr)
                            next_session = await _schedule_next(mgr, now)
                        if next_session:
                            next_check = min(
                                next_session.pre_open_utc,
                                now + timedelta(seconds=poll),
                            )
                        else:
                            mgr.log.debug(
                                "market schedule has no future sessions; "
                                "retrying"
                            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            mgr.log.exception("schedule supervisor tick failed: %s", exc)

        wait = max(1.0, (next_check - _utcnow()).total_seconds())
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=wait)
        except asyncio.TimeoutError:
            continue
        else:
            break
