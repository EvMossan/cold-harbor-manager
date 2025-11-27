"""Helpers for building and querying market session schedules.

The AccountManager relies on a dedicated ``market_schedule`` table that
stores one row per trading day with a pre-market wake time and the actual
session boundaries. This module centralises the logic required to build
that table from the Alpaca calendar API and to query it at runtime.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Iterable, List, Optional

import pandas as pd
from alpaca_trade_api.rest import REST
from sqlalchemy import text
from sqlalchemy.engine import Engine
from zoneinfo import ZoneInfo

from cold_harbour.infrastructure.db import AsyncAccountRepository

NY_TZ = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class SessionWindow:
    """Container describing a single trading session."""

    session_date: date
    pre_open_utc: datetime
    open_utc: datetime
    close_utc: datetime
    post_close_utc: datetime

    def as_dict(self) -> dict[str, object]:
        """Return a mapping suitable for parametrised SQL statements."""

        return {
            "session_date": self.session_date,
            "pre_open_utc": self.pre_open_utc,
            "open_utc": self.open_utc,
            "close_utc": self.close_utc,
            "post_close_utc": self.post_close_utc,
        }


def _parse_calendar(
    cal: Iterable[object],
    *,
    pre_open_time: time,
) -> List[SessionWindow]:
    """Return structured sessions from Alpaca calendar entries."""

    rows: List[SessionWindow] = []
    data = list(cal)
    if not data:
        return rows

    frame = pd.DataFrame(getattr(entry, "_raw", entry) for entry in data)
    if frame.empty:
        return rows

    # Normalise date and open/close columns to timezone-aware datetimes.
    frame["session_date"] = pd.to_datetime(frame["date"], utc=False).dt.date
    frame["open_local"] = (
        pd.to_datetime(frame["date"] + " " + frame["open"] + ":00")
        .dt.tz_localize(NY_TZ)
    )
    frame["close_local"] = (
        pd.to_datetime(frame["date"] + " " + frame["close"] + ":00")
        .dt.tz_localize(NY_TZ)
    )

    for row in frame.itertuples(index=False):
        sess_date: date = getattr(row, "session_date")
        open_local: pd.Timestamp = getattr(row, "open_local")
        close_local: pd.Timestamp = getattr(row, "close_local")

        pre_local = datetime.combine(sess_date, pre_open_time, tzinfo=NY_TZ)
        pre_utc = pre_local.astimezone(timezone.utc)
        open_utc = open_local.astimezone(timezone.utc).to_pydatetime()
        close_utc = close_local.astimezone(timezone.utc).to_pydatetime()

        # Extend the session to cover post-market trading (20:00 NY).
        post_local = datetime.combine(
            sess_date,
            time(hour=20, minute=0),
            tzinfo=NY_TZ,
        )
        post_close_utc = post_local.astimezone(timezone.utc)

        rows.append(
            SessionWindow(
                session_date=sess_date,
                pre_open_utc=pre_utc,
                open_utc=open_utc,
                close_utc=close_utc,
                post_close_utc=post_close_utc,
            )
        )

    return rows


def fetch_sessions(
    rest: REST,
    *,
    start: date,
    end: date,
    pre_open_time: time,
) -> List[SessionWindow]:
    """Return session windows between ``start`` and ``end`` inclusive."""

    cal = rest.get_calendar(start=start, end=end)
    return _parse_calendar(cal, pre_open_time=pre_open_time)


def sync_schedule(
    engine: Engine,
    table: str,
    rest: REST,
    *,
    lookback_days: int = 7,
    forward_days: int = 21,
    pre_open_time: time = time(hour=4, minute=0),
    retain_days: int = 120,
) -> int:
    """Upsert upcoming sessions into ``table`` and trim stale rows.

    Returns the number of sessions processed. All operations are executed in
    a single transaction for safety.
    """

    today = date.today()
    start = today - timedelta(days=lookback_days)
    end = today + timedelta(days=forward_days)

    sessions = fetch_sessions(
        rest,
        start=start,
        end=end,
        pre_open_time=pre_open_time,
    )
    if not sessions:
        return 0

    with engine.begin() as conn:
        for sess in sessions:
            conn.execute(
                text(
                    f"""
                    INSERT INTO {table} (
                        session_date,
                        pre_open_utc,
                        open_utc,
                        close_utc,
                        post_close_utc,
                        updated_at
                    ) VALUES (
                        :session_date,
                        :pre_open_utc,
                        :open_utc,
                        :close_utc,
                        :post_close_utc,
                        now()
                    )
                    ON CONFLICT (session_date) DO UPDATE SET
                        pre_open_utc   = EXCLUDED.pre_open_utc,
                        open_utc       = EXCLUDED.open_utc,
                        close_utc      = EXCLUDED.close_utc,
                        post_close_utc = EXCLUDED.post_close_utc,
                        updated_at     = now()
                    """
                ),
                sess.as_dict(),
            )

        cutoff = today - timedelta(days=retain_days)
        conn.execute(
            text(
                f"DELETE FROM {table} WHERE session_date < :cutoff"
            ),
            {"cutoff": cutoff},
        )

    return len(sessions)


def _row_to_session(
    row: Optional[dict[str, object]],
) -> Optional[SessionWindow]:
    if not row:
        return None
    return SessionWindow(
        session_date=row["session_date"],
        pre_open_utc=row["pre_open_utc"],
        open_utc=row["open_utc"],
        close_utc=row["close_utc"],
        post_close_utc=row["post_close_utc"],
    )


def session_for_timestamp(
    engine: Engine,
    table: str,
    ts: datetime,
) -> Optional[SessionWindow]:
    """Return the session that covers ``ts`` or ``None`` when outside."""

    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    with engine.begin() as conn:
        row = conn.execute(
            text(
                f"""
                SELECT session_date,
                       pre_open_utc,
                       open_utc,
                       close_utc,
                       post_close_utc
                  FROM {table}
                 WHERE pre_open_utc <= :ts
                   AND post_close_utc > :ts
              ORDER BY pre_open_utc DESC
                 LIMIT 1
                """
            ),
            {"ts": ts},
        ).mappings().first()
    return _row_to_session(row)


def next_session_after(
    engine: Engine,
    table: str,
    ts: datetime,
) -> Optional[SessionWindow]:
    """Return the first session whose ``pre_open_utc`` is after ``ts``."""

    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    with engine.begin() as conn:
        row = conn.execute(
            text(
                f"""
                SELECT session_date,
                       pre_open_utc,
                       open_utc,
                       close_utc,
                       post_close_utc
                  FROM {table}
                 WHERE pre_open_utc > :ts
              ORDER BY pre_open_utc
                 LIMIT 1
                """
            ),
            {"ts": ts},
        ).mappings().first()
    return _row_to_session(row)


# ---------------------------------------------------------------------------
# Async variants
# ---------------------------------------------------------------------------


async def sync_schedule_async(
    repo: AsyncAccountRepository,
    table: str,
    rest: REST,
    *,
    lookback_days: int = 7,
    forward_days: int = 21,
    pre_open_time: time = time(hour=4, minute=0),
    retain_days: int = 120,
) -> int:
    """Async version of ``sync_schedule`` using asyncpg."""

    today = date.today()
    start = today - timedelta(days=lookback_days)
    end = today + timedelta(days=forward_days)

    sessions = fetch_sessions(
        rest,
        start=start,
        end=end,
        pre_open_time=pre_open_time,
    )
    if not sessions:
        return 0

    rows = [
        (
            sess.session_date,
            sess.pre_open_utc,
            sess.open_utc,
            sess.close_utc,
            sess.post_close_utc,
        )
        for sess in sessions
    ]
    insert_sql = f"""
    INSERT INTO {table} (
        session_date,
        pre_open_utc,
        open_utc,
        close_utc,
        post_close_utc,
        updated_at
    ) VALUES (
        $1, $2, $3, $4, $5, now()
    )
    ON CONFLICT (session_date) DO UPDATE SET
        pre_open_utc   = EXCLUDED.pre_open_utc,
        open_utc       = EXCLUDED.open_utc,
        close_utc      = EXCLUDED.close_utc,
        post_close_utc = EXCLUDED.post_close_utc,
        updated_at     = now();
    """
    cutoff = today - timedelta(days=retain_days)

    async with repo.pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(insert_sql, rows)
            await conn.execute(
                f"DELETE FROM {table} WHERE session_date < $1",
                cutoff,
            )

    return len(sessions)


async def session_for_timestamp_async(
    repo: AsyncAccountRepository,
    table: str,
    ts: datetime,
) -> Optional[SessionWindow]:
    """Async version of ``session_for_timestamp``."""

    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    async with repo.pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT session_date,
                   pre_open_utc,
                   open_utc,
                   close_utc,
                   post_close_utc
              FROM {table}
             WHERE pre_open_utc <= $1
               AND post_close_utc > $1
          ORDER BY pre_open_utc DESC
             LIMIT 1
            """,
            ts,
        )
    return _row_to_session(dict(row) if row else None)


async def next_session_after_async(
    repo: AsyncAccountRepository,
    table: str,
    ts: datetime,
) -> Optional[SessionWindow]:
    """Async version of ``next_session_after``."""

    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    async with repo.pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT session_date,
                   pre_open_utc,
                   open_utc,
                   close_utc,
                   post_close_utc
              FROM {table}
             WHERE pre_open_utc > $1
          ORDER BY pre_open_utc
             LIMIT 1
            """,
            ts,
        )
    return _row_to_session(dict(row) if row else None)


# ---------------------------------------------------------------------------
# Backwards-compatible helpers used by legacy modules
# ---------------------------------------------------------------------------


def market_status(config: dict) -> bool:
    """Return ``True`` when the market is currently open.

    The ``config`` mapping may contain generic keys (``API_KEY`` /
    ``SECRET_KEY`` / ``ALPACA_BASE_URL``) or the older ``*_JOHNYSSAN``
    variants. This keeps legacy scripts working while the new schedule
    table is used elsewhere.
    """

    api_key = (
        config.get("API_KEY")
        or config.get("API_KEY_JOHNYSSAN")
    )
    secret_key = (
        config.get("SECRET_KEY")
        or config.get("SECRET_KEY_JOHNYSSAN")
    )
    base_url = config.get(
        "ALPACA_BASE_URL",
        config.get(
            "ALPACA_BASE_URL_PAPER",
            "https://paper-api.alpaca.markets",
        ),
    )
    if not api_key or not secret_key:
        raise ValueError("API credentials are required to query market status")

    rest = REST(api_key, secret_key, base_url)
    now = datetime.now(timezone.utc)
    sessions = fetch_sessions(
        rest,
        start=now.date() - timedelta(days=1),
        end=now.date() + timedelta(days=1),
        pre_open_time=time(hour=4),
    )
    for sess in sessions:
        if sess.open_utc <= now < sess.close_utc:
            return True
    return False


def latest_sessions(
    api_key: str,
    secret_key: str,
    lookback_days: int = 30,
    *,
    base_url: str | None = None,
) -> pd.DataFrame:
    """Return recent sessions as a DataFrame (legacy helper)."""

    rest = REST(api_key, secret_key, base_url)
    today = date.today()
    cal = rest.get_calendar(
        start=today - timedelta(days=lookback_days),
        end=today,
    )
    sessions = _parse_calendar(cal, pre_open_time=time(hour=4))
    if not sessions:
        return pd.DataFrame(
            columns=[
                "session_date",
                "open_utc",
                "close_utc",
                "pre_open_utc",
                "post_close_utc",
            ]
        )

    data = [
        {
            "session_date": sess.session_date,
            "open_utc": sess.open_utc,
            "close_utc": sess.close_utc,
            "pre_open_utc": sess.pre_open_utc,
            "post_close_utc": sess.post_close_utc,
        }
        for sess in sessions
    ]
    return pd.DataFrame(data)
