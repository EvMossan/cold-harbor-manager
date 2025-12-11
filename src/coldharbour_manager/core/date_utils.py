"""Date helpers shared between web and manager runtimes."""

from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from zoneinfo import ZoneInfo

import pandas as pd
import pandas_market_calendars as mcal

_NYSE_CAL = mcal.get_calendar("NYSE")


@lru_cache(maxsize=128)
def _nyse_valid_days(start: date, end: date) -> tuple[date, ...]:
    """Return NYSE trading days in [start, end] inclusive."""
    start_iso = start.isoformat()
    end_iso = end.isoformat()
    days = _NYSE_CAL.valid_days(start_date=start_iso, end_date=end_iso)
    return tuple(pd.Timestamp(d).date() for d in days)


def trading_session_date(now_ts: datetime) -> date:
    """Return the most recent NYSE trading day."""
    if now_ts.tzinfo is None:
        now_ts = now_ts.replace(tzinfo=timezone.utc)

    ny_tz = ZoneInfo("America/New_York")
    now_ny = now_ts.astimezone(ny_tz)
    base_date = now_ny.date()
    if now_ny.hour < 4:
        base_date -= timedelta(days=1)

    search_end = base_date
    search_start = search_end - timedelta(days=10)

    while True:
        days = _nyse_valid_days(search_start, search_end)
        for day in reversed(days):
            if day <= base_date:
                return day

        search_end = search_start - timedelta(days=1)
        search_start = search_end - timedelta(days=10)
#