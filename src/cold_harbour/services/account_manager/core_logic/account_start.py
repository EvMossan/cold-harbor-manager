"""Shared helpers to derive the earliest known activity date for an account.

Sources checked (best-effort, tolerant to missing tables/columns):
- Closed trades: ``entry_time`` and ``exit_time``
- Cash flows: ``ts``
- Open positions: ``filled_at``

When nothing is found, fall back to ``today - fallback_years``.
"""

from __future__ import annotations

import datetime as dt
from typing import Dict, Iterable, Optional

from sqlalchemy import text

from cold_harbour.infrastructure.db import AsyncAccountRepository


def _parse_cash_flow_types(raw_types: Optional[Iterable[str]] | str) -> str:
    if raw_types is None:
        return ""
    if isinstance(raw_types, str):
        tokens = [t.strip() for t in raw_types.split(",") if t.strip()]
    else:
        tokens = [str(t).strip() for t in raw_types if str(t).strip()]
    return " OR ".join([f"type ILIKE '{t}%'" for t in tokens])


def earliest_activity_date(
    engine,
    tables: Dict[str, str],
    *,
    cash_flow_types: Optional[Iterable[str]] | str = None,
    fallback_years: int = 5,
) -> dt.date:
    """Return the earliest known activity date for this account (sync)."""

    today = dt.date.today()
    fallback = today - dt.timedelta(days=365 * max(1, fallback_years))
    dates: list[dt.date] = []

    where_types = _parse_cash_flow_types(cash_flow_types) or "FALSE"

    with engine.begin() as conn:
        # Closed trades
        try:
            d = conn.execute(
                text(
                    f"""
                    SELECT MIN(LEAST(entry_time::date, exit_time::date))
                      FROM {tables['closed']}
                    """
                )
            ).scalar()
            if d:
                dates.append(d)
        except Exception:
            pass

        # Cash flows
        try:
            d = conn.execute(
                text(
                    f"""
                    SELECT MIN(ts::date)
                      FROM {tables['cash_flows']}
                     WHERE {where_types}
                    """
                )
            ).scalar()
            if d:
                dates.append(d)
        except Exception:
            pass

        # Open positions
        try:
            d = conn.execute(
                text(
                    f"""
                    SELECT MIN(filled_at::date)
                      FROM {tables['open']}
                    """
                )
            ).scalar()
            if d:
                dates.append(d)
        except Exception:
            pass

    return min(dates) if dates else fallback


async def earliest_activity_date_async(
    repo: AsyncAccountRepository,
    tables: Dict[str, str],
    *,
    cash_flow_types: Optional[Iterable[str]] | str = None,
    fallback_years: int = 5,
) -> dt.date:
    """Async version of earliest activity date lookup."""

    today = dt.date.today()
    fallback = today - dt.timedelta(days=365 * max(1, fallback_years))
    dates: list[dt.date] = []

    where_types = _parse_cash_flow_types(cash_flow_types) or "FALSE"

    # Closed trades
    try:
        row = await repo.fetchrow(
            f"""
            SELECT MIN(LEAST(entry_time::date, exit_time::date)) AS d
              FROM {tables['closed']}
            """
        )
        d = (row or {}).get("d")
        if d:
            dates.append(d)
    except Exception:
        pass

    # Cash flows
    try:
        row = await repo.fetchrow(
            f"""
            SELECT MIN(ts::date) AS d
              FROM {tables['cash_flows']}
             WHERE {where_types}
            """
        )
        d = (row or {}).get("d")
        if d:
            dates.append(d)
    except Exception:
        pass

    # Open positions
    try:
        row = await repo.fetchrow(
            f"""
            SELECT MIN(filled_at::date) AS d
              FROM {tables['open']}
            """
        )
        d = (row or {}).get("d")
        if d:
            dates.append(d)
    except Exception:
        pass

    return min(dates) if dates else fallback
