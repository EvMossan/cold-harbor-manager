"""Provide pure helper utilities for the AccountManager runtime."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Optional

import pandas as pd


def _utcnow() -> datetime:
    """Return a timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


def _json_safe(v: Any) -> Any:
    """Convert DB, numpy, or datetime values to JSON-safe primitives."""
    if isinstance(v, (datetime, date, pd.Timestamp)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    return v


def _parse_bool(value: Any, default: bool = False) -> bool:
    """Return a boolean from common string or numeric representations."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _is_at_break_even(
    side: str,
    entry: float,
    stop_px: Optional[float],
    min_stop_gap: float,
) -> bool:
    """Return whether the stop sits at or inside break-even while
    respecting tick size."""
    if stop_px is None:
        return False

    gap = max(min_stop_gap, 0.0001 if entry < 1 else 0.01)
    tick = 0.0001 if entry < 1 else 0.01

    legal_be = entry - gap if side.lower() == "long" else entry + gap
    steps = round(legal_be / tick)
    legal_be = round(steps * tick, 4 if entry < 1 else 2)

    if side.lower() == "long":
        return stop_px >= legal_be
    return stop_px <= legal_be


def _is_flat(qty: float) -> bool:
    """Return whether qty is effectively zero within a tight tolerance."""
    return abs(qty) < 1e-12
