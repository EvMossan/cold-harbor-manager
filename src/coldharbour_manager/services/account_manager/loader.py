"""Load orders and activities for AccountManager from local schema."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

import pandas as pd

from coldharbour_manager.infrastructure.db import AsyncAccountRepository
from coldharbour_manager.services.account_manager.utils import _utcnow


async def load_orders_from_db(
    repo: AsyncAccountRepository,
    slug: str,
    days_back: int = 365,
) -> pd.DataFrame:
    """Load historical orders from account_activities raw order tables."""
    safe_slug = slug.replace("-", "_").lower()
    table_name = f"account_activities.raw_orders_{safe_slug}"

    cutoff = _utcnow() - timedelta(days=days_back)
    sql = f"""
        SELECT
            id, client_order_id, parent_id, symbol, side, order_type, status,
            qty, filled_qty, filled_avg_price, limit_price, stop_price,
            created_at, updated_at, submitted_at, filled_at,
            expires_at, expired_at, canceled_at, replaced_by, replaces,
            legs, raw_json
        FROM {table_name}
        WHERE created_at >= $1
        ORDER BY created_at DESC
    """
    try:
        rows = await repo.fetch(sql, cutoff)
    except Exception:
        return pd.DataFrame()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    date_cols = [
        "created_at",
        "updated_at",
        "submitted_at",
        "filled_at",
        "expires_at",
        "expired_at",
        "canceled_at",
    ]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True)

    num_cols = [
        "qty",
        "filled_qty",
        "filled_avg_price",
        "limit_price",
        "stop_price",
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    uuid_cols = ["id", "parent_id", "client_order_id", "replaced_by", "replaces"]
    for col in uuid_cols:
        if col in df.columns:
            # Convert to str while leaving None unchanged.
            df[col] = df[col].apply(lambda x: str(x) if x else None)

    return df


async def load_activities_from_db(
    repo: AsyncAccountRepository, slug: str
) -> pd.DataFrame:
    """Load historical activities from account_activities raw tables."""
    safe_slug = slug.replace("-", "_").lower()
    table_name = f"account_activities.raw_activities_{safe_slug}"

    sql = f"""
        SELECT
            id, activity_type, transaction_time, symbol, side,
            qty, price, net_amount, execution_id, order_id
        FROM {table_name}
        ORDER BY transaction_time DESC
    """

    try:
        rows = await repo.fetch(sql)
    except Exception:
        return pd.DataFrame()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    if "transaction_time" in df.columns:
        df["exec_time"] = pd.to_datetime(
            df["transaction_time"], utc=True
        )
        df["transaction_time"] = df["exec_time"]

    for col in ("qty", "price", "net_amount"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    uuid_cols = ["id", "order_id", "execution_id"]
    for col in uuid_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: str(x) if x else None)

    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.upper()
    if "activity_type" in df.columns:
        df["activity_type"] = df["activity_type"].astype(str).str.upper()
    if "side" in df.columns:
        df["side"] = df["side"].astype(str).str.lower()

    return df


async def fetch_history_meta_dates(
    repo: AsyncAccountRepository,
    slug: str,
) -> Tuple[Optional[datetime], Optional[datetime]]:
    """Return cached earliest order and activity timestamps or
    (None, None) when table data is absent.
    """
    safe_slug = slug.replace("-", "_").lower()
    table_name = f"account_activities.raw_history_meta_{safe_slug}"

    sql = f"""
        SELECT metric_key, metric_value
        FROM {table_name}
        WHERE metric_key IN ('earliest_order', 'earliest_activity')
    """

    try:
        rows = await repo.fetch(sql)
    except Exception:
        return None, None

    data = {r["metric_key"]: r["metric_value"] for r in rows}

    e_order = data.get("earliest_order")
    e_activity = data.get("earliest_activity")

    if e_order and e_order.tzinfo is None:
        e_order = e_order.replace(tzinfo=timezone.utc)
    if e_activity and e_activity.tzinfo is None:
        e_activity = e_activity.replace(tzinfo=timezone.utc)

    return e_order, e_activity
