"""
Database storage layer for the Data Ingester.

Handles dynamic table creation per account slug and UPSERT
operations.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, List, Optional, Dict

from coldharbour_manager.infrastructure.db import AsyncAccountRepository
from .config import IngesterConfig

log = logging.getLogger("IngesterStorage")


def _table_name(base: str, slug: str) -> str:
    """Construct schema-qualified table name for a specific account slug."""
    # Example: account_activities.raw_orders_live
    safe_slug = slug.lower().replace("-", "_")
    return f"{IngesterConfig.DB_SCHEMA}.{base}_{safe_slug}"


async def ensure_schema_and_tables(
    repo: AsyncAccountRepository,
    slug: str
) -> None:
    """
    Ensure the schema and per-account tables exist idempotently.
    """
    schema = IngesterConfig.DB_SCHEMA
    t_orders = _table_name("raw_orders", slug)
    t_activities = _table_name("raw_activities", slug)

    await repo.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    # Orders table stores the mutable state of each order lifecycle.
    ddl_orders = f"""
        CREATE TABLE IF NOT EXISTS {t_orders} (
            id UUID PRIMARY KEY,
            client_order_id TEXT,
            request_id TEXT,
            parent_id TEXT,
            replaced_by TEXT,
            replaces TEXT,

            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            order_type TEXT NOT NULL,
            status TEXT NOT NULL,

            qty NUMERIC,
            filled_qty NUMERIC DEFAULT 0,
            filled_avg_price NUMERIC,
            limit_price NUMERIC,
            stop_price NUMERIC,

            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ,
            submitted_at TIMESTAMPTZ,
            filled_at TIMESTAMPTZ,
            expired_at TIMESTAMPTZ,
            canceled_at TIMESTAMPTZ,
            replaced_at TIMESTAMPTZ,
            expires_at TIMESTAMPTZ,
            raw_json JSONB,
            legs JSONB,

            ingested_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_{slug}_orders_updated ON {t_orders}(updated_at);
        CREATE INDEX IF NOT EXISTS idx_{slug}_orders_symbol ON {t_orders}(symbol);
        CREATE INDEX IF NOT EXISTS idx_{slug}_orders_created ON {t_orders}(created_at);
    """
    await repo.execute(ddl_orders)

    # Activities table stores append-only history keyed by Time::UUID.
    ddl_activities = f"""
        CREATE TABLE IF NOT EXISTS {t_activities} (
            id TEXT PRIMARY KEY,
            
            activity_type TEXT NOT NULL,
            transaction_time TIMESTAMPTZ NOT NULL,
            
            symbol TEXT,
            side TEXT,
            qty NUMERIC,
            price NUMERIC,
            net_amount NUMERIC,
            
            execution_id UUID,
            order_id UUID,
            
            raw_json JSONB,
            ingested_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_{slug}_activities_time ON {t_activities}(transaction_time);
        CREATE INDEX IF NOT EXISTS idx_{slug}_activities_type ON {t_activities}(activity_type);
        CREATE INDEX IF NOT EXISTS idx_{slug}_activities_exec_id ON {t_activities}(execution_id);
    """
    await repo.execute(ddl_activities)

    # Normalize activity_type casing to keep partial indexes consistent.
    await repo.execute(
        f"""
        UPDATE {t_activities}
        SET activity_type = UPPER(activity_type)
        WHERE activity_type IS NOT NULL
          AND activity_type <> UPPER(activity_type);
        """
    )

    # Deduplicate fills by execution_id, preferring REST rows over stream
    # payloads when both exist.
    await repo.execute(
        f"""
        WITH ranked AS (
            SELECT
                ctid,
                ROW_NUMBER() OVER (
                    PARTITION BY execution_id
                    ORDER BY (raw_json ? 'event')::int ASC, ingested_at DESC
                ) AS rn
            FROM {t_activities}
            WHERE activity_type = 'FILL' AND execution_id IS NOT NULL
        )
        DELETE FROM {t_activities}
        WHERE ctid IN (SELECT ctid FROM ranked WHERE rn > 1);
        """
    )

    # Ensure fills are unique per execution_id to prevent stream/REST doubles.
    await repo.execute(
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_{slug}_activities_exec
        ON {t_activities}(execution_id)
        WHERE activity_type = 'FILL' AND execution_id IS NOT NULL;
        """
    )


async def ensure_history_meta_table(
    repo: AsyncAccountRepository,
    slug: str
) -> None:
    """
    Ensure the history metadata table exists for the account slug.
    Store timestamps as a key-value metric record.
    """
    t_meta = _table_name("raw_history_meta", slug)

    ddl = f"""
        CREATE TABLE IF NOT EXISTS {t_meta} (
            metric_key TEXT PRIMARY KEY,
            metric_value TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
    """
    await repo.execute(ddl)


async def upsert_history_meta(
    repo: AsyncAccountRepository,
    slug: str,
    metrics: dict[str, datetime]
) -> None:
    """
    Upsert metadata keys and timestamps for history boundaries.
    """
    if not metrics:
        return

    t_meta = _table_name("raw_history_meta", slug)
    values = [(k, v) for k, v in metrics.items() if v is not None]

    sql = f"""
        INSERT INTO {t_meta} (metric_key, metric_value)
        VALUES ($1, $2)
        ON CONFLICT (metric_key) DO UPDATE SET
            metric_value = EXCLUDED.metric_value,
            updated_at = NOW();
    """

    await repo.executemany(sql, values)


async def get_latest_order_time(
    repo: AsyncAccountRepository,
    slug: str
) -> Optional[datetime]:
    """
    Return the timestamp of the most recently updated order.
    Use it to decide if backfill is needed or where to begin healing.
    """
    t_orders = _table_name("raw_orders", slug)
    try:
        # We check updated_at because it reflects the last change
        row = await repo.fetchrow(
            f"SELECT MAX(updated_at) as last_ts FROM {t_orders}"
        )
        if row and row.get("last_ts"):
            return row["last_ts"]
    except Exception as e:
        # If table doesn't exist yet, return None
        log.debug(f"Could not fetch latest order time for {slug}: {e}")
    return None


async def upsert_orders(
    repo: AsyncAccountRepository,
    slug: str,
    orders: List[Dict[str, Any]]
) -> int:
    """
    Batch UPSERT orders, fully overwriting existing rows on conflict.
    """
    if not orders:
        return 0

    t_orders = _table_name("raw_orders", slug)
    
    # Columns matching the dict keys from transform.normalize_order
    cols = [
        "id", "client_order_id", "request_id", "parent_id", "replaced_by",
        "replaces", "symbol", "side", "order_type", "status",
        "qty", "filled_qty", "filled_avg_price", "limit_price", "stop_price",
        "created_at", "updated_at", "submitted_at", "filled_at",
        "expired_at", "canceled_at", "replaced_at", "expires_at", "raw_json",
        "legs",
        "ingested_at"
    ]
    
    values = []
    for o in orders:
        values.append(tuple(o.get(c) for c in cols))

    placeholders = ",".join(f"${i+1}" for i in range(len(cols)))
    
    # Force full overwrite: update every column except `id`.
    update_cols = [c for c in cols if c != "id"]
    updates = ",".join(f"{c}=EXCLUDED.{c}" for c in update_cols)

    sql = f"""
        INSERT INTO {t_orders} ({",".join(cols)})
        VALUES ({placeholders})
        ON CONFLICT (id) DO UPDATE SET
        {updates}
    """

    await repo.executemany(sql, values)
    return len(values)


async def upsert_activities(
    repo: AsyncAccountRepository,
    slug: str,
    activities: List[Dict[str, Any]]
) -> int:
    """
    Batch insert activities while deduplicating fills by execution_id.
    """
    if not activities:
        return 0

    t_activities = _table_name("raw_activities", slug)

    # Columns matching transform.normalize_stream_activity / normalize_rest_activity
    cols = [
        "id", "activity_type", "transaction_time", "symbol", "side",
        "qty", "price", "net_amount", "execution_id", "order_id",
        "raw_json", "ingested_at"
    ]

    def _extract_execution_id(activity_id: Any) -> Optional[str]:
        if not activity_id:
            return None
        activity_id = str(activity_id)
        if "::" not in activity_id:
            return None
        return activity_id.split("::", 1)[1]

    fill_values: List[tuple[Any, ...]] = []
    other_values: List[tuple[Any, ...]] = []

    for raw in activities:
        activity = dict(raw)
        activity_type = str(activity.get("activity_type") or "").upper()
        if activity_type:
            activity["activity_type"] = activity_type

        if activity_type == "FILL":
            exec_id = activity.get("execution_id") or _extract_execution_id(
                activity.get("id")
            )
            if exec_id:
                activity["execution_id"] = exec_id
                fill_values.append(tuple(activity.get(c) for c in cols))
                continue

        other_values.append(tuple(activity.get(c) for c in cols))

    placeholders = ",".join(f"${i+1}" for i in range(len(cols)))
    total = 0

    if other_values:
        sql = f"""
            INSERT INTO {t_activities} ({",".join(cols)})
            VALUES ({placeholders})
            ON CONFLICT (id) DO NOTHING
        """
        await repo.executemany(sql, other_values)
        total += len(other_values)

    if fill_values:
        update_cols = [c for c in cols if c != "id"]
        updates = ",".join(f"{c}=EXCLUDED.{c}" for c in update_cols)
        sql = f"""
            INSERT INTO {t_activities} ({",".join(cols)})
            VALUES ({placeholders})
            ON CONFLICT (execution_id)
            WHERE activity_type = 'FILL' AND execution_id IS NOT NULL
            DO UPDATE SET {updates}
        """
        await repo.executemany(sql, fill_values)
        total += len(fill_values)

    return total
