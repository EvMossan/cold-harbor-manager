"""
Database storage layer for the Data Ingester.
Handles dynamic table creation per account slug and UPSERT operations.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, List, Optional, Dict

from cold_harbour.infrastructure.db import AsyncAccountRepository
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
    Idempotently create the schema and per-account tables.
    """
    schema = IngesterConfig.DB_SCHEMA
    t_orders = _table_name("raw_orders", slug)
    t_activities = _table_name("raw_activities", slug)

    # 1. Ensure Schema
    await repo.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    # 2. Table: Orders (Mutable state)
    # We store the full lifecycle of an order here.
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

            ingested_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_{slug}_orders_updated ON {t_orders}(updated_at);
        CREATE INDEX IF NOT EXISTS idx_{slug}_orders_symbol ON {t_orders}(symbol);
        CREATE INDEX IF NOT EXISTS idx_{slug}_orders_created ON {t_orders}(created_at);
    """
    await repo.execute(ddl_orders)

    # 3. Table: Activities (Append-only history)
    # ID is the synthetic "Time::UUID" key for deduplication.
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


async def get_latest_order_time(
    repo: AsyncAccountRepository,
    slug: str
) -> Optional[datetime]:
    """
    Find the timestamp of the most recently updated order.
    Used to determine if backfill is needed or where to start 'healing'.
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
    Batch UPSERT orders. Updates status/filled_qty if order exists.
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
        "expired_at", "canceled_at", "replaced_at", "ingested_at"
    ]
    
    # Prepare values list
    values = []
    for o in orders:
        values.append(tuple(o.get(c) for c in cols))

    # Construct placeholders ($1, $2, ...)
    placeholders = ",".join(f"${i+1}" for i in range(len(cols)))
    
    # Build UPDATE clause for ON CONFLICT
    # We update fields that can change during order lifecycle
    update_cols = [
        "status", "filled_qty", "filled_avg_price", "updated_at",
        "filled_at", "expired_at", "canceled_at", "replaced_at",
        "replaced_by"
    ]
    updates = ",".join(f"{c}=EXCLUDED.{c}" for c in update_cols)

    sql = f"""
        INSERT INTO {t_orders} ({",".join(cols)})
        VALUES ({placeholders})
        ON CONFLICT (id) DO UPDATE SET
        {updates}
    """

    # AsyncAccountRepository.executemany handles batching logic internally via asyncpg
    await repo.executemany(sql, values)
    return len(values)


async def upsert_activities(
    repo: AsyncAccountRepository,
    slug: str,
    activities: List[Dict[str, Any]]
) -> int:
    """
    Batch INSERT activities. Ignores duplicates (idempotent).
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

    values = []
    for a in activities:
        values.append(tuple(a.get(c) for c in cols))

    placeholders = ",".join(f"${i+1}" for i in range(len(cols)))

    # ON CONFLICT DO NOTHING because activities are immutable history events.
    # If we already have this ID (Time::UUID), we skip it.
    sql = f"""
        INSERT INTO {t_activities} ({",".join(cols)})
        VALUES ({placeholders})
        ON CONFLICT (id) DO NOTHING
    """

    await repo.executemany(sql, values)
    return len(values)
