"""Async workers for Data Ingester: Stream, Backfill, and Healing."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional, Dict

import pandas as pd
from alpaca_trade_api.rest import REST
from alpaca_trade_api.stream import Stream

from cold_harbour.core.account_analytics import (
    fetch_all_activities,
    fetch_orders,
    get_history_start_dates,
)
from cold_harbour.infrastructure.db import AsyncAccountRepository
from . import storage, transform
from .config import IngesterConfig

log = logging.getLogger("IngesterWorkers")


def _get_rest_client(api_key: str, secret_key: str, base_url: str) -> REST:
    return REST(api_key, secret_key, base_url)


def _timestamp_to_datetime(value: Any) -> Optional[datetime]:
    """Convert pandas-compatible timestamps to datetimes."""
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, datetime):
        return value
    return None


def _df_to_ingest_records(df: pd.DataFrame) -> List[dict[str, Any]]:
    """Transform fetched order table into storage-ready records."""
    ingest_time = datetime.now(timezone.utc)
    records: List[dict[str, Any]] = []
    for _, row in df.iterrows():
        records.append(
            {
                "id": row.get("id"),
                "client_order_id": row.get("client_order_id"),
                "parent_id": row.get("parent_id"),
                "symbol": row.get("symbol"),
                "side": row.get("side"),
                "order_type": row.get("order_type"),
                "status": row.get("status"),
                "qty": row.get("qty"),
                "filled_qty": row.get("filled_qty"),
                "filled_avg_price": row.get("filled_avg_price"),
                "limit_price": row.get("limit_price"),
                "stop_price": row.get("stop_price"),
                "created_at": _timestamp_to_datetime(row.get("created_at")),
                "updated_at": _timestamp_to_datetime(row.get("updated_at")),
                "submitted_at": _timestamp_to_datetime(row.get("submitted_at")),
                "filled_at": _timestamp_to_datetime(row.get("filled_at")),
                "expired_at": _timestamp_to_datetime(row.get("expired_at")),
                "canceled_at": _timestamp_to_datetime(row.get("canceled_at")),
                "ingested_at": ingest_time,
            }
        )
    return records


def _df_to_activity_records(
    df: pd.DataFrame,
    default_time: Optional[datetime] = None,
) -> List[dict[str, Any]]:
    """Transform fetched activity table into storage-ready records."""
    ingest_time = datetime.now(timezone.utc)
    records: List[dict[str, Any]] = []
    for _, row in df.iterrows():
        def _safe_value(key: str) -> Any:
            value = row.get(key)
            return None if pd.isna(value) else value

        net_amount = _safe_value("net_amount")
        if net_amount is None:
            net_amount = _safe_value("amount")
        if net_amount is None:
            net_amount = 0

        transaction_time = _timestamp_to_datetime(row.get("exec_time"))
        if transaction_time is None:
            transaction_time = (
                default_time or datetime.now(timezone.utc)
            )
        records.append(
            {
                "id": row.get("id"),
                "activity_type": _safe_value("activity_type"),
                "transaction_time": transaction_time,
                "symbol": _safe_value("symbol"),
                "side": _safe_value("side"),
                "qty": _safe_value("qty"),
                "price": _safe_value("price"),
                "net_amount": net_amount,
                "order_id": _safe_value("order_id"),
                "execution_id": None,
                "raw_json": None,
                "ingested_at": ingest_time,
            }
        )
    return records


# -------------------------------------------------------------------------
# Helper for JSON Logging
# -------------------------------------------------------------------------
def _json_dumper(obj: Any) -> str:
    """Safe JSON dumper for logging purposes."""
    def _default(o):
        if isinstance(o, (datetime, pd.Timestamp)):
            return o.isoformat()
        if hasattr(o, "__str__"):
            return str(o)
        return str(type(o))
    try:
        return json.dumps(obj, default=_default)
    except Exception:
        return str(obj)


async def run_backfill_task(
    repo: AsyncAccountRepository,
    api_key: str,
    secret_key: str,
    base_url: str,
    slug: str,
) -> None:
    """
    One-off task on startup.
    Uses the latest order time as a watermark for fetch_orders.
    """
    log.info(f"[{slug}] Starting Backfill/Catch-up check...")
    rest = _get_rest_client(api_key, secret_key, base_url)
    
    last_update = await storage.get_latest_order_time(repo, slug)

    if last_update:
        order_start_date = last_update - timedelta(minutes=30)
        activity_start_date = order_start_date
        log.info(
            f"[{slug}] Incremental backfill from {order_start_date}"
        )
    else:
        log.info(
            f"[{slug}] Detecting account history start dates via API..."
        )
        (
            order_start_date,
            activity_start_date,
        ) = get_history_start_dates(rest)
        log.info(
            f"[{slug}] Dynamic Backfill Plan: Orders from "
            f"{order_start_date}, Activities from {activity_start_date}"
        )

    try:
        df = fetch_orders(rest, start_date=order_start_date)
    except Exception as exc:
        log.error(f"[{slug}] fetch_orders failed: {exc}")
        return

    if df.empty:
        log.info(f"[{slug}] No orders returned; backfill is complete.")
    else:
        records = _df_to_ingest_records(df)
        count = await storage.upsert_orders(repo, slug, records)
        log.info(f"[{slug}] Upserted {count} orders from fetch_orders.")

    log.info(
        f"[{slug}] Starting Activities Backfill from {activity_start_date}"
    )
    try:
        act_df = fetch_all_activities(
            rest, start_date=activity_start_date
        )
        if not act_df.empty:
            act_records = _df_to_activity_records(
                act_df, default_time=activity_start_date
            )
            act_count = await storage.upsert_activities(
                repo, slug, act_records
            )
            log.info(f"[{slug}] Upserted {act_count} activities.")
    except Exception as exc:
        log.error(f"[{slug}] Activities backfill failed: {exc}")


async def run_healing_worker(
    repo: AsyncAccountRepository,
    api_key: str,
    secret_key: str,
    base_url: str,
    slug: str,
) -> None:
    """
    Periodic background task.
    Polls REST API for recent history to fix any gaps ("heals" the data).
    """
    log.info(f"[{slug}] Healing worker started.")
    rest = _get_rest_client(api_key, secret_key, base_url)
    while True:
        try:
            await asyncio.sleep(IngesterConfig.HEALING_INTERVAL_SEC)
            now = datetime.now(timezone.utc)
            start_date = now - timedelta(
                seconds=IngesterConfig.HEALING_LOOKBACK_SEC
            )
            # log.debug(f"[{slug}] Running healing cycle from {start_date}")
            df = fetch_orders(rest, start_date=start_date)
            if not df.empty:
                records = _df_to_ingest_records(df)
                count = await storage.upsert_orders(repo, slug, records)
                if count > 0:
                    log.info(f"[{slug}] Healing upserted {count} orders.")

            try:
                act_df = fetch_all_activities(rest, start_date=start_date)
                if not act_df.empty:
                    act_records = _df_to_activity_records(
                        act_df, default_time=start_date
                    )
                    act_count = await storage.upsert_activities(
                        repo, slug, act_records
                    )
                    if act_count > 0:
                        log.info(
                            f"[{slug}] Healing upserted {act_count} activities."
                        )
            except Exception as exc:
                log.error(
                    f"[{slug}] Healing activities fetch failed: {exc}"
                )
        except asyncio.CancelledError:
            log.info(f"[{slug}] Healing worker cancelled.")
            break
        except Exception as exc:
            log.error(f"[{slug}] Healing worker crashed (retrying): {exc}")
            await asyncio.sleep(60)


async def run_stream_consumer(
    repo: AsyncAccountRepository,
    api_key: str,
    secret_key: str,
    base_url: str,
    slug: str,
) -> None:
    """
    Real-time WebSocket consumer.
    Listens for 'trade_updates', transforms them, and upserts to DB immediately.
    Logs payload details to assist debugging.
    """
    log.info(f"[{slug}] Stream consumer starting...")
    
    async def _on_trade_update(*args: Any):
        try:
            # support multiple handler signatures (conn, channel, data)
            raw = args[-1]
            event = raw._raw if hasattr(raw, "_raw") else raw
            
            # --- DEBUG LOGGING: RAW INCOMING ---
            # Using INFO level so you can see it without changing env vars
            log.info(f"[{slug}] >>> STREAM RAW: {_json_dumper(event)}")

            event_type = event.get("event")
            
            # 1. Handle Order Updates (status changes)
            # The event itself contains the 'order' object
            order_data = event.get("order")
            if order_data:
                try:
                    norm_order = transform.normalize_order(order_data)
                    # --- DEBUG LOGGING: NORMALIZED ORDER ---
                    log.info(f"[{slug}] NORM ORDER: {_json_dumper(norm_order)}")
                    
                    cnt = await storage.upsert_orders(repo, slug, [norm_order])
                    log.info(f"[{slug}] DB UPSERT ORDER: {cnt} rows")
                except Exception as oe:
                    log.error(f"[{slug}] Order process error: {oe}")
            
            # 2. Handle Activities (Fills)
            # 'fill' and 'partial_fill' events are essentially Trade Activities
            if event_type in ("fill", "partial_fill"):
                try:
                    norm_activity = transform.normalize_stream_activity(event)
                    # --- DEBUG LOGGING: NORMALIZED ACTIVITY ---
                    log.info(f"[{slug}] NORM ACTIVITY (ID={norm_activity.get('id')}): {_json_dumper(norm_activity)}")
                    
                    cnt = await storage.upsert_activities(repo, slug, [norm_activity])
                    if cnt == 0:
                        log.warning(f"[{slug}] DB UPSERT ACTIVITY SKIPPED (Duplicate ID?): {norm_activity.get('id')}")
                    else:
                        log.info(f"[{slug}] DB UPSERT ACTIVITY: {cnt} rows")
                except Exception as ae:
                    log.error(f"[{slug}] Activity process error: {ae}")
                
        except Exception as e:
            log.error(f"[{slug}] Stream processing fatal error: {e}")

    # Reconnection loop
    while True:
        try:
            stream = Stream(
                api_key,
                secret_key,
                base_url=base_url,
                data_feed="iex" 
            )
            
            stream.subscribe_trade_updates(_on_trade_update)
            
            log.info(f"[{slug}] Connecting to WebSocket...")
            await stream._run_forever()
            
        except asyncio.CancelledError:
            log.info(f"[{slug}] Stream consumer cancelled.")
            break
        except Exception as e:
            log.error(f"[{slug}] Stream disconnected: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)