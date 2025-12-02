"""Async workers for Data Ingester: Stream, Backfill, and Healing."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional

import pandas as pd
from alpaca_trade_api.rest import REST
from alpaca_trade_api.stream import Stream

from cold_harbour.infrastructure.db import AsyncAccountRepository
from . import storage, transform
from .config import IngesterConfig

log = logging.getLogger("IngesterWorkers")


def _get_rest_client(api_key: str, secret_key: str, base_url: str) -> REST:
    return REST(api_key, secret_key, base_url)


def _to_datetime(value: Any) -> Optional[datetime]:
    """Parse Alpaca timestamps to UTC datetimes."""
    if value is None:
        return None
    try:
        parsed = pd.to_datetime(value, utc=True, errors="coerce")
        if pd.isna(parsed):
            return None
        return parsed.to_pydatetime()
    except Exception:
        return None


def _sync_determine_backfill_start(rest: REST) -> datetime:
    """Find the oldest known activity/order to limit the backfill range."""
    try:
        first_orders = rest.list_orders(
            status="all",
            limit=1,
            direction="asc",
            nested=True,
        )
        if first_orders:
            dt = _to_datetime(getattr(first_orders[0], "created_at", None))
            if dt:
                log.info(f"Detected first order timestamp: {dt}")
                return dt

        account = rest.get_account()
        account_created = _to_datetime(getattr(account, "created_at", None))
        if account_created:
            log.info(
                f"No orders found; using account creation date {account_created}"
            )
            return account_created
    except Exception as exc:
        log.warning(f"Could not determine earliest start date: {exc}")

    fallback = pd.Timestamp.now(tz=timezone.utc) - timedelta(
        days=IngesterConfig.BACKFILL_DAYS
    )
    log.info(f"Falling back to default backfill window: {fallback}")
    return fallback.to_pydatetime()


async def _determine_backfill_start(rest: REST) -> datetime:
    """Async wrapper around the blocking Alpaca helper."""
    return await asyncio.to_thread(_sync_determine_backfill_start, rest)


async def _fetch_and_ingest_window(
    repo: AsyncAccountRepository,
    rest: REST,
    slug: str,
    start: datetime,
    end: datetime,
) -> None:
    """
    Helper: Fetch Orders and Activities for a specific time window via REST
    and upsert them into the DB.
    """
    start_iso = start.isoformat()
    end_iso = end.isoformat()
    
    # 1. Fetch Orders (nested=True to get legs)
    # Using 'after'/'until' logic similar to the robust sliding window method
    try:
        # Alpaca list_orders doesn't support 'after'/'until' strictly in all lib versions
        # but standard params are usually 'after' and 'until' for dates.
        # We fetch 'all' statuses to capture history.
        orders = rest.list_orders(
            status="all",
            limit=500,
            nested=True,
            direction="asc",  # Oldest first within window
            after=start_iso,
            until=end_iso,
        )
        
        # Normalize and Flatten
        flat_orders = []
        for o in orders:
            # Handle Alpaca object to dict
            raw = o._raw if hasattr(o, "_raw") else o
            flat_orders.append(transform.normalize_order(raw))
            
            # Handle legs if present (bracket orders)
            legs = raw.get("legs", [])
            if legs:
                for leg in legs:
                    # Leg usually doesn't have parent_id populated inside 'legs' list
                    # depending on API version, but we treat it as a standalone order update
                    flat_orders.append(transform.normalize_order(leg))

        if flat_orders:
            count = await storage.upsert_orders(repo, slug, flat_orders)
            log.debug(f"[{slug}] Ingested {count} orders from REST window {start_iso}")

    except Exception as e:
        log.error(f"[{slug}] Error fetching orders for {start_iso}: {e}")

    # 2. Fetch Activities (FILL, DIV, etc.)
    try:
        # activity_types=None fetches ALL types
        activities = rest.get_activities(
            after=start_iso,
            until=end_iso,
            direction="asc",
            page_size=100
        )
        
        norm_activities = []
        for a in activities:
            raw = a._raw if hasattr(a, "_raw") else a
            norm_activities.append(transform.normalize_rest_activity(raw))
            
        if norm_activities:
            count = await storage.upsert_activities(repo, slug, norm_activities)
            log.debug(f"[{slug}] Ingested {count} activities from REST window {start_iso}")

    except Exception as e:
        log.error(f"[{slug}] Error fetching activities for {start_iso}: {e}")


async def run_backfill_task(
    repo: AsyncAccountRepository,
    api_key: str,
    secret_key: str,
    base_url: str,
    slug: str,
) -> None:
    """
    One-off task on startup.
    Checks DB for latest data. If empty or stale, fetches history up to NOW.
    """
    log.info(f"[{slug}] Starting Backfill/Catch-up check...")
    
    # 1. Determine Start Time
    last_update = await storage.get_latest_order_time(repo, slug)
    now = datetime.now(timezone.utc)
    rest = _get_rest_client(api_key, secret_key, base_url)

    if last_update:
        # We have data, start slightly before last update to ensure overlap
        start_cursor = last_update - timedelta(minutes=10)
        log.info(f"[{slug}] Found existing data. Catching up from {start_cursor}")
    else:
        # No data, query the API for the earliest available timestamp
        start_cursor = await _determine_backfill_start(rest)
        log.info(
            f"[{slug}] Database empty. Starting full backfill from "
            f"{start_cursor}"
        )

    log.info(
        f"[{slug}] Backfill range determined: {start_cursor.isoformat()} -> "
        f"{now.isoformat()}"
    )

    # 2. Sliding Window Loop
    # We move in 5-day chunks to be safe with API limits
    window_size = timedelta(days=5)
    current_start = start_cursor

    while current_start < now:
        current_end = min(current_start + window_size, now + timedelta(minutes=1))
        
        await _fetch_and_ingest_window(repo, rest, slug, current_start, current_end)
        
        current_start = current_end
        # Slight pause to be nice to API rate limits
        await asyncio.sleep(0.2)

    log.info(f"[{slug}] Backfill complete.")


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
            lookback = timedelta(seconds=IngesterConfig.HEALING_LOOKBACK_SEC)
            start_time = now - lookback
            
            log.debug(f"[{slug}] Running healing cycle from {start_time}")
            
            # Re-fetch recent data.
            # Upsert logic in storage handles deduplication (idempotency).
            await _fetch_and_ingest_window(repo, rest, slug, start_time, now)
            
        except asyncio.CancelledError:
            log.info(f"[{slug}] Healing worker cancelled.")
            break
        except Exception as e:
            log.error(f"[{slug}] Healing worker crashed (retrying): {e}")
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
    """
    log.info(f"[{slug}] Stream consumer starting...")
    
    # Handle Paper vs Live URL for Stream
    # alpaca-trade-api Stream class usually expects the base API URL
    # and handles the conversion to wss:// internally, or we pass the specific WS URL.
    # We'll rely on the library's default behavior based on 'paper' in base_url.
    is_paper = "paper" in base_url
    
    async def _on_trade_update(*args: Any):
        try:
            # support multiple handler signatures (conn, channel, data)
            raw = args[-1]
            event = raw._raw if hasattr(raw, "_raw") else raw
            event_type = event.get("event")
            
            # 1. Handle Order Updates (status changes)
            # The event itself contains the 'order' object
            order_data = event.get("order")
            if order_data:
                norm_order = transform.normalize_order(order_data)
                await storage.upsert_orders(repo, slug, [norm_order])
            
            # 2. Handle Activities (Fills)
            # 'fill' and 'partial_fill' events are essentially Trade Activities
            if event_type in ("fill", "partial_fill"):
                norm_activity = transform.normalize_stream_activity(event)
                await storage.upsert_activities(repo, slug, [norm_activity])
                
            log.debug(f"[{slug}] Stream event processed: {event_type}")
            
        except Exception as e:
            log.error(f"[{slug}] Stream processing error: {e}")

    # Reconnection loop
    while True:
        try:
            stream = Stream(
                api_key,
                secret_key,
                base_url=base_url,
                data_feed="iex"  # Not used for trade_updates but required param
            )
            
            # Subscribe
            stream.subscribe_trade_updates(_on_trade_update)
            
            log.info(f"[{slug}] Connecting to WebSocket...")
            # _run_forever is a blocking async call
            await stream._run_forever()
            
        except asyncio.CancelledError:
            log.info(f"[{slug}] Stream consumer cancelled.")
            break
        except Exception as e:
            log.error(f"[{slug}] Stream disconnected: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)
