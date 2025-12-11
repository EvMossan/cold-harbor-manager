"""
Pure data transformation logic for the Data Ingester.
Handles normalization of Stream/REST data into DB-ready structures
and generation of idempotent IDs.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional, TypedDict
from zoneinfo import ZoneInfo

import pandas as pd

# Timezone for Alpaca ID generation (Eastern Time)
NY_TZ = ZoneInfo("America/New_York")


class OrderRecord(TypedDict, total=False):
    """Normalized schema for `raw_orders_<slug>`."""

    id: Optional[str]
    client_order_id: Optional[str]
    request_id: Optional[str]
    parent_id: Optional[str]
    replaced_by: Optional[str]
    replaces: Optional[str]
    symbol: Optional[str]
    side: Optional[str]
    order_type: Optional[str]
    status: Optional[str]
    qty: Optional[Decimal]
    filled_qty: Optional[Decimal]
    filled_avg_price: Optional[Decimal]
    limit_price: Optional[Decimal]
    stop_price: Optional[Decimal]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    submitted_at: Optional[datetime]
    filled_at: Optional[datetime]
    expired_at: Optional[datetime]
    canceled_at: Optional[datetime]
    replaced_at: Optional[datetime]
    expires_at: Optional[datetime]
    raw_json: Optional[str]
    legs: Optional[str]
    ingested_at: datetime


class ActivityRecord(TypedDict, total=False):
    """Normalized schema for `raw_activities_<slug>`."""

    id: Optional[str]
    activity_type: Optional[str]
    transaction_time: Optional[datetime]
    symbol: Optional[str]
    side: Optional[str]
    qty: Optional[Decimal]
    price: Optional[Decimal]
    net_amount: Optional[Decimal]
    execution_id: Optional[str]
    order_id: Optional[str]
    raw_json: str
    ingested_at: datetime


def _to_decimal(value: Any) -> Optional[Decimal]:
    """Convert value to Decimal, handling None and strings safely."""
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (ValueError, TypeError):
        return None


def _parse_ts(value: Any) -> Optional[datetime]:
    """Parse timestamp to UTC datetime."""
    if value is None:
        return None
    try:
        # Pandas is robust at parsing ISO strings and various formats
        dt = pd.to_datetime(value, utc=True)
        if pd.isna(dt):
            return None
        return dt.to_pydatetime()
    except Exception:
        return None


def _json_default(obj: Any) -> Any:
    """JSON serializer for non-serializable objects (datetime, Decimal)."""
    if isinstance(obj, (datetime, pd.Timestamp)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)  # or str(obj) depending on preference
    try:
        return str(obj)
    except Exception:
        return None


def _serialize_raw(data: Any) -> str:
    """Dump raw data to JSON string for storage."""
    return json.dumps(data, default=_json_default)


def _generate_synthetic_activity_id(timestamp: datetime, execution_id: str) -> str:
    """
    Reconstruct Alpaca's REST API Activity ID format from Stream data.
    
    Format: YYYYMMDDHHMMSSmmm::execution_id
    Timezone: America/New_York (ET)
    
    Args:
        timestamp: Aware datetime (UTC) from the stream event.
        execution_id: The UUID of the execution.
    """
    if not timestamp or not execution_id:
        # Fallback if critical data is missing (should not happen in valid stream)
        return f"unknown::{execution_id}"

    # Convert UTC to Eastern Time (Alpaca's server time for IDs)
    ts_ny = timestamp.astimezone(NY_TZ)
    
    # Format: 20251202094600829 (17 digits)
    # %f is microseconds (000000), we need milliseconds (000)
    # We take the first 3 digits of microseconds.
    date_part = ts_ny.strftime("%Y%m%d%H%M%S")
    millis_part = ts_ny.strftime("%f")[:3]
    
    time_str = f"{date_part}{millis_part}"
    
    return f"{time_str}::{execution_id}"


def normalize_order(raw: Dict[str, Any]) -> OrderRecord:
    """
    Normalize REST or stream orders for insertion into `raw_orders`.

    Args:
        raw: Raw API payload containing order metadata.

    Returns:
        OrderRecord where prices, quantities, and timestamps are coerced
        into Decimal/UTC datetime, `symbol` is upper-cased, `side` is
        lower-case, and `ingested_at` records the timestamp of ingestion.
    """
    data = raw

    parent_id_raw = (
        data.get("parent_order_id") or data.get("parent_id")
    )
    replaced_by_raw = data.get("replaced_by")
    replaces_raw = data.get("replaces")

    order_type_raw = (
        data.get("order_type") or data.get("type") or ""
    )

    legs_raw = data.get("legs")
    legs_json = (
        _serialize_raw(legs_raw) if legs_raw is not None else None
    )

    return {
        "id": data.get("id"),
        "client_order_id": data.get("client_order_id"),
        "request_id": data.get("request_id"),
        "parent_id": str(parent_id_raw) if parent_id_raw else None,
        "replaced_by": str(replaced_by_raw) if replaced_by_raw else None,
        "replaces": str(replaces_raw) if replaces_raw else None,
        "symbol": str(data.get("symbol") or "").upper(),
        "side": str(data.get("side") or "").lower(),
        "order_type": str(order_type_raw).lower(),
        "status": str(data.get("status") or "").lower(),

        "qty": _to_decimal(data.get("qty")),
        "filled_qty": _to_decimal(data.get("filled_qty") or 0),
        "filled_avg_price": _to_decimal(data.get("filled_avg_price")),
        "limit_price": _to_decimal(data.get("limit_price")),
        "stop_price": _to_decimal(data.get("stop_price")),

        "created_at": _parse_ts(data.get("created_at")),
        "updated_at": _parse_ts(data.get("updated_at")),
        "submitted_at": _parse_ts(data.get("submitted_at")),
        "filled_at": _parse_ts(data.get("filled_at")),
        "expired_at": _parse_ts(data.get("expired_at")),
        "canceled_at": _parse_ts(data.get("canceled_at")),
        "replaced_at": _parse_ts(data.get("replaced_at")),
        "expires_at": _parse_ts(data.get("expires_at")),

        "raw_json": _serialize_raw(data),
        "legs": legs_json,

        "ingested_at": datetime.now(timezone.utc),
    }


def normalize_rest_activity(raw: Dict[str, Any]) -> ActivityRecord:
    """
    Normalize REST activity rows for idempotent inserts.

    Args:
        raw: Response from `GET /v2/account/activities`.

    Returns:
        ActivityRecord with parsed amounts, UTC timestamps, and extracted
        execution IDs when the REST ID embeds them.
    """
    # REST API provides the ID natively
    activity_id = raw.get("id")
    
    # Extract execution_id from the ID string if possible (format: time::uuid)
    execution_id = None
    if activity_id and "::" in activity_id:
        parts = activity_id.split("::")
        if len(parts) == 2:
            execution_id = parts[1]

    return {
        "id": activity_id,
        "activity_type": str(raw.get("activity_type") or "").upper(),
        "transaction_time": _parse_ts(raw.get("transaction_time") or raw.get("date")),
        
        "symbol": str(raw.get("symbol") or "").upper() if raw.get("symbol") else None,
        "side": str(raw.get("side") or "").lower() if raw.get("side") else None,
        
        "qty": _to_decimal(raw.get("qty")),
        "price": _to_decimal(raw.get("price")),
        "net_amount": _to_decimal(raw.get("net_amount")),
        
        "execution_id": execution_id,
        "order_id": raw.get("order_id"),
        
        "raw_json": _serialize_raw(raw),
        "ingested_at": datetime.now(timezone.utc),
    }


def normalize_stream_activity(event: Dict[str, Any]) -> ActivityRecord:
    """
    Normalize a WebSocket `trade_updates` event for ingestion.

    Args:
        event: Alpaca WebSocket payload describing a fill or partial.

    Returns:
        ActivityRecord with synthetic `id` (YYYYMMDDHHMMSSmmm::exec_id),
        UTC-normalized `transaction_time`, Decimal amounts, and signed
        `net_amount` where buys are negative.
    """
    # Event structure: {"event": "fill", "execution_id": "...", "order": {...}, ...}
    event_type = event.get("event")
    execution_id = event.get("execution_id")
    timestamp = _parse_ts(event.get("timestamp"))
    
    # Order details are nested
    order = event.get("order", {})
    
    # Generate the synthetic ID
    if timestamp and execution_id:
        synthetic_id = _generate_synthetic_activity_id(timestamp, execution_id)
    else:
        # Should not happen for valid fills, but safe fallback
        synthetic_id = f"stream_{execution_id or datetime.now().timestamp()}"

    # Calculate net_amount approximation for FILL (cash impact)
    # Logic: buy = negative cash, sell = positive cash
    qty = _to_decimal(event.get("qty")) or Decimal(0)
    price = _to_decimal(event.get("price")) or Decimal(0)
    side = str(order.get("side") or "").lower()
    
    net_amount = qty * price
    if side == "buy":
        net_amount = -net_amount
        
    return {
        "id": synthetic_id,
        "activity_type": "FILL",  # Stream trade_updates are fills
        "transaction_time": timestamp,
        
        "symbol": str(order.get("symbol") or "").upper(),
        "side": side,
        
        "qty": qty,
        "price": price,
        "net_amount": net_amount,
        
        "execution_id": execution_id,
        "order_id": order.get("id"),
        
        "raw_json": _serialize_raw(event),
        "ingested_at": datetime.now(timezone.utc),
    }
