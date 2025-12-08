from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Protocol, Sequence, List, Set
import ast

from alpaca_trade_api.rest import REST

import numpy as np
import pandas as pd
from pandas import Timestamp
import logging
import asyncpg
import json

log = logging.getLogger("IngesterRuntime")

class AlpacaOrdersAPI(Protocol):
    """Minimal interface for Alpaca clients used in this module."""

    def list_orders(
        self,
        *,
        status: str,
        limit: int,
        nested: bool,
        after: str,
        until: str,
        direction: str,
    ) -> Sequence[Any]:
        """Return a batch of orders matching the given window."""


class AlpacaPositionsAPI(Protocol):
    """Minimal interface for Alpaca clients that expose positions."""

    def list_positions(self) -> Sequence[Any]:
        """Return current positions (qty, avg_entry_price, etc.)."""


def _normalize_timestamp(
    value: Optional[datetime], *, default: datetime
) -> datetime:
    if value is None:
        return default
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize(timezone.utc)
    else:
        ts = ts.astimezone(timezone.utc)
    return ts


def fetch_orders(
    api: AlpacaOrdersAPI,
    days_back: int = 365,
    *,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> pd.DataFrame:
    """
    Download orders in fixed windows to avoid cursor pagination issues.

    Args:
        api: Alpaca-compatible client exposing `list_orders`.
        days_back: Default days to look back when no start is provided.
        start_date: Optional earliest timestamp to include (UTC).
        end_date: Optional exclusive upper bound (UTC).

    Returns:
        DataFrame with flattened parent/leg rows including `id`,
        `parent_id`, `symbol`, `side`, `qty`, `order_type`, `status`,
        `created_at`, `updated_at`, `filled_at`, `limit_price`, and
        `stop_price`.

    Raises:
        Exception: Propagates Alpaca API failures from a window.
    """
    # print(f">>> ROBUST ORDER DOWNLOAD (last {days_back} days)...")

    # 1. Define time boundaries
    # Since the simulation is in the future (2025), take 'now' from
    # the API or system. For reliability, set the end date to tomorrow.
    now = pd.Timestamp.now(tz=timezone.utc)
    target_end = _normalize_timestamp(
        end_date, default=now + timedelta(days=1)
    )
    target_start = _normalize_timestamp(
        start_date, default=target_end - timedelta(days=days_back)
    )
    if target_start >= target_end:
        target_start = target_end - timedelta(days=days_back)

    actual_days = max(1, (target_end - target_start).days)
    # print(
    #     f">>> ROBUST ORDER DOWNLOAD ({target_start.date()} -> "
    #     f"{target_end.date()}, ~{actual_days} days)..."
    # )
    all_raw_orders = []
    seen_ids = set()
    window_size = timedelta(days=5)
    current_start = target_start

    while current_start < target_end:
        current_end = min(
            current_start + window_size, target_end
        )
        t_start = current_start.isoformat()
        t_end = current_end.isoformat()
        try:
            batch = api.list_orders(
                status="all",
                limit=500,
                nested=True,
                after=t_start,
                until=t_end,
                direction="desc",
            )
        except Exception as exc:
            print(f"   [Error] In window {t_start}: {exc}")
            raise exc
        if batch:
            for order_obj in batch:
                root = getattr(order_obj, "_raw", order_obj)
                if root["id"] in seen_ids:
                    continue
                seen_ids.add(root["id"])
                all_raw_orders.append(order_obj)
        current_start = current_end

    # --- UNPACKING AND CLEANING ---
    if not all_raw_orders:
        return pd.DataFrame()

    flat_rows = []
    seen_ids = set()

    for order_obj in all_raw_orders:
        # Handle Alpaca object vs dict
        root = getattr(order_obj, '_raw', order_obj) 
        
        # Deduplicate (Window overlap protection)
        if root['id'] in seen_ids:
            continue
        seen_ids.add(root['id'])

        # 1. Process Parent
        parent_row = root.copy()
        
        # DON'T POP! Just get the data. 
        # We want 'legs' to remain in parent_row as a backup source of truth.
        legs_data = parent_row.get('legs', None) 
        
        # Ensure parent_id is None for the root (unless it really has one)
        if 'parent_id' not in parent_row:
            parent_row['parent_id'] = None
            
        flat_rows.append(parent_row)

        # 2. Process Children (Flattening)
        if legs_data:
            for leg in legs_data:
                leg_row = leg.copy()
                # Explicitly link child to parent
                leg_row['parent_id'] = root['id'] 
                
                # Add child as its own row
                flat_rows.append(leg_row) 
                
                # Add child ID to seen to prevent duplicates if logic runs twice
                seen_ids.add(leg['id']) 

    df = pd.DataFrame(flat_rows)

    # Type conversion
    date_cols = [
        'created_at',
        'updated_at',
        'filled_at',
        'submitted_at',
        'replaced_at',
    ]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors='coerce')

    num_cols = ['qty', 'filled_qty', 'limit_price', 'stop_price']
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Sorting for aesthetics
    if 'created_at' in df.columns:
        df = df.sort_values('created_at', ascending=False)

    log.debug(f"SUCCESS. Final table: {len(df)} rows. ")
    return df


async def load_orders_from_db(
    conn_str: str,
    slug: str = "live",
    days_back: int = 365
) -> pd.DataFrame:
    """
    Adapted function to load orders from DB directly in notebook.
    Fixes UUID vs String type mismatch for compatibility with portfolio logic.
    """
    if not conn_str:
        raise ValueError("Connection string is empty!")
    
    # Secure table name formatting
    safe_slug = slug.replace("-", "_").lower()
    table_name = f"account_activities.raw_orders_{safe_slug}"
    
    # Cutoff date (UTC)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    print(f"🔌 Connecting to DB to fetch orders for '{slug}' "
          f"from table '{table_name}'...")
    
    # SQL Query
    sql = f"""
        SELECT
            id, client_order_id, parent_id, symbol, side, order_type, status,
            qty, filled_qty, filled_avg_price, limit_price, stop_price,
            created_at, updated_at, submitted_at, filled_at,
            expired_at, canceled_at, replaced_by, replaces,
            legs, raw_json
        FROM {table_name}
        WHERE created_at >= $1
        ORDER BY created_at DESC
    """
    
    try:
        conn = await asyncpg.connect(conn_str)
        try:
            rows = await conn.fetch(sql, cutoff)
        finally:
            await conn.close()
            
    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()

    if not rows:
        print("No rows returned.")
        return pd.DataFrame()

    print(f"Loaded {len(rows)} rows. Processing DataFrame...")
    
    # Convert asyncpg Records to list of dicts
    data = [dict(row) for row in rows]
    df = pd.DataFrame(data)
    
    # --- Type Conversion ---
    
    # 1. Dates to UTC
    date_cols = [
        "created_at", "updated_at", "submitted_at", 
        "filled_at", "expired_at", "canceled_at"
    ]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True)
            
    # 2. Numbers (Decimal to float)
    num_cols = [
        "qty", "filled_qty", "filled_avg_price", 
        "limit_price", "stop_price"
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 3. UUIDs to Strings (CRITICAL FIX)
    # asyncpg returns uuid.UUID objects, but portfolio logic expects strings.
    # We explicitly cast them, preserving None values as None (not "None").
    uuid_cols = ["id", "parent_id", "client_order_id"]
    for col in uuid_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: str(x) if x else None)

    return df


def get_history_start_dates(api: REST) -> tuple[datetime, datetime]:
    """
    Determines the earliest timestamps for orders and activities via API.
    """
    fallback_date = (
        pd.Timestamp.now(tz=timezone.utc).to_pydatetime()
        - timedelta(days=365)
    )

    try:
        acct = api.get_account()
        created_at_raw = getattr(acct, "created_at", None)
        account_created = (
            pd.to_datetime(created_at_raw, utc=True).to_pydatetime()
            if created_at_raw
            else fallback_date
        )
    except Exception as exc:
        log.warning("Could not detect account creation date: %s", exc)
        account_created = fallback_date

    earliest_order = account_created
    earliest_activity = account_created

    try:
        orders = api.list_orders(status="all", limit=1, direction="asc")
        if orders:
            raw_o = getattr(orders[0], "_raw", {})
            ts_str = (
                raw_o.get("submitted_at")
                or raw_o.get("created_at")
                or raw_o.get("updated_at")
            )
            if ts_str:
                earliest_order = pd.to_datetime(
                    ts_str, utc=True
                ).to_pydatetime()
    except Exception as exc:
        log.warning("Could not fetch earliest order: %s", exc)

    try:
        acts = api.get_activities(page_size=1, direction="asc")
        if acts:
            raw_a = getattr(acts[0], "_raw", {})
            ts_str = (
                raw_a.get("transaction_time")
                or raw_a.get("activity_time")
                or raw_a.get("date")
            )
            if ts_str:
                earliest_activity = pd.to_datetime(
                    ts_str, utc=True
                ).to_pydatetime()
    except Exception as exc:
        log.warning("Could not fetch earliest activity: %s", exc)

    return (
        earliest_order - timedelta(days=1),
        earliest_activity - timedelta(days=1),
    )


def fetch_all_activities(api, start_date=None, end_date=None):
    """
    Fetches account activity history (FILL, FEE, DIV, JNLS, etc.) 
    and normalizes the resulting DataFrame.

    This function coalesces multiple potential timestamp fields into 
    a single 'exec_time' column, ensuring that non-trade activities 
    (like deposits and fees) are correctly dated even when 
    'transaction_time' is missing.

    Args:
        api: Alpaca-compatible client.
        start_date (datetime, optional): Earliest date to fetch (inclusive).
        end_date (datetime, optional): Latest date to fetch (exclusive).

    Returns:
        pd.DataFrame: Normalized activity history.
    """
    def _to_iso(value):
        if value is None:
            return None
        ts = pd.Timestamp(value)
        if ts.tzinfo is None:
            ts = ts.tz_localize(timezone.utc)
        else:
            ts = ts.astimezone(timezone.utc)
        return ts.isoformat()

    after_iso = _to_iso(start_date)
    until_iso = _to_iso(end_date)

    all_activities = []
    last_id = None

    while True:
        params = {
            "direction": "desc",
            "page_size": 100
        }
        if after_iso:
            params["after"] = after_iso
        if until_iso:
            params["until"] = until_iso
        if last_id:
            params["page_token"] = last_id

        try:
            activities = api.get_activities(**params)
        except Exception as e:
            # log.error should be configured in your analytics logger
            raise e

        if not activities:
            break

        all_activities.extend(activities)
        last_id = getattr(activities[-1], 'id', None)
        
        if len(activities) < params["page_size"]:
            break

    if not all_activities:
        return pd.DataFrame()

    data = [x._raw if hasattr(x, '_raw') else x for x in all_activities]
    df = pd.DataFrame(data)

    # --- COALESCE STRATEGY FOR TIMESTAMPS ---
    # We iterate through priority columns to fill gaps in exec_time.
    # FILL rows usually have transaction_time. 
    # CSD/FEE rows usually have date.
    possible_time_cols = [
        "transaction_time",
        "activity_time",
        "date",
        "timestamp",
        "created_at",
    ]

    df["exec_time"] = pd.NaT

    for col in possible_time_cols:
        if col in df.columns:
            # Fill missing values from the current column
            current_col_dates = pd.to_datetime(
                df[col], utc=True, errors="coerce"
            )
            df["exec_time"] = df["exec_time"].fillna(current_col_dates)

    df["exec_time"] = df["exec_time"].infer_objects(copy=False)
    # --- DATA NORMALIZATION ---
    # Ensure net_amount is numeric as it powers Equity and Metrics flows.
    numeric_cols = [
        "qty",
        "price",
        "cum_qty",
        "leaves_qty",
        "net_amount",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # String sanitization
    if "side" in df.columns:
        df["side"] = df["side"].str.lower()
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).replace("nan", "")
    if "activity_type" in df.columns:
        df["activity_type"] = df["activity_type"].str.upper()

    return df


def build_lot_portfolio(
    fills_df: pd.DataFrame,
    orders_df: pd.DataFrame,
    api: Any = None,
    debug: bool = False,
    rescue_orphans: bool = False,
) -> pd.DataFrame:
    """
    Builds a portfolio using deep leg extraction.

    Args:
        fills_df: DataFrame containing fill/execution data.
        orders_df: DataFrame containing raw order data.
        api: Alpaca API instance for fetching current prices.
        debug: If True, prints detailed logic logs.
        rescue_orphans: If True, enables linking Stop/TP orders based on
            quantity if the Broker ID chain is broken.

    Returns:
        pd.DataFrame: A DataFrame containing active portfolio lots.
    """
    pos_map: Dict[str, Dict[str, float]] = {}
    if api:
        try:
            raw_positions = api.list_positions()
            for p in raw_positions:
                q = float(p.qty)
                if abs(q) < 1e-9:
                    continue

                cur_px = float(getattr(p, "current_price", 0.0))
                if cur_px == 0.0 and q != 0:
                    try:
                        cur_px = float(p.market_value) / q
                    except Exception:
                        cur_px = 0.0

                pos_map[p.symbol.lower()] = {
                    "qty": q,
                    "avg_px": float(p.avg_entry_price),
                    "cur_px": cur_px,
                }
        except Exception:
            pass

    empty_cols = [
        "Parent ID",
        "Symbol",
        "Buy Date",
        "Buy Qty",
        "Remaining Qty",
        "Buy Price",
        "Take_Profit_Price",
        "Stop_Loss_Price",
        "Source",
        "Holding_Period",
        "Current_Price",
        "Current Market Value",
        "Profit/Loss",
        "TP_reach, %",
        "BrE",
    ]

    if fills_df.empty:
        return pd.DataFrame(columns=empty_cols)

    time_col = "exec_time"
    if time_col not in fills_df.columns:
        for alt in ["transaction_time", "timestamp", "created_at"]:
            if alt in fills_df.columns:
                fills_df[time_col] = pd.to_datetime(
                    fills_df[alt], utc=True
                )
                break

    if time_col not in fills_df.columns:
        return pd.DataFrame(columns=empty_cols)

    fills_df = fills_df.sort_values(time_col)

    orders_map: Dict[str, Dict] = {}
    children_map: Dict[str, List[str]] = {}
    chain_map: Dict[str, str] = {}
    orphan_candidates: Dict[str, List[str]] = {}

    def get_val(obj, key, default=None):
        if isinstance(obj, dict):
            return obj.get(key, default)
        if hasattr(obj, key):
            return getattr(obj, key, default)
        return default

    if not orders_df.empty:
        if "updated_at" in orders_df.columns:
            orders_df = orders_df.sort_values(
                "updated_at", ascending=True
            )

        num_cols = [
            "limit_price",
            "stop_price",
            "leaves_qty",
            "qty",
            "filled_qty",
        ]
        for c in num_cols:
            if c in orders_df.columns:
                orders_df[c] = (
                    pd.to_numeric(orders_df[c], errors="coerce")
                    .fillna(0.0)
                )

        for _, row in orders_df.iterrows():
            r_dict = row.to_dict()
            oid = str(r_dict.get("id"))
            orders_map[oid] = r_dict

            sym = str(r_dict.get("symbol")).upper()
            side = str(r_dict.get("side")).lower()

            pid = str(r_dict.get("parent_id"))
            if pid and pid.lower() not in ["none", "nan", ""]:
                if pid not in children_map:
                    children_map[pid] = []
                if oid not in children_map[pid]:
                    children_map[pid].append(oid)

            rep_by = str(r_dict.get("replaced_by"))
            if rep_by and rep_by.lower() not in ["none", "nan", ""]:
                chain_map[oid] = rep_by

            if rescue_orphans and side == "sell":
                if sym not in orphan_candidates:
                    orphan_candidates[sym] = []
                orphan_candidates[sym].append(oid)

            legs_raw = r_dict.get("legs")
            legs_to_process: List[Any] = []

            if isinstance(legs_raw, list):
                legs_to_process = legs_raw
            elif isinstance(legs_raw, str) and len(legs_raw) > 2:
                try:
                    legs_to_process = ast.literal_eval(legs_raw)
                except Exception:
                    try:
                        legs_to_process = json.loads(
                            legs_raw.replace("'", '"')
                            .replace("None", "null")
                            .replace("True", "true")
                            .replace("False", "false")
                        )
                    except Exception:
                        pass

            if legs_to_process:
                if oid not in children_map:
                    children_map[oid] = []

                for leg in legs_to_process:
                    l_id = str(get_val(leg, "id"))
                    leg_record = {
                        "id": l_id,
                        "parent_id": oid,
                        "symbol": str(get_val(leg, "symbol") or sym),
                        "side": str(get_val(leg, "side")),
                        "order_type": str(
                            get_val(leg, "order_type")
                            or get_val(leg, "type")
                        ),
                        "status": str(get_val(leg, "status")),
                        "limit_price": float(
                            get_val(leg, "limit_price") or 0.0
                        ),
                        "stop_price": float(
                            get_val(leg, "stop_price") or 0.0
                        ),
                        "qty": float(get_val(leg, "qty") or 0.0),
                        "leaves_qty": float(
                            get_val(leg, "leaves_qty") or 0.0
                        ),
                        "filled_qty": float(
                            get_val(leg, "filled_qty") or 0.0
                        ),
                        "replaced_by": str(get_val(leg, "replaced_by")),
                    }

                    orders_map[l_id] = leg_record
                    if l_id not in children_map[oid]:
                        children_map[oid].append(l_id)

                    l_rep = leg_record["replaced_by"]
                    if l_rep and l_rep.lower() not in ["none", "nan", ""]:
                        chain_map[l_id] = l_rep

                    if rescue_orphans and leg_record["side"] == "sell":
                        if sym not in orphan_candidates:
                            orphan_candidates[sym] = []
                        if l_id not in orphan_candidates[sym]:
                            orphan_candidates[sym].append(l_id)

    used_orders: Set[str] = set()

    def get_root_parent(oid: str) -> str:
        curr = oid
        visited: Set[str] = set()
        while curr and curr not in visited:
            visited.add(curr)
            if curr not in orders_map:
                break
            pid = str(orders_map[curr].get("parent_id"))
            if pid and pid.lower() not in ["none", "nan", ""]:
                curr = pid
            else:
                return curr
        return curr

    def get_latest_version(oid: str) -> str:
        curr = oid
        visited: Set[str] = set()
        while curr in chain_map and curr not in visited:
            visited.add(curr)
            curr = chain_map[curr]
        return curr

    def get_all_descendants(root_id: str) -> Set[str]:
        family: Set[str] = set()
        queue: List[str] = [root_id]
        while queue:
            curr = queue.pop(0)
            if curr in family:
                continue
            family.add(curr)
            if curr in children_map:
                queue.extend(children_map[curr])
        return family

    def get_order_priority(order: Dict) -> int:
        status = str(order.get("status", "")).lower()
        leaves = float(order.get("leaves_qty", 0.0))

        if status in [
            "new",
            "open",
            "held",
            "accepted",
            "partially_filled",
        ]:
            return 3
        if (
            leaves > 0
            and status
            not in [
                "canceled",
                "rejected",
                "expired",
                "filled",
                "done_for_day",
            ]
        ):
            return 3

        if status == "filled":
            return 2

        if status in ["canceled", "expired", "done_for_day"]:
            return 1

        return 0

    lots: List[Dict[str, Any]] = []
    fifo_queues: Dict[str, List[int]] = {}

    for _, row in fills_df.iterrows():
        side = str(row.get("side")).lower()
        symbol = str(row.get("symbol"))
        qty = float(row.get("qty", 0))
        price = float(row.get("price", 0))
        exec_time = row.get(time_col)
        fill_oid = str(row.get("order_id"))

        if side == "buy":
            lot_data: Dict[str, Any] = {
                "Parent ID": fill_oid,
                "Symbol": symbol,
                "Buy Date": exec_time,
                "Buy Qty": qty,
                "Remaining Qty": qty,
                "Buy Price": price,
                "Original Qty": qty,
                "Source": "Matched",
                "Take_Profit_Price": None,
                "Stop_Loss_Price": None,
            }
            lots.append(lot_data)

            if symbol not in fifo_queues:
                fifo_queues[symbol] = []
            fifo_queues[symbol].append(len(lots) - 1)

            root_id = get_root_parent(fill_oid)
            family_ids = get_all_descendants(root_id)

            candidates: List[Dict[str, Any]] = []
            for mid in family_ids:
                lid = get_latest_version(mid)
                if lid not in orders_map:
                    continue

                o = orders_map[lid]
                if str(o.get("side")).lower() != "sell":
                    continue

                prio = get_order_priority(o)
                c_data = {
                    "id": lid,
                    "type": str(
                        o.get("order_type", o.get("type", "unknown"))
                    ).lower(),
                    "limit": float(o.get("limit_price", 0.0)),
                    "stop": float(o.get("stop_price", 0.0)),
                    "status": o.get("status"),
                    "priority": prio,
                }
                candidates.append(c_data)

            candidates.sort(
                key=lambda x: x["priority"],
                reverse=True,
            )

            for c in candidates:
                if c["priority"] == 0:
                    continue

                if c["priority"] == 3:
                    suffix = " (Active)"
                else:
                    suffix = f" ({c['status']})"

                if (
                    c["type"] == "limit"
                    and not lot_data["Take_Profit_Price"]
                ):
                    if c["limit"] > 0:
                        lot_data["Take_Profit_Price"] = c["limit"]
                        lot_data["Source"] = "Chain" + suffix
                        used_orders.add(c["id"])

                elif c["type"] in ["stop", "stop_limit", "trailing_stop"]:
                    if not lot_data["Stop_Loss_Price"]:
                        px = c["stop"] if c["stop"] > 0 else c["limit"]
                        if px > 0:
                            lot_data["Stop_Loss_Price"] = px
                            lot_data["Source"] = "Chain" + suffix
                            used_orders.add(c["id"])

            if (
                rescue_orphans
                and (
                    not lot_data["Stop_Loss_Price"]
                    or not lot_data["Take_Profit_Price"]
                )
                and symbol.upper() in orphan_candidates
            ):
                potential_orphans: List[Dict[str, Any]] = []
                for oid in orphan_candidates[symbol.upper()]:
                    if oid in used_orders:
                        continue

                    lid = get_latest_version(oid)
                    if lid in used_orders:
                        continue

                    o = orders_map.get(lid)
                    if not o:
                        continue

                    o_qty = float(o.get("qty", 0))
                    if not (
                        np.isclose(
                            o_qty, lot_data["Remaining Qty"], atol=0.01
                        )
                        or np.isclose(
                            o_qty, lot_data["Original Qty"], atol=0.01
                        )
                    ):
                        continue

                    prio = get_order_priority(o)
                    if prio > 0:
                        potential_orphans.append(
                            {
                                "id": lid,
                                "type": str(
                                    o.get("order_type", "")
                                ).lower(),
                                "limit": float(o.get("limit_price", 0)),
                                "stop": float(o.get("stop_price", 0)),
                                "status": o.get("status"),
                                "priority": prio,
                            }
                        )

                potential_orphans.sort(
                    key=lambda x: x["priority"],
                    reverse=True,
                )

                for c in potential_orphans:
                    if c["priority"] == 3:
                        suffix = " (Active)"
                    else:
                        suffix = f" ({c['status']})"

                    if (
                        c["type"] == "limit"
                        and not lot_data["Take_Profit_Price"]
                    ):
                        lot_data["Take_Profit_Price"] = c["limit"]
                        lot_data["Source"] = "Orphan" + suffix
                        used_orders.add(c["id"])
                    elif (
                        c["type"]
                        in ["stop", "stop_limit", "trailing_stop"]
                        and not lot_data["Stop_Loss_Price"]
                    ):
                        px = c["stop"] if c["stop"] > 0 else c["limit"]
                        lot_data["Stop_Loss_Price"] = px
                        lot_data["Source"] = "Orphan" + suffix
                        used_orders.add(c["id"])

        elif side == "sell":
            q_sell = qty
            if symbol in fifo_queues:
                for lot_idx in list(fifo_queues[symbol]):
                    if q_sell <= 1e-9:
                        break

                    tgt = lots[lot_idx]
                    if tgt["Remaining Qty"] <= 1e-9:
                        continue

                    deduct = min(tgt["Remaining Qty"], q_sell)
                    tgt["Remaining Qty"] -= deduct
                    q_sell -= deduct

    if rescue_orphans:
        unprotected_lots: Dict[str, List[Dict[str, Any]]] = {}
        for lot in lots:
            if not lot["Stop_Loss_Price"] or not lot["Take_Profit_Price"]:
                sym = lot["Symbol"].upper()
                if sym not in unprotected_lots:
                    unprotected_lots[sym] = []
                unprotected_lots[sym].append(lot)

        for sym, lot_list in unprotected_lots.items():
            if sym not in orphan_candidates:
                continue

            no_sl_lots = [
                l for l in lot_list if not l["Stop_Loss_Price"]
            ]
            if no_sl_lots:
                total_req = sum(l["Remaining Qty"] for l in no_sl_lots)
                for oid in orphan_candidates[sym]:
                    if oid in used_orders:
                        continue

                    o = orders_map.get(get_latest_version(oid))
                    if not o:
                        continue

                    if (
                        np.isclose(float(o["qty"]), total_req, atol=1.0)
                        and get_order_priority(o) > 0
                    ):
                        used_orders.add(str(o["id"]))
                        o_type = str(o.get("order_type", "")).lower()
                        if o_type in [
                            "stop",
                            "stop_limit",
                            "trailing_stop",
                        ]:
                            px = float(o.get("stop_price", 0)) or float(
                                o.get("limit_price", 0)
                            )
                            for lot in no_sl_lots:
                                lot["Stop_Loss_Price"] = px
                                lot["Source"] = "Orphan (Agg)"
                        break

            no_tp_lots = [
                l for l in lot_list if not l["Take_Profit_Price"]
            ]
            if no_tp_lots:
                total_req = sum(l["Remaining Qty"] for l in no_tp_lots)
                for oid in orphan_candidates[sym]:
                    if oid in used_orders:
                        continue

                    o = orders_map.get(get_latest_version(oid))
                    if not o:
                        continue

                    if (
                        np.isclose(float(o["qty"]), total_req, atol=1.0)
                        and get_order_priority(o) > 0
                    ):
                        used_orders.add(str(o["id"]))
                        o_type = str(o.get("order_type", "")).lower()
                        if o_type == "limit":
                            px = float(o.get("limit_price", 0))
                            for lot in no_tp_lots:
                                lot["Take_Profit_Price"] = px
                                lot["Source"] = "Orphan (Agg)"
                        break

    active_lots = [l for l in lots if l["Remaining Qty"] > 1e-6]
    df = pd.DataFrame(active_lots)

    if df.empty:
        return pd.DataFrame(columns=empty_cols)

    now = pd.Timestamp.now(tz=Timestamp.utcnow().tz)

    def get_market_px(row):
        return pos_map.get(row["Symbol"].lower(), {}).get(
            "cur_px", np.nan
        )

    df["Current_Price"] = df.apply(get_market_px, axis=1)
    df["Buy Date Obj"] = pd.to_datetime(df["Buy Date"])
    df["Holding_Period"] = (now - df["Buy Date Obj"]).dt.days

    df["Current Market Value"] = (
        df["Remaining Qty"] * df["Current_Price"]
    )
    df["Profit/Loss"] = (
        df["Current_Price"] - df["Buy Price"]
    ) * df["Remaining Qty"]

    diff = df["Current_Price"] - df["Buy Price"]
    tp_dist = df["Take_Profit_Price"] - df["Buy Price"]
    sl_dist = df["Buy Price"] - df["Stop_Loss_Price"]

    df["TP_reach, %"] = np.nan
    mask_profit = (diff >= 0) & (tp_dist > 0)
    df.loc[mask_profit, "TP_reach, %"] = (
        diff / tp_dist * 100
    ).round(2)

    mask_loss = (diff < 0) & (sl_dist > 0)
    df.loc[mask_loss, "TP_reach, %"] = (
        diff / sl_dist * 100
    ).round(2)

    df["BrE"] = df.apply(
        lambda r: 1
        if (
            pd.notna(r["Stop_Loss_Price"])
            and r["Buy Price"] != 0
            and abs(r["Stop_Loss_Price"] - r["Buy Price"])
            < (0.01 * r["Buy Price"])
        )
        else 0,
        axis=1,
    )

    df["Buy Date"] = df["Buy Date Obj"].dt.strftime("%Y-%m-%d %H:%M:%S")

    final_cols = [c for c in empty_cols if c in df.columns]

    return (
        df[final_cols]
        .sort_values(["Symbol", "Buy Date"], ascending=[True, False])
        .reset_index(drop=True)
    )


def build_closed_trades_df_lot(
    fills_df: pd.DataFrame,
    orders_df: Optional[pd.DataFrame] = None
) -> pd.DataFrame:
    """
    Builds CLOSED trades with STRICT priority on Parent-Child linkage.
    Includes robust FIFO PnL comparison using Per-Share normalization.
    
    Fixes:
    - DeprecationWarning regarding GroupBy.apply.
    - Performance optimization using vectorized .agg() instead of .apply().
    """

    # === 1. HELPER: COMPRESS FILLS (OPTIMIZED) ===
    def _compress_fills(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or 'order_id' not in df.columns:
            return df
        
        # Work on a copy to avoid SettingWithCopy warnings on the input df
        local_df = df.copy()
        
        # FIX: Ensure order_id is strictly string
        local_df['order_id'] = local_df['order_id'].astype(str)
        
        # Pre-calculate Total Value for Weighted Average Price
        # (Math: Sum(Price * Qty) / Sum(Qty))
        local_df['_total_val'] = local_df['price'] * local_df['qty']
        
        # Use .agg() instead of .apply() -> This fixes the DeprecationWarning and is much faster
        grouped = local_df.groupby(['order_id', 'side', 'symbol'], as_index=False).agg(
            qty=('qty', 'sum'),
            _total_val=('_total_val', 'sum'),
            exec_time_min=('exec_time', 'min'),
            exec_time_max=('exec_time', 'max')
        )
        
        # Calculate Weighted Average Price
        # Use np.where to handle division by zero safely
        grouped['price'] = np.where(
            grouped['qty'] > 0,
            grouped['_total_val'] / grouped['qty'],
            0.0
        )
        
        # Clean up temporary column
        return grouped.drop(columns=['_total_val'])

    # === 2. DATA PREPARATION ===
    if fills_df.empty:
        return pd.DataFrame()

    df_fills = fills_df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df_fills['exec_time']):
        df_fills['exec_time'] = pd.to_datetime(df_fills['exec_time'], utc=True)
        
    # FIX: Ensure global IDs are strings
    if 'order_id' in df_fills.columns:
        df_fills['order_id'] = df_fills['order_id'].astype(str)

    # A. COMPRESS FOR LOT LOGIC
    df_metrics = _compress_fills(df_fills)

    # Meta Map
    meta_map = {}
    if orders_df is not None and not orders_df.empty:
        temp_o = orders_df.sort_values('updated_at', ascending=False).drop_duplicates('id')
        for _, row in temp_o.iterrows():
            meta_map[str(row['id'])] = {
                'parent_id': str(row['parent_id']) if pd.notna(row.get('parent_id')) else None,
                'type': row.get('type') or row.get('order_type') or 'unknown'
            }

    # === 3. INVENTORY & MATCHING ===
    buys_dict = {} 
    sells_list = []

    df_metrics = df_metrics.sort_values('exec_time_min')

    for _, row in df_metrics.iterrows():
        oid = str(row['order_id'])
        side = str(row['side']).lower()
        qty = float(row['qty'])
        
        meta = meta_map.get(oid, {})

        if side == 'buy':
            buys_dict[oid] = {
                'id': oid, 'symbol': str(row['symbol']),
                'qty': qty, 'remaining': qty,
                'price': float(row['price']), 'time': row['exec_time_min']
            }
        elif side == 'sell':
            sells_list.append({
                'id': oid, 'symbol': str(row['symbol']),
                'qty': qty, 'remaining': qty,
                'price': float(row['price']), 'time': row['exec_time_max'],
                'parent_id': meta.get('parent_id'), 'type': meta.get('type', 'unknown')
            })

    closed_trades = []

    # Helper to record trades
    def _add_trade_record(results, buy, sell, qty):
        pnl = (sell['price'] - buy['price']) * qty
        pnl_pct = ((sell['price'] - buy['price']) / buy['price']) * 100 if buy['price'] else 0
        duration = (sell['time'] - buy['time']).total_seconds()
        
        results.append({
            "entry_lot_id": buy['id'],
            "exit_order_id": sell['id'],
            "exit_parent_id": sell.get('parent_id'),
            "symbol": sell['symbol'],
            "side": "Long",
            "qty": qty,
            "entry_time": buy['time'],
            "entry_price": buy['price'],
            "exit_time": sell['time'],
            "exit_price": sell['price'],
            "exit_type": sell.get('type', 'unknown'),
            "pnl_cash": pnl,
            "pnl_pct": pnl_pct,
            "duration_sec": int(duration)
        })

    # Phase 1: Direct Parent Match
    for sell in sells_list:
        pid = sell['parent_id']
        if pid and pid in buys_dict:
            buy = buys_dict[pid]
            if buy['symbol'] == sell['symbol'] and buy['remaining'] > 1e-9:
                match_qty = min(sell['remaining'], buy['remaining'])
                if match_qty > 0:
                    _add_trade_record(closed_trades, buy, sell, match_qty)
                    sell['remaining'] -= match_qty
                    buy['remaining'] -= match_qty

    # Phase 2: FIFO Fallback for Orphans
    sorted_buy_ids = sorted(buys_dict.keys(), key=lambda k: buys_dict[k]['time'])
    for sell in sells_list:
        if sell['remaining'] <= 1e-9: continue
        for bid in sorted_buy_ids:
            if sell['remaining'] <= 1e-9: break
            buy = buys_dict[bid]
            if buy['symbol'] != sell['symbol'] or buy['remaining'] <= 1e-9: continue
            
            match_qty = min(sell['remaining'], buy['remaining'])
            _add_trade_record(closed_trades, buy, sell, match_qty)
            sell['remaining'] -= match_qty
            buy['remaining'] -= match_qty

    # === 4. ROBUST FIFO PnL CALCULATION ===
    df_lot = pd.DataFrame(closed_trades)
    
    if not df_lot.empty:
        # Run pure FIFO engine
        # Assuming build_closed_trades_df_fifo is available in scope or imported
        df_fifo = build_closed_trades_df_fifo(fills_df, orders_df)
        
        if not df_fifo.empty:
            # FIX: Ensure IDs are strings here too
            df_fifo['exit_order_id'] = df_fifo['exit_order_id'].astype(str)
            df_lot['exit_order_id'] = df_lot['exit_order_id'].astype(str)

            # 1. Aggregate FIFO results by Order ID
            fifo_stats = df_fifo.groupby('exit_order_id')[['pnl_cash', 'qty']].sum()
            
            # 2. Calculate PnL Per Share (Unit PnL)
            fifo_stats['unit_pnl'] = fifo_stats['pnl_cash'] / fifo_stats['qty']
            
            # 3. Map Unit PnL to Lot DF
            unit_pnl_map = fifo_stats['unit_pnl']
            df_lot['fifo_unit_pnl'] = df_lot['exit_order_id'].map(unit_pnl_map)
            
            # 4. Calculate Final FIFO PnL for this specific lot-row
            df_lot['pnl_cash_fifo'] = df_lot['qty'] * df_lot['fifo_unit_pnl']
            
            # 5. Diff
            df_lot['diff_pnl'] = df_lot['pnl_cash'] - df_lot['pnl_cash_fifo']
            
            # Clean up temp col
            df_lot.drop(columns=['fifo_unit_pnl'], inplace=True)
            
        else:
            df_lot['pnl_cash_fifo'] = 0.0
            df_lot['diff_pnl'] = df_lot['pnl_cash']

    # === 5. FINAL FORMATTING ===
    cols = [
        "entry_lot_id", "exit_order_id", "exit_parent_id", "symbol", "side", 
        "qty", "entry_time", "entry_price", "exit_time", "exit_price", 
        "exit_type", "pnl_cash", "pnl_cash_fifo", "diff_pnl", "pnl_pct", "duration_sec"
    ]
    
    if df_lot.empty:
        return pd.DataFrame(columns=cols)

    for c in ['entry_time', 'exit_time']:
        df_lot[c] = pd.to_datetime(df_lot[c]).dt.strftime('%Y-%m-%d %H:%M:%S')
        
    for c in ['pnl_cash', 'pnl_cash_fifo', 'diff_pnl', 'pnl_pct']:
        if c in df_lot.columns:
            df_lot[c] = df_lot[c].fillna(0).round(2)
            
    df_lot['qty'] = df_lot['qty'].round(4)

    return df_lot.sort_values('exit_time', ascending=False).reset_index(drop=True)[cols]


def build_closed_trades_df_fifo(
    fills_df: pd.DataFrame,
    orders_df: Optional[pd.DataFrame] = None
) -> pd.DataFrame:
    """
    Constructs a DataFrame of CLOSED trades based on PURE FIFO matching.
    Strictly matches Sells against the oldest Buys based on execution time.
    """

    # === 1. VALIDATION ===
    required_cols = ['symbol', 'side', 'qty', 'price', 'exec_time']
    if fills_df.empty or not all(c in fills_df.columns for c in required_cols):
        return pd.DataFrame(columns=[
            "entry_lot_id", "exit_order_id", "exit_parent_id", "symbol", "side", 
            "qty", "entry_time", "entry_price", "exit_time", "exit_price", 
            "exit_type", "pnl_cash", "pnl_pct", "duration_sec"
        ])

    df = fills_df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df['exec_time']):
        df['exec_time'] = pd.to_datetime(df['exec_time'], utc=True)

    # Strict sorting for FIFO
    df = df.sort_values('exec_time').reset_index(drop=True)

    # Meta Map
    meta_map = {}
    if orders_df is not None and not orders_df.empty:
        subset = orders_df.sort_values('updated_at', ascending=False).drop_duplicates('id')
        for _, row in subset.iterrows():
            meta_map[str(row['id'])] = {
                'type': row.get('type') or row.get('order_type') or 'unknown',
                'parent_id': str(row['parent_id']) if pd.notna(row.get('parent_id')) else None
            }

    closed_trades = []

    # === 2. MATCHING ===
    for symbol, sym_data in df.groupby('symbol'):
        inventory = []

        for _, row in sym_data.iterrows():
            side = str(row['side']).lower()
            qty = float(row['qty'])
            price = float(row['price'])
            time = row['exec_time']
            oid = str(row.get('order_id', ''))
            
            curr_meta = meta_map.get(oid, {})

            if side == 'buy':
                inventory.append({
                    'qty': qty, 'price': price, 'time': time, 'id': oid
                })

            elif side == 'sell':
                qty_to_close = qty

                while qty_to_close > 1e-9 and inventory:
                    lot = inventory[0]
                    matched_qty = min(lot['qty'], qty_to_close)

                    pnl_cash = (price - lot['price']) * matched_qty
                    pnl_pct = 0.0
                    if lot['price'] != 0:
                        pnl_pct = ((price - lot['price']) / lot['price']) * 100.0

                    duration = (time - lot['time']).total_seconds()

                    closed_trades.append({
                        "entry_lot_id": lot['id'],
                        "exit_order_id": oid,
                        "exit_parent_id": curr_meta.get('parent_id'),
                        "symbol": symbol,
                        "side": "Long",
                        "qty": matched_qty,
                        "entry_time": lot['time'],
                        "entry_price": lot['price'],
                        "exit_time": time,
                        "exit_price": price,
                        "exit_type": curr_meta.get('type', 'unknown'),
                        "pnl_cash": pnl_cash,
                        "pnl_pct": pnl_pct,
                        "duration_sec": int(duration)
                    })

                    qty_to_close -= matched_qty
                    lot['qty'] -= matched_qty

                    if lot['qty'] < 1e-9:
                        inventory.pop(0)

    # === 3. FORMATTING ===
    if not closed_trades:
        return pd.DataFrame()

    results_df = pd.DataFrame(closed_trades)
    
    for col in ['entry_time', 'exit_time']:
        results_df[col] = results_df[col].dt.strftime('%Y-%m-%d %H:%M:%S')

    results_df['pnl_cash'] = results_df['pnl_cash'].round(2)
    results_df['pnl_pct'] = results_df['pnl_pct'].round(2)
    results_df['qty'] = results_df['qty'].round(4)
    
    return results_df.sort_values('exit_time', ascending=False).reset_index(drop=True)


def calculate_metrics(
    trades_df: pd.DataFrame,
    open_trades_df: Optional[pd.DataFrame] = None,
    activities_df: Optional[pd.DataFrame] = None,
    value_weighted: bool = True,
) -> pd.DataFrame:
    """
    Build a final report across closed trades, open positions, and events.

    Args:
        trades_df: DataFrame with `qty`, `entry_price`, `exit_price`,
            `pnl_cash`, `pnl_cash_fifo`, etc.
        open_trades_df: Optional DataFrame with open positions.
        activities_df: Optional DataFrame of account activities.
        value_weighted: Whether to weight averages by invested volume.

    Returns:
        pd.DataFrame: Transposed DataFrame containing equity totals and
        performance metrics.
    """
    total_buy_closed = 0.0
    total_sell_closed = 0.0
    total_pnl_lot = 0.0
    total_pnl_fifo = 0.0

    rr_ratio = np.nan
    breakeven = np.nan
    pl_ratio = np.nan

    avg_take = 0.0
    avg_stop = 0.0
    avg_hold_take = 0.0
    avg_hold_stop = 0.0

    closed_trades_count = 0

    if not trades_df.empty:
        df = trades_df.copy()

        if "pnl_cash_fifo" not in df.columns:
            df["pnl_cash_fifo"] = 0.0

        cols_num = [
            "qty",
            "entry_price",
            "exit_price",
            "pnl_cash",
            "pnl_cash_fifo",
        ]
        for c in cols_num:
            if c in df.columns:
                df[c] = (
                    pd.to_numeric(df[c], errors="coerce").fillna(0)
                )

        for c in ["entry_time", "exit_time"]:
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], utc=True)

        def identify_exit_class(val):
            s = str(val).lower()
            if "limit" in s:
                return "TP"
            if "stop" in s:
                return "SL"
            return "MANUAL"

        if "exit_type" in df.columns:
            df["exit_class_raw"] = df["exit_type"].apply(
                identify_exit_class
            )
        else:
            df["exit_class_raw"] = "MANUAL"

        if "entry_lot_id" in df.columns:

            def aggregate_lot(sub):
                qty_total = sub["qty"].sum()
                type_counts = (
                    sub.groupby("exit_class_raw")["qty"]
                    .sum()
                    .sort_values(ascending=False)
                )
                major_type = (
                    type_counts.index[0]
                    if not type_counts.empty
                    else "MANUAL"
                )

                w_entry = (
                    np.average(
                        sub["entry_price"], weights=sub["qty"]
                    )
                    if qty_total > 0
                    else 0
                )
                w_exit = (
                    np.average(
                        sub["exit_price"], weights=sub["qty"]
                    )
                    if qty_total > 0
                    else 0
                )

                return pd.Series(
                    {
                        "qty": qty_total,
                        "entry_price": w_entry,
                        "exit_price": w_exit,
                        "pnl_cash": sub["pnl_cash"].sum(),
                        "pnl_cash_fifo": sub["pnl_cash_fifo"].sum(),
                        "entry_time": sub["entry_time"].min(),
                        "exit_time": sub["exit_time"].max(),
                        "exit_class": major_type,
                    }
                )

            calc_cols = [
                "qty",
                "entry_price",
                "exit_price",
                "pnl_cash",
                "pnl_cash_fifo",
                "entry_time",
                "exit_time",
                "exit_class_raw",
            ]
            collapsed = (
                df.groupby("entry_lot_id")[calc_cols]
                .apply(aggregate_lot)
                .reset_index()
            )
        else:
            collapsed = df.copy()
            collapsed.rename(
                columns={"exit_class_raw": "exit_class"}, inplace=True
            )

        closed_trades_count = len(collapsed)

        collapsed["duration_days"] = (
            collapsed["exit_time"] - collapsed["entry_time"]
        ).dt.total_seconds() / 86400.0
        collapsed["invested_vol"] = (
            collapsed["qty"] * collapsed["entry_price"]
        )

        tp_wins = collapsed[
            (collapsed["exit_class"] == "TP")
            & (collapsed["pnl_cash"] > 0)
        ]
        sl_losses = collapsed[
            (collapsed["exit_class"] == "SL")
            & (collapsed["pnl_cash"] < 0)
        ]

        def get_w_avg(data, col):
            if data.empty or data["invested_vol"].sum() == 0:
                return 0.0
            if value_weighted:
                return np.average(data[col], weights=data["invested_vol"])
            return data[col].mean()

        avg_take = get_w_avg(tp_wins, "pnl_cash")
        avg_stop = abs(get_w_avg(sl_losses, "pnl_cash"))
        avg_hold_take = get_w_avg(tp_wins, "duration_days")
        avg_hold_stop = get_w_avg(sl_losses, "duration_days")

        rr_ratio = (avg_take / avg_stop) if avg_stop > 0 else 0.0
        breakeven = (
            (100 / (1 + rr_ratio)) if rr_ratio > 0 else 0.0
        )
        denom = len(tp_wins) + len(sl_losses)
        pl_ratio = (
            (len(tp_wins) / denom * 100) if denom > 0 else 0.0
        )

        total_buy_closed = (
            collapsed["qty"] * collapsed["entry_price"]
        ).sum()
        total_sell_closed = (
            collapsed["qty"] * collapsed["exit_price"]
        ).sum()

        total_pnl_lot = collapsed["pnl_cash"].sum()
        total_pnl_fifo = collapsed["pnl_cash_fifo"].sum()

    total_unrealized_pnl = 0.0
    count_open = 0
    total_open_val = 0.0
    total_buy_open = 0.0

    if open_trades_df is not None and not open_trades_df.empty:
        # PnL Column
        pnl_col = next(
            (
                col
                for col in ["Profit/Loss", "Unrealized PnL", "pnl"]
                if col in open_trades_df.columns
            ),
            None,
        )
        # Market Value Column
        val_col = next(
            (
                col
                for col in [
                    "Current Market Value",
                    "Market Value",
                    "value",
                ]
                if col in open_trades_df.columns
            ),
            None,
        )
        # Price Column
        price_col = next(
            (
                col
                for col in ["Buy Price", "entry_price"]
                if col in open_trades_df.columns
            ),
            None,
        )
        # Quantity Column (Priority: Remaining -> Buy -> qty)
        qty_col = next(
            (
                col
                for col in ["Remaining Qty", "Buy Qty", "qty"]
                if col in open_trades_df.columns
            ),
            None,
        )

        if pnl_col:
            total_unrealized_pnl = (
                pd.to_numeric(
                    open_trades_df[pnl_col], errors="coerce"
                )
                .fillna(0)
                .sum()
            )

        if val_col:
            total_open_val = (
                pd.to_numeric(
                    open_trades_df[val_col], errors="coerce"
                )
                .fillna(0)
                .sum()
            )

        if price_col and qty_col:
            op_p = pd.to_numeric(
                open_trades_df[price_col], errors="coerce"
            ).fillna(0)
            op_q = pd.to_numeric(
                open_trades_df[qty_col], errors="coerce"
            ).fillna(0)
            total_buy_open = (op_p * op_q).sum()

        count_open = len(open_trades_df)

    total_fees = 0.0
    total_divs = 0.0
    total_int = 0.0

    if activities_df is not None and not activities_df.empty:
        act = activities_df.copy()
        if (
            "net_amount" not in act.columns
            and "qty" in act.columns
        ):
            act["net_amount"] = act["qty"]
        act["net_amount"] = (
            pd.to_numeric(act["net_amount"], errors="coerce")
            .fillna(0)
        )

        total_fees = act[act["activity_type"] == "FEE"][
            "net_amount"
        ].sum()
        total_divs = act[
            act["activity_type"].isin(["DIV", "DIVNRA"])
        ]["net_amount"].sum()
        total_int = act[act["activity_type"] == "INT"][
            "net_amount"
        ].sum()

    net_realized_fifo = (
        total_pnl_fifo + total_fees + total_divs + total_int
    )
    total_account_result = net_realized_fifo + total_unrealized_pnl
    grand_total_buy = total_buy_closed + total_buy_open

    result = {
        "TOTAL ACCOUNT RESULT (Equity)": round(total_account_result, 2),
        "NET REALIZED CASH (FIFO)": round(net_realized_fifo, 2),
        "Unrealized P/L (Open)": round(total_unrealized_pnl, 2),
        "Current Open Value": f"{total_open_val:,.2f}",
        "------------------": "",
        "Breakeven %": (
            f"%{breakeven:.1f}" if not pd.isna(breakeven) else "N/A"
        ),
        "Win Rate %": (
            f"%{pl_ratio:.1f}" if not pd.isna(pl_ratio) else "N/A"
        ),
        "Risk-Reward Ratio": (
            round(rr_ratio, 2) if not pd.isna(rr_ratio) else "N/A"
        ),
        "Mean Take ($)": round(avg_take, 2),
        "Mean Stop ($)": round(avg_stop, 2),
        "Mean Holding Take (Days)": round(avg_hold_take, 2),
        "Mean Holding Stop (Days)": round(avg_hold_stop, 2),
        "------------------ ": "",
        "Realized P/L (Broker/FIFO)": round(total_pnl_fifo, 2),
        "Realized P/L (Strategy Only)": round(total_pnl_lot, 2),
        "Total Fees": round(total_fees, 2),
        "Net Dividends": round(total_divs, 2),
        "Net Interest": round(total_int, 2),
        "Total Buy Volume": f"{grand_total_buy:,.2f}",
        "Total Sell Volume": f"{total_sell_closed:,.2f}",
        "Closed Trades (n)": closed_trades_count,
        "Open Trades (n)": count_open,
    }

    return pd.DataFrame([result]).T.rename(columns={0: "Value"})


def check_sl_tp(api, symbols="RIG"):
    """
    Performs a summarized check of broker state (Open + Held orders)
    for the given symbol or list of symbols.

    Instead of printing a long report per symbol, this function
    prints a single table with one row per symbol showing whether
    the position is correctly protected by orders or has issues.

    Args:
        api: The Alpaca API instance.
        symbols (str or list): Ticker symbol or list of symbols.
    """
    # Normalize symbols to a list
    if isinstance(symbols, str):
        symbols = [symbols]

    summary_rows = []

    for symbol in symbols:
        # 1. Query open orders (live)
        try:
            active_orders = api.list_orders(
                status="open",
                symbols=[symbol],
                limit=50,
            )
        except Exception as e:
            print(f"Error fetching open orders for {symbol}: {e}")
            active_orders = []

        # 2. Query held orders (waiting)
        try:
            held_orders = api.list_orders(
                status="held",
                symbols=[symbol],
                limit=50,
            )
        except Exception as e:
            print(f"Error fetching held orders for {symbol}: {e}")
            held_orders = []

        # Compute sell-side quantities for protection
        open_sell_qty = sum(
            float(o.qty) for o in active_orders if o.side == "sell"
        )
        held_sell_qty = sum(
            float(o.qty) for o in held_orders if o.side == "sell"
        )

        # 3. Get actual position
        try:
            pos = api.get_position(symbol)
            pos_qty = float(pos.qty)
        except Exception:
            pos_qty = 0.0

        # 4. Determine protection status
        if pos_qty == 0 and open_sell_qty == 0 and held_sell_qty == 0:
            status = "No position, no orders"
            issue = False
        elif pos_qty == 0 and (open_sell_qty > 0 or held_sell_qty > 0):
            status = "No position, but exit orders exist"
            issue = True
        elif (
            pos_qty > 0
            and open_sell_qty == pos_qty
            and held_sell_qty == pos_qty
        ):
            status = "OK (fully protected bracket)"
            issue = False
        elif pos_qty > 0 and open_sell_qty == 0 and held_sell_qty == 0:
            status = "Unprotected (no exit orders)"
            issue = True
        else:
            status = "Imbalance between position and exit orders"
            issue = True

        summary_rows.append(
            {
                "Symbol": symbol,
                "PositionQty": pos_qty,
                "OpenSellQty": open_sell_qty,
                "HeldSellQty": held_sell_qty,
                "Status": status,
                "HasIssue": issue,
            }
        )

    # 5. Print single summary table
    if not summary_rows:
        print("No symbols to check.")
        return pd.DataFrame(columns=[
            "Symbol",
            "PositionQty",
            "OpenSellQty",
            "HeldSellQty",
            "Status",
            "HasIssue",
        ])

    df_summary = pd.DataFrame(summary_rows)

    print("\n=== Broker Protection Summary ===")
    print(df_summary.to_string(index=False))

    total = len(df_summary)
    issues = df_summary["HasIssue"].sum()
    ok = total - issues

    print("\nSummary:")
    print(f"  Total symbols checked: {total}")
    print(f"  OK (fully consistent): {ok}")
    print(f"  With issues:           {issues}")

    return df_summary
