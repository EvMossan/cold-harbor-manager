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
    Download orders in fixed windows so pagination stays reliable.

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

    # Derive UTC window boundaries; default end is tomorrow.
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
        # Accept Alpaca objects and dicts alike.
        root = getattr(order_obj, '_raw', order_obj)

        # Skip IDs already captured by overlapping windows.
        if root['id'] in seen_ids:
            continue
        seen_ids.add(root['id'])

        parent_row = root.copy()

        # Keep legs in parent_row as a fallback; do not remove them.
        legs_data = parent_row.get('legs')
        
        # Keep parent_id None for root orders unless it already exists.
        if 'parent_id' not in parent_row:
            parent_row['parent_id'] = None
            
        flat_rows.append(parent_row)

        # Flatten legs into their own rows.
        if legs_data:
            for leg in legs_data:
                leg_row = leg.copy()
                # Explicitly link child to parent
                leg_row['parent_id'] = root['id'] 
                
                # Retain child row separately.
                flat_rows.append(leg_row)

                # Track leg IDs to avoid duplicate flattening.
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

    # Keep newest records first by creation timestamp.
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
    Load raw orders from the database and cast UUIDs to strings.
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
    
    sql = f"""
        SELECT
            id, client_order_id, parent_id, symbol, side, order_type, status,
            qty, filled_qty, filled_avg_price, limit_price, stop_price,
            created_at, updated_at, submitted_at, filled_at,
            expired_at, expires_at, canceled_at, replaced_by, replaces,
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
        "filled_at", "expired_at", "canceled_at",
        'expires_at',
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
    # asyncpg returns uuid.UUID objects while portfolio logic expects
    # strings.
    # We cast them explicitly, keeping None as None instead of "None".
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
    Fetch account activity history and normalize timestamps.

    This coalesces multiple timestamp fields into a single
    `exec_time` column so non-trade activities stay dated when
    `transaction_time` is missing.

    Args:
        api: Alpaca-compatible client.
        start_date (datetime, optional): Earliest date to fetch,
            inclusive.
        end_date (datetime, optional): Latest date to fetch,
            exclusive.

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
    # We iterate through priority columns to fill exec_time gaps.
    # FILL rows usually have transaction_time.
    # CSD/FEE rows rely on date.
    possible_time_cols = [
        "transaction_time",
        "activity_time",
        "date",
        "timestamp",
        "created_at",
    ]

    df["exec_time"] = pd.Series(
        pd.NaT, index=df.index, dtype="datetime64[ns, UTC]"
    )

    for col in possible_time_cols:
        if col in df.columns:
            # Fill missing values from the current column
            current_col_dates = pd.to_datetime(
                df[col], utc=True, errors="coerce"
            )
            df["exec_time"] = df["exec_time"].fillna(current_col_dates)

    df["exec_time"] = df["exec_time"].infer_objects(copy=False)
    # --- DATA NORMALIZATION ---
    # Ensure net_amount is numeric because it powers equity metrics.
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
    orders_df: pd.DataFrame, 
    api: Any = None
) -> pd.DataFrame:
    """
    Build a lot portfolio by preferring each leg's latest replacement.

    Resolve every leg to its newest successor, use that row for status and
    pricing, and fall back to the 90-day expiration rule when needed.
    """
    # --- 1. Fetch Broker Data (Market Data & Averages) ---
    market_data: Dict[str, Dict[str, float]] = {}
    
    if api:
        try:
            raw_positions = api.list_positions()
            for p in raw_positions:
                qty = float(p.qty)
                cur_px = float(getattr(p, "current_price", 0.0))
                
                # Fallback for market value calculation
                if cur_px == 0.0 and qty != 0:
                    try:
                        cur_px = float(p.market_value) / qty
                    except Exception:
                        cur_px = 0.0
                
                avg_entry = float(p.avg_entry_price)
                
                market_data[p.symbol.lower()] = {
                    "cur_px": cur_px,
                    "avg_entry": avg_entry
                }
        except Exception as e:
            print(f"Warning: API fetch failed. {e}")

    # --- 2. Pre-process: Clean, sort, and deduplicate ---
    if orders_df.empty:
        return pd.DataFrame()
    
    df_clean = orders_df.copy()
    
    # Choose updated_at or created_at for datetime conversion.
    sort_col = 'updated_at' if 'updated_at' in df_clean.columns else 'created_at'
    if sort_col in df_clean.columns:
        df_clean[sort_col] = pd.to_datetime(df_clean[sort_col], utc=True, errors='coerce')
    
    # Sort descending to keep the latest version of each ID.
    df_clean.sort_values(by=sort_col, ascending=False, inplace=True)
    
    # Deduplicate primarily by 'id'
    dedup_subset = ['id']
    if 'order_id' in df_clean.columns and df_clean['order_id'].notna().any():
        dedup_subset = ['order_id']
            
    df_clean.drop_duplicates(subset=dedup_subset, keep='first', inplace=True)

    # --- 3. Build replacement chain and lookup map ---
    
    # Index rows by ID for fast lookup of fresh data.
    if 'id' not in df_clean.columns:
        return pd.DataFrame()
        
    orders_map = df_clean.set_index('id').to_dict('index')
    
    # Map old IDs to their replacements.
    replacement_map = {}
    if 'replaces' in df_clean.columns:
        replacers = df_clean.dropna(subset=['replaces'])
        for _, row in replacers.iterrows():
            old_id = str(row['replaces'])
            new_id = str(row['id'])
            replacement_map[old_id] = new_id

    # Helper that follows replacements to the final ID.
    def get_latest_successor(order_id):
        curr = str(order_id)
        while curr in replacement_map:
            curr = replacement_map[curr]
        return curr

    # --- 4. Filter Parents ---
    parents = df_clean[
        (df_clean['side'] == 'buy') & 
        (df_clean['status'] == 'filled') &
        (df_clean['legs'].notna())
    ].copy()
    
    active_lots = []
    live_statuses = {'new', 'held', 'accepted', 'partially_filled', 'open', 'calculated', 'pending_replace'}
    
    # CONSTANT: Alpaca GTC Order Expiration (Days)
    ALPACA_GTC_DAYS = 90

    # --- 5. Iterate & Unpack ---
    for _, row in parents.iterrows():
        legs_raw = row.get('legs')
        legs_data = []
        
        # Safe JSON Parsing
        if isinstance(legs_raw, list):
            legs_data = legs_raw
        elif isinstance(legs_raw, str):
            try:
                clean_json = (
                    legs_raw.replace("'", '"')
                    .replace("None", "null")
                    .replace("True", "true")
                    .replace("False", "false")
                )
                legs_data = json.loads(clean_json)
            except Exception:
                try:
                    legs_data = ast.literal_eval(legs_raw)
                except Exception:
                    continue
        
        if not legs_data:
            continue

        # Init Leg Placeholders
        tp_price = None
        tp_id = None
        sl_price = None
        sl_id = None
        expiry_ts = None
        has_active_legs = False
        
        # Parent creation time (fallback for expiry calculation)
        parent_created_at = row.get('created_at')
        
        for leg in legs_data:
            original_leg_id = str(leg.get('id'))
            
            # === RESOLVE LATEST SUCCESSOR ===
            latest_leg_id = get_latest_successor(original_leg_id)
            
            # === GET FRESH DATA ===
            fresh_order = orders_map.get(latest_leg_id)
            
            leg_created_at = None
            
            if fresh_order:
                # Use DB data (Preferred)
                current_status = str(fresh_order.get('status')).lower()
                l_type = str(fresh_order.get('order_type') or fresh_order.get('type')).lower()
                limit_px = fresh_order.get('limit_price')
                stop_px  = fresh_order.get('stop_price')
                
                # Capture timestamps from fresh order
                if not expiry_ts:
                    expiry_ts = fresh_order.get('expires_at') or fresh_order.get('expired_at')
                leg_created_at = fresh_order.get('created_at')
                
            else:
                # Fallback to JSON data (Stale)
                current_status = str(leg.get('status')).lower()
                l_type = str(leg.get('order_type') or leg.get('type')).lower()
                limit_px = leg.get('limit_price')
                stop_px  = leg.get('stop_price')
                
                # Capture timestamps from JSON
                if not expiry_ts:
                    expiry_ts = leg.get('expires_at')
                leg_created_at = leg.get('created_at')

            # === CHECK IF LIVE ===
            if current_status in live_statuses:
                has_active_legs = True
                
                # Adjust expiration when explicit value is missing.
                # Estimate expiry as created_at plus 90 days.
                if not expiry_ts:
                    # Use leg creation time before parent time.
                    base_time = leg_created_at or parent_created_at
                    if base_time:
                        try:
                            dt_base = pd.to_datetime(base_time, utc=True)
                            if pd.notna(dt_base):
                                expiry_ts = dt_base + pd.Timedelta(days=ALPACA_GTC_DAYS)
                        except Exception:
                            pass
                
                # Normalize Prices
                l_limit = float(limit_px) if limit_px is not None else 0.0
                l_stop  = float(stop_px) if stop_px is not None else 0.0
                
                # Classify TP vs SL
                if l_type == 'limit':
                    tp_price = l_limit
                    tp_id = latest_leg_id
                elif 'stop' in l_type:
                    sl_price = l_stop if l_stop > 0 else l_limit
                    sl_id = latest_leg_id

        # Build a lot row only when legs remain live.
        if has_active_legs:
            p_id = str(row.get('id'))
            if 'order_id' in row and pd.notna(row['order_id']):
                p_id = str(row.get('order_id'))

            symbol = str(row.get('symbol'))
            qty = float(row.get('filled_qty') or 0.0)
            
            # Prices
            strategy_entry = float(row.get('filled_avg_price') or 0.0)
            
            # Broker average fallback
            m_data = market_data.get(symbol.lower(), {})
            current_price = m_data.get("cur_px", 0.0)
            broker_avg = m_data.get("avg_entry", 0.0)
            
            if broker_avg == 0.0:
                broker_avg = strategy_entry
            
            pos = {
                "Parent ID": p_id,
                "Symbol": symbol,
                "Buy Date": row.get('created_at'),
                "Buy Qty": qty,
                "Remaining Qty": qty,
                
                "Buy Price": strategy_entry,
                "Avg Entry (API)": broker_avg,
                
                "Take_Profit_Price": tp_price,
                "Stop_Loss_Price": sl_price,
                "Take_Profit_ID": tp_id,
                "Stop_Loss_ID": sl_id,
                "Expiration Date": expiry_ts,
                
                "Source": "Simple",
                "Current_Price": current_price,
            }
            active_lots.append(pos)

    # --- 7. DataFrame Construction & Metrics ---
    df = pd.DataFrame(active_lots)
    
    final_cols_ordered = [
        "Parent ID", "Symbol", "Buy Date", "Buy Qty", "Remaining Qty", 
        "Buy Price", "Avg Entry (API)", 
        "Take_Profit_Price", "Stop_Loss_Price", 
        "Take_Profit_ID", "Stop_Loss_ID", 
        "Source", "Holding_Period", "Days_To_Expire",
        "Current_Price", "Current Market Value", "Profit/Loss", 
        "TP_reach, %", "BrE"
    ]

    if df.empty:
        return pd.DataFrame(columns=final_cols_ordered)

    # Calculate Time Metrics
    df["Buy Date Obj"] = pd.to_datetime(df["Buy Date"], utc=True)
    now = pd.Timestamp.now(tz=df["Buy Date Obj"].dt.tz)
    df["Holding_Period"] = (now - df["Buy Date Obj"]).dt.days
    df["Buy Date"] = df["Buy Date Obj"].dt.strftime("%Y-%m-%d %H:%M:%S")
    
    # Calculate Days To Expire
    # Use 'coerce' to handle parsing errors.
    df["Expiration Obj"] = pd.to_datetime(df["Expiration Date"], utc=True, errors='coerce')
    df["Days_To_Expire"] = (df["Expiration Obj"] - now).dt.days

    # Financials
    df["Current_Price"] = df["Current_Price"].fillna(0.0)
    df["Current Market Value"] = df["Remaining Qty"] * df["Current_Price"]
    df["Profit/Loss"] = (df["Current_Price"] - df["Avg Entry (API)"]) * df["Remaining Qty"]

    # Technical Metrics
    diff_strategy = df["Current_Price"] - df["Buy Price"]
    tp_dist = df["Take_Profit_Price"] - df["Buy Price"]
    sl_dist = df["Buy Price"] - df["Stop_Loss_Price"]
    
    df["TP_reach, %"] = np.nan
    mask_profit = (diff_strategy >= 0) & (tp_dist > 0)
    df.loc[mask_profit, "TP_reach, %"] = (diff_strategy / tp_dist * 100).round(2)
    mask_loss = (diff_strategy < 0) & (sl_dist > 0)
    df.loc[mask_loss, "TP_reach, %"] = (diff_strategy / sl_dist * 100).round(2)

    # Break-Even Flag
    df["BrE"] = df.apply(
        lambda r: 1
        if (
            pd.notna(r["Stop_Loss_Price"])
            and r["Buy Price"] != 0
            and abs(r["Stop_Loss_Price"] - r["Buy Price"]) < (0.01 * r["Buy Price"])
        )
        else 0,
        axis=1,
    )

    return (
        df[final_cols_ordered]
        .sort_values(["Symbol", "Buy Date"], ascending=[True, False])
        .reset_index(drop=True)
    )


def build_closed_trades_df_lot(
    fills_df: pd.DataFrame,
    orders_df: Optional[pd.DataFrame] = None
) -> pd.DataFrame:
    """
    Build CLOSED trades with strict parent-child linkage.

    Compress fills and aggregate via `.agg()` so PnL stays aligned with the
    FIFO engine while avoiding GroupBy.apply.
    """

    # === 1. HELPER: COMPRESS FILLS (OPTIMIZED) ===
    def _compress_fills(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or 'order_id' not in df.columns:
            return df
        
        # Work on a copy to avoid SettingWithCopy warnings.
        local_df = df.copy()
        
        # Force order_id to string for consistency.
        local_df['order_id'] = local_df['order_id'].astype(str)
        
        # Pre-calculate Total Value for Weighted Average Price
        # (Math: Sum(Price * Qty) / Sum(Qty))
        local_df['_total_val'] = local_df['price'] * local_df['qty']
        
        # Use .agg() instead of .apply() to drop the warning.
        # This is also faster.
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
        
    # Ensure global IDs are strings.
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
        # Run pure FIFO engine.
        # Assume build_closed_trades_df_fifo is available here.
        df_fifo = build_closed_trades_df_fifo(fills_df, orders_df)
        
        if not df_fifo.empty:
            # Ensure IDs are strings here too.
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
