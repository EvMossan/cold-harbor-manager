from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd


def fetch_orders(api, days_back=365):
    """
    Downloads orders using the "Sliding Window" (Time Window) method.

    Ignores Alpaca cursors. Simply takes date intervals and downloads
    everything within them. The most reliable method if the API is
    glitchy with pagination.
    """
    # print(f">>> ROBUST ORDER DOWNLOAD (last {days_back} days)...")

    # 1. Define time boundaries
    # Since the simulation is in the future (2025), take 'now' from
    # the API or system. For reliability, set the end date to tomorrow.
    end_date = pd.Timestamp.now(tz=timezone.utc) + timedelta(days=1)
    start_date = end_date - timedelta(days=days_back)

    all_raw_orders = []

    # Window size in days (if too large, API might truncate data)
    # 5 days is optimal for status='all' + nested=True
    window_size = timedelta(days=5)

    current_start = start_date

    while current_start < end_date:
        current_end = current_start + window_size

        # Format dates for API (ISO format)
        t_start = current_start.isoformat()
        t_end = current_end.isoformat()

        # print(f"   Requesting window: {current_start.date()} -> "
        #       f"{current_end.date()}")

        try:
            # Request a specific chunk of time
            # 'after'/'until' work more reliably than 'page_token'
            batch = api.list_orders(
                status='all',
                limit=500,
                nested=True,
                after=t_start,
                until=t_end,
                direction='desc'
            )

            if batch:
                all_raw_orders.extend(batch)
                # print(f"      -> Found {len(batch)} orders")

        except Exception as e:
            print(f"   [Error] In window {t_start}: {e}")

        current_start += window_size

    # --- UNPACKING AND CLEANING ---
    if not all_raw_orders:
        print("No orders found.")
        return pd.DataFrame()

    # print(f"Total objects downloaded: {len(all_raw_orders)}. "
    #       f"Starting unpacking...")

    flat_rows = []
    seen_ids = set()  # Protection against duplicates at date boundaries

    for order_obj in all_raw_orders:
        root = getattr(order_obj, '_raw', order_obj)

        if root['id'] in seen_ids:
            continue
        seen_ids.add(root['id'])

        # Parent
        parent_row = root.copy()
        legs_data = parent_row.pop('legs', None)
        parent_row['parent_id'] = None
        flat_rows.append(parent_row)

        # Children
        if legs_data:
            for leg in legs_data:
                leg_row = leg.copy()
                leg_row['parent_id'] = root['id']
                flat_rows.append(leg_row)

    df = pd.DataFrame(flat_rows)

    # Type conversion
    date_cols = ['created_at', 'updated_at', 'filled_at', 'submitted_at']
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

    # print(f"SUCCESS. Final table: {len(df)} rows. "
    #       f"(Check for presence of 2025-10-23)")
    return df


def fetch_all_activities(api):
    """
    Fetches the complete account activity history (FILL, FEE, DIV, JNLS, etc.).
    Does not filter by activity type.
    """
    # print(">>> Downloading full account history (All Events)...")
    all_activities = []
    last_id = None

    while True:
        # Removing 'activity_types' fetches all event types
        params = {
            'direction': 'desc',
            'page_size': 100
        }
        if last_id:
            params['page_token'] = last_id

        try:
            # This returns a list of mixed activity objects
            activities = api.get_activities(**params)
        except Exception as e:
            print(f"API Error: {e}")
            break

        if not activities:
            break
        
        all_activities.extend(activities)
        last_id = getattr(activities[-1], 'id', None)

    # --- PROTECTION AGAINST EMPTY RESPONSE ---
    if not all_activities:
        # print("Account history is empty.")
        return pd.DataFrame()

    # Convert to DataFrame
    # Note: Different activity types have different fields.
    # Pandas will create columns for all fields, filling missing ones with NaN.
    data = [x._raw if hasattr(x, '_raw') else x for x in all_activities]
    df = pd.DataFrame(data)

    # --- GUARANTEED CREATION OF EXEC_TIME ---
    # Common timestamp fields across different activity types
    possible_time_cols = [
        'transaction_time', 'activity_time', 'date', 'timestamp', 'created_at'
    ]
    time_col_found = False

    for col in possible_time_cols:
        if col in df.columns:
            df['exec_time'] = pd.to_datetime(
                df[col], utc=True, errors='coerce'
            )
            time_col_found = True
            break

    if not time_col_found:
        print(f"!!! WARNING: Time column not found. "
              f"Available: {list(df.columns)}")
        df['exec_time'] = pd.NaT

    # --- DATA NORMALIZATION ---
    # Apply numeric conversion only if columns exist 
    # (some events like 'JNLS' might not have price/qty)
    numeric_cols = ['qty', 'price', 'cum_qty', 'leaves_qty']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Normalize strings
    if 'side' in df.columns:
        df['side'] = df['side'].str.lower()
    if 'symbol' in df.columns:
        df['symbol'] = df['symbol'].astype(str).replace('nan', '')
    if 'activity_type' in df.columns:
        df['activity_type'] = df['activity_type'].str.upper()

    # print(f"Loaded {len(df)} account events.")
    return df


def build_lot_portfolio(
    fills_df: pd.DataFrame,
    orders_df: pd.DataFrame,
    api: Any = None
) -> pd.DataFrame:
    """
    Builds a portfolio based on lots (FIFO/Specific ID).
    
    Includes market data columns (Current Price, P/L, TP Reach, etc.)
    to align with the open positions schema.

    Args:
        fills_df: DataFrame of filled trades.
        orders_df: DataFrame of orders.
        api: Alpaca/Broker API object (must support .list_positions()).
    """
    # print(">>> Building portfolio: Lot-Based + Chain + "
    #       "Orphan Matching (FIXED)...")

    # === 0. HELPER: Mock _be_price if not defined globally ===
    # Assumes Break Even is roughly the Buy Price. 
    # Adjust logic if you have a specific formula for fees.
    def _be_price(p):
        return p

    # === 1. FETCH MARKET DATA ===
    pos_map: Dict[str, Dict[str, float]] = {}
    if api:
        try:
            for p in api.list_positions():
                q = float(p.qty)
                if abs(q) < 1e-9:
                    continue
                # Handle API variations for current price
                cur_px = float(getattr(p, "current_price", 0.0))
                if cur_px == 0.0 and q != 0:
                    cur_px = float(p.market_value) / q
                
                pos_map[p.symbol.lower()] = {
                    "qty": q, 
                    "avg_px": float(p.avg_entry_price), 
                    "cur_px": cur_px
                }
        except Exception as e:
            print(f"Warning: list_positions failed: {e}")
            pos_map = {}

    # === 2. PROTECTION AGAINST EMPTY INPUT ===
    empty_cols = [
        'Parent ID', 'Symbol', 'Buy Date', 'Buy Qty', 'Buy Price', 'Buy_Value',
        'Take_Profit_Price', 'Stop_Loss_Price', 'Source', 
        'Holding_Period', 'Current_Price', 'Current Market Value', 
        'Profit/Loss', '_avg_px_symbol', 'TP_reach, %', 'BrE'
    ]

    if fills_df.empty:
        print("Warning: Trades table is empty. Portfolio is empty.")
        return pd.DataFrame(columns=empty_cols)

    # Check for the presence of the critical time column
    time_col = 'exec_time'
    if time_col not in fills_df.columns:
        for alt in ['transaction_time', 'timestamp', 'created_at']:
            if alt in fills_df.columns:
                fills_df[time_col] = pd.to_datetime(fills_df[alt], utc=True)
                break

    if time_col not in fills_df.columns:
        print("ERROR: Trade time column not found. Sorting impossible.")
        return pd.DataFrame(columns=empty_cols)

    # Sort trades chronologically (critical for FIFO)
    fills_df = fills_df.sort_values(time_col)

    # === 3. PREPARING ORDERS ===
    orders_map = {}
    sell_order_parents = {}
    children_map = {}

    if not orders_df.empty:
        # Remove duplicates
        orders_df = orders_df.drop_duplicates(subset='id')
        orders_map = orders_df.set_index('id').to_dict('index')

        # Map parents for sells
        if 'parent_id' in orders_df.columns:
            temp = orders_df[['id', 'parent_id']].dropna()
            sell_order_parents = pd.Series(
                temp.parent_id.values, index=temp.id
            ).to_dict()

            # Map children
            kids = orders_df[orders_df['parent_id'].notna()]
            for pid, group in kids.groupby('parent_id'):
                children_map[str(pid)] = group['id'].tolist()

    # === 4. HELPER FOR CHAINS ===
    def trace_active_leg(start_order_id, visited=None):
        if start_order_id not in orders_map:
            return None
        if visited is None:
            visited = set()
        if start_order_id in visited:
            return None
        visited.add(start_order_id)

        order = orders_map[start_order_id]
        status = order.get('status')
        o_type = order.get('order_type') or order.get('type')

        active_statuses = [
            'new', 'held', 'accepted', 'partially_filled',
            'pending_new', 'calculated', 'open'
        ]
        if status in active_statuses:
            if o_type == 'limit':
                return order.get('limit_price')
            elif o_type in ['stop', 'stop_limit']:
                return order.get('stop_price')

        if status == 'replaced':
            replaced_by_id = order.get('replaced_by')
            if replaced_by_id:
                return trace_active_leg(str(replaced_by_id), visited)
        return None

    # === 5. LOT ASSEMBLY (CORE LOGIC) ===
    lots = {}
    fifo_queues = {}

    for _, row in fills_df.iterrows():
        side = str(row.get('side')).lower()
        symbol = str(row.get('symbol'))
        qty = float(row.get('qty', 0))
        price = float(row.get('price', 0))
        exec_time = row.get(time_col)
        fill_oid = str(row.get('order_id'))

        if side == 'buy':
            if fill_oid not in lots:
                lots[fill_oid] = {
                    'Parent ID': fill_oid,
                    'Symbol': symbol,
                    'Buy Date': exec_time,
                    'Buy Price': price,
                    'Total Cost': price * qty,
                    'Original Qty': qty,
                    'Remaining Qty': qty,
                    'Source': 'Matched'
                }
                if symbol not in fifo_queues:
                    fifo_queues[symbol] = []
                fifo_queues[symbol].append(fill_oid)
            else:
                lot = lots[fill_oid]
                lot['Original Qty'] += qty
                lot['Remaining Qty'] += qty
                lot['Total Cost'] += (price * qty)
                if lot['Original Qty'] > 0:
                    lot['Buy Price'] = lot['Total Cost'] / lot['Original Qty']

        elif side == 'sell':
            qty_to_sell = qty
            
            # 1. Direct link
            parent_id = str(sell_order_parents.get(fill_oid))
            if parent_id in lots and lots[parent_id]['Remaining Qty'] > 0:
                deduct = min(lots[parent_id]['Remaining Qty'], qty_to_sell)
                lots[parent_id]['Remaining Qty'] -= deduct
                qty_to_sell -= deduct

            # 2. FIFO Matching
            if qty_to_sell > 0.000001 and symbol in fifo_queues:
                for lot_id in list(fifo_queues[symbol]):
                    if qty_to_sell <= 0.000001:
                        break
                    target = lots[lot_id]
                    if target['Remaining Qty'] <= 0.000001:
                        continue
                    deduct = min(target['Remaining Qty'], qty_to_sell)
                    target['Remaining Qty'] -= deduct
                    qty_to_sell -= deduct

    # === 6. FINALIZATION AND TARGET SEARCH ===
    now = pd.Timestamp.now(tz=timezone.utc)
    final_lots_data = []

    for oid, lot in lots.items():
        qty_rem = lot['Remaining Qty']
        if qty_rem <= 0.000001:
            continue

        symbol = lot['Symbol']
        buy_price = lot['Buy Price']
        buy_dt = lot['Buy Date']

        tp_price = None
        sl_price = None
        source_note = 'Calculated'

        # Search via Parent/Child links
        if oid in children_map:
            for child_id in children_map[oid]:
                child_id = str(child_id)
                active_price = trace_active_leg(child_id)
                if active_price is not None:
                    child = orders_map[child_id]
                    o_type = child.get('order_type') or child.get('type')
                    if o_type == 'limit':
                        tp_price = float(active_price)
                    elif o_type in ['stop', 'stop_limit']:
                        sl_price = float(active_price)

        if tp_price or sl_price:
            source_note = 'Chain Traced'

        # Look up market data
        sym_data = pos_map.get(symbol.lower(), {})
        cur_px = sym_data.get("cur_px", np.nan)
        avg_px_symbol = sym_data.get("avg_px", np.nan)

        # Compute lot-level P/L: (current price - symbol average price) * remaining qty
        pnl = np.nan
        if pd.notna(cur_px) and pd.notna(avg_px_symbol):
            pnl = (cur_px - avg_px_symbol) * qty_rem

        final_lots_data.append({
            'Parent ID': oid,
            'Symbol': symbol,
            'Buy Date': buy_dt,
            'Buy Qty': qty_rem,
            'Buy Price': buy_price,
            'Buy_Value': qty_rem * buy_price,
            'Take_Profit_Price': tp_price,
            'Stop_Loss_Price': sl_price,
            'Holding_Period': (now - buy_dt).days, 
            'Source': source_note,
            'Current_Price': cur_px,
            'Current Market Value': (qty_rem * cur_px) if pd.notna(cur_px) else np.nan,
            'Profit/Loss': pnl,
            '_avg_px_symbol': avg_px_symbol
        })

    # === 7. ORPHAN MATCHING (FIXED VERSION) ===
    if not orders_df.empty:
        active_statuses = [
            'new', 'held', 'accepted', 'pending_new', 'open', 'calculated'
        ]
        type_col = 'type' if 'type' in orders_df.columns else 'order_type'

        potential_orphans = orders_df[
            (orders_df['status'].isin(active_statuses)) &
            (orders_df['side'] == 'sell') &
            (orders_df[type_col].isin(['stop', 'stop_limit', 'limit']))
        ]

        unprotected_by_symbol = {}
        for item in final_lots_data:
            if (item['Stop_Loss_Price'] is None or
                    item['Take_Profit_Price'] is None):
                sym = item['Symbol']
                if sym not in unprotected_by_symbol:
                    unprotected_by_symbol[sym] = []
                unprotected_by_symbol[sym].append(item)

        for sym, items in unprotected_by_symbol.items():
            total_exposed_qty = sum(x['Buy Qty'] for x in items)
            sym_orders = potential_orphans[potential_orphans['symbol'] == sym]

            found_tp = None
            found_sl = None

            for _, o_row in sym_orders.iterrows():
                o_qty = float(o_row.get('qty', 0))
                if abs(o_qty - total_exposed_qty) < 0.1:
                    current_type = o_row.get(type_col)
                    if current_type == 'limit' and found_tp is None:
                        found_tp = float(o_row.get('limit_price'))
                    elif (current_type in ['stop', 'stop_limit'] and
                          found_sl is None):
                        found_sl = float(o_row.get('stop_price'))

            if found_tp is not None or found_sl is not None:
                for item in items:
                    updated = False
                    if (found_tp is not None and
                            item['Take_Profit_Price'] is None):
                        item['Take_Profit_Price'] = found_tp
                        updated = True

                    if (found_sl is not None and
                            item['Stop_Loss_Price'] is None):
                        item['Stop_Loss_Price'] = found_sl
                        updated = True

                    if updated:
                        item['Source'] = 'Orphan Match'

    if not final_lots_data:
        return pd.DataFrame(columns=empty_cols)

    pos_df = pd.DataFrame(final_lots_data)

    # === 8. DERIVED COLUMNS (IMPROVED LOGIC) ===

    # 1. Calculate price difference (P/L per share)
    diff_cur = pos_df["Current_Price"] - pos_df["Buy Price"]

    # 2. Calculate distances to targets
    # Distance to TP (positive value)
    dist_tp = pos_df["Take_Profit_Price"] - pos_df["Buy Price"]
    # Distance to SL (positive value: Buy - SL, e.g., 100 - 90 = 10)
    dist_sl = pos_df["Buy Price"] - pos_df["Stop_Loss_Price"]

    # 3. Smart TP_reach calculation
    # If profitable (>=0) -> (Current - Buy) / (TP - Buy)
    # If loss (<0)        -> (Current - Buy) / (Buy - SL) -> yields negative %

    # Conditions
    cond_profit = (diff_cur >= 0) & (dist_tp > 0)
    cond_loss = (diff_cur < 0) & (dist_sl > 0)

    pos_df["TP_reach, %"] = np.select(
        [
            cond_profit,  # If profitable and TP exists
            cond_loss     # If loss and SL exists
        ],
        [
            (diff_cur / dist_tp) * 100.0,  # % progress toward TP
            (diff_cur / dist_sl) * 100.0   # % progress toward SL (negative)
        ],
        default=np.nan  # If data is missing (e.g., no SL), set to NaN
    ).round(2)

    # Break Even (BrE) Check (Remains unchanged)
    def _be_price(p):
        return p  # Placeholder if no external fee logic

    def _be_price(p):
        return p  # Placeholder if no external fee logic

    pos_df["BrE"] = pos_df.apply(
        lambda r: int(
            pd.notna(r["Stop_Loss_Price"]) and
            np.isclose(
                r["Stop_Loss_Price"],
                _be_price(r["Buy Price"]),
                atol=0.01,  # CHANGED: Allow 0.01 (1 cent) difference
                rtol=0
            )
        ),
        axis=1,
    )

    # Format Buy Date
    pos_df["Buy Date"] = (
        pd.to_datetime(pos_df["Buy Date"], utc=True)
        .dt.strftime("%Y-%m-%d %H:%M:%S")
    )

    # Final Sort
    pos_df = pos_df.sort_values(
        by=['Symbol', 'Buy Date'], ascending=[True, False]
    ).reset_index(drop=True)

    return pos_df


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
            "side": "long",
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
                        "side": "long",
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


import pandas as pd
import numpy as np
import warnings
from typing import Optional

def calculate_metrics(
    trades_df: pd.DataFrame,
    open_trades_df: Optional[pd.DataFrame] = None,
    activities_df: Optional[pd.DataFrame] = None,
    value_weighted: bool = True
) -> pd.DataFrame:
    """
    Final Financial Report.
    """
    
    # Initialize all variables to safe defaults to avoid UnboundLocalError
    # This ensures that even if trades_df is empty, the final report generation works.
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

    # ==========================================
    # 1. PROCESS CLOSED TRADES
    # ==========================================
    if not trades_df.empty:
        df = trades_df.copy()

        # Conversions
        if 'pnl_cash_fifo' not in df.columns: df['pnl_cash_fifo'] = 0.0
        cols_num = ['qty', 'entry_price', 'exit_price', 'pnl_cash', 'pnl_cash_fifo']
        for c in cols_num:
            if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
        for c in ['entry_time', 'exit_time']:
            if c in df.columns: df[c] = pd.to_datetime(df[c], utc=True)

        # Identify Exit Class
        def identify_exit_class(val):
            s = str(val).lower()
            if 'limit' in s: return 'TP'
            if 'stop' in s: return 'SL'
            return 'MANUAL'

        if 'exit_type' in df.columns:
            df['exit_class_raw'] = df['exit_type'].apply(identify_exit_class)
        else:
            df['exit_class_raw'] = 'MANUAL'

        # COLLAPSE BY LOT
        if 'entry_lot_id' in df.columns:
            def aggregate_lot(sub):
                qty_total = sub['qty'].sum()
                type_counts = sub.groupby('exit_class_raw')['qty'].sum().sort_values(ascending=False)
                major_type = type_counts.index[0] if not type_counts.empty else 'MANUAL'
                
                w_entry = np.average(sub['entry_price'], weights=sub['qty']) if qty_total > 0 else 0
                w_exit = np.average(sub['exit_price'], weights=sub['qty']) if qty_total > 0 else 0
                
                return pd.Series({
                    'qty': qty_total,
                    'entry_price': w_entry,
                    'exit_price': w_exit,
                    'pnl_cash': sub['pnl_cash'].sum(),
                    'pnl_cash_fifo': sub['pnl_cash_fifo'].sum(),
                    'entry_time': sub['entry_time'].min(),
                    'exit_time': sub['exit_time'].max(),
                    'exit_class': major_type
                })
            
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=DeprecationWarning)
                # Fix for FutureWarning in groupby
                calc_cols = ['qty', 'entry_price', 'exit_price', 'pnl_cash', 'pnl_cash_fifo', 'entry_time', 'exit_time', 'exit_class_raw']
                collapsed = df.groupby('entry_lot_id')[calc_cols].apply(aggregate_lot).reset_index()
        else:
            collapsed = df.copy()
            collapsed.rename(columns={'exit_class_raw': 'exit_class'}, inplace=True)

        # Set count
        closed_trades_count = len(collapsed)

        # Volumes & Averages
        collapsed['duration_days'] = (collapsed['exit_time'] - collapsed['entry_time']).dt.total_seconds() / 86400.0
        # Calculate volume for weighting
        collapsed['invested_vol'] = collapsed['qty'] * collapsed['entry_price']

        tp_wins = collapsed[(collapsed['exit_class'] == 'TP') & (collapsed['pnl_cash'] > 0)]
        sl_losses = collapsed[(collapsed['exit_class'] == 'SL') & (collapsed['pnl_cash'] < 0)]

        def get_w_avg(data, col):
            if data.empty or data['invested_vol'].sum() == 0: return 0.0
            if value_weighted:
                return np.average(data[col], weights=data['invested_vol'])
            return data[col].mean()

        # Update variables with calculated values
        avg_take = get_w_avg(tp_wins, 'pnl_cash')
        avg_stop = abs(get_w_avg(sl_losses, 'pnl_cash'))
        avg_hold_take = get_w_avg(tp_wins, 'duration_days')
        avg_hold_stop = get_w_avg(sl_losses, 'duration_days')
        
        rr_ratio = (avg_take / avg_stop) if avg_stop > 0 else 0.0
        breakeven = (100 / (1 + rr_ratio)) if rr_ratio > 0 else 0.0
        denom = len(tp_wins) + len(sl_losses)
        pl_ratio = (len(tp_wins) / denom * 100) if denom > 0 else 0.0

        # Volume (Closed Portion)
        total_buy_closed = (collapsed['qty'] * collapsed['entry_price']).sum()
        total_sell_closed = (collapsed['qty'] * collapsed['exit_price']).sum()
        
        total_pnl_lot = collapsed['pnl_cash'].sum()
        total_pnl_fifo = collapsed['pnl_cash_fifo'].sum()

    # ==========================================
    # 2. ANALYZE OPEN TRADES
    # ==========================================
    total_unrealized_pnl = 0.0
    count_open = 0
    total_open_val = 0.0
    total_buy_open = 0.0

    if open_trades_df is not None and not open_trades_df.empty:
        # Check standard columns
        pnl_col = next((col for col in ['Profit/Loss', 'Unrealized PnL', 'pnl'] if col in open_trades_df.columns), None)
        val_col = next((col for col in ['Current Market Value', 'Market Value', 'value'] if col in open_trades_df.columns), None)
        price_col = next((col for col in ['Buy Price', 'entry_price'] if col in open_trades_df.columns), None)
        qty_col = next((col for col in ['Buy Qty', 'qty'] if col in open_trades_df.columns), None)

        if pnl_col:
            total_unrealized_pnl = pd.to_numeric(open_trades_df[pnl_col], errors='coerce').fillna(0).sum()
        
        if val_col:
            total_open_val = pd.to_numeric(open_trades_df[val_col], errors='coerce').fillna(0).sum()
            
        if price_col and qty_col:
            op_p = pd.to_numeric(open_trades_df[price_col], errors='coerce').fillna(0)
            op_q = pd.to_numeric(open_trades_df[qty_col], errors='coerce').fillna(0)
            total_buy_open = (op_p * op_q).sum()

        count_open = len(open_trades_df)

    # ==========================================
    # 3. ANALYZE ACTIVITIES
    # ==========================================
    total_fees = 0.0
    total_divs = 0.0
    total_int = 0.0

    if activities_df is not None and not activities_df.empty:
        act = activities_df.copy()
        if 'net_amount' not in act.columns and 'qty' in act.columns: act['net_amount'] = act['qty']
        act['net_amount'] = pd.to_numeric(act['net_amount'], errors='coerce').fillna(0)
        
        total_fees = act[act['activity_type'] == 'FEE']['net_amount'].sum()
        total_divs = act[act['activity_type'].isin(['DIV', 'DIVNRA'])]['net_amount'].sum()
        total_int = act[act['activity_type'] == 'INT']['net_amount'].sum()

    # --- FINAL CALCULATIONS ---
    net_realized_fifo = total_pnl_fifo + total_fees + total_divs + total_int
    total_account_result = net_realized_fifo + total_unrealized_pnl
    grand_total_buy = total_buy_closed + total_buy_open

    # ==========================================
    # 4. CONSTRUCT RESULT
    # ==========================================
    result = {
        # --- 1. ACCOUNT OVERVIEW ---
        "TOTAL ACCOUNT RESULT (Equity)": round(total_account_result, 2),
        "NET REALIZED CASH (FIFO)": round(net_realized_fifo, 2),
        "Unrealized P/L (Open)": round(total_unrealized_pnl, 2),
        "Current Open Value": f"{total_open_val:,.2f}",
        
        # --- 2. PERFORMANCE (RATIOS) ---
        "------------------": "",
        "Breakeven %": f"%{breakeven:.1f}" if not pd.isna(breakeven) else "N/A",
        "Win Rate %": f"%{pl_ratio:.1f}" if not pd.isna(pl_ratio) else "N/A",
        "Risk-Reward Ratio": round(rr_ratio, 2) if not pd.isna(rr_ratio) else "N/A",
        
        # --- 3. AVERAGE METRICS ---
        "Mean Take ($)": round(avg_take, 2),
        "Mean Stop ($)": round(avg_stop, 2),
        "Mean Holding Take (Days)": round(avg_hold_take, 2),
        "Mean Holding Stop (Days)": round(avg_hold_stop, 2),
        
        # --- 4. PnL BREAKDOWN ---
        "------------------ ": "",
        "Realized P/L (Broker/FIFO)": round(total_pnl_fifo, 2),
        "Realized P/L (Strategy Only)": round(total_pnl_lot, 2),
        "Total Fees": round(total_fees, 2),
        "Net Dividends": round(total_divs, 2),
        "Net Interest": round(total_int, 2),
        
        # --- 5. VOLUMES ---
        "Total Buy Volume": f"{grand_total_buy:,.2f}",
        "Total Sell Volume": f"{total_sell_closed:,.2f}",
        "Closed Trades (n)": closed_trades_count,
        "Open Trades (n)": count_open,
    }

    return pd.DataFrame([result]).T.rename(columns={0: 'Value'})
