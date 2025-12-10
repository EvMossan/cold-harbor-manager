import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Iterable, Tuple

# Third-party
import requests
import numpy as np
import pandas as pd
import pandas_market_calendars as mcal
from alpaca_trade_api.rest import REST
from collections import deque


def fetch_all_orders(api: REST, days_back: int = 365) -> pd.DataFrame:
    """Walk Alpaca orders in fixed windows, flatten parents with legs,
    and normalize common numeric/timestamp columns."""
    window_len = timedelta(days=5)
    end_date = pd.Timestamp.now(tz=timezone.utc) + timedelta(days=1)
    start_date = end_date - timedelta(days=days_back)
    current_start = start_date
    raw_orders: list[dict] = []
    seen_ids: set[str] = set()

    def _append(raw: dict) -> None:
        oid = str(raw.get("id") or "")
        if not oid or oid in seen_ids:
            return
        seen_ids.add(oid)
        raw_orders.append(raw)

    while current_start < end_date:
        current_end = min(current_start + window_len, end_date)
        try:
            batch = api.list_orders(
                status="all",
                limit=500,
                nested=True,
                direction="desc",
                after=current_start.isoformat(),
                until=current_end.isoformat(),
            )
        except Exception:
            current_start = current_end
            continue
        for order in batch:
            raw = getattr(order, "_raw", order)
            if isinstance(raw, dict):
                _append(raw)
        current_start = current_end

    if not raw_orders:
        return pd.DataFrame()

    def _pick(d: dict, *keys: str) -> Any:
        for key in keys:
            if key in d and d[key] is not None:
                return d[key]
        return None

    flat_rows: list[dict] = []
    for parent in raw_orders:
        parent_id = str(parent.get("id") or "")
        parent_row = dict(parent)
        if "parent_id" in parent_row:
            parent_row["parent_id"] = None
        parent_row["order_type"] = _pick(parent_row, "order_type", "type")
        flat_rows.append(parent_row)

        legs = parent.get("legs") or []
        if isinstance(legs, list):
            for leg in legs:
                child = dict(leg)
                child["parent_id"] = parent_id or None
                child["order_type"] = _pick(child, "order_type", "type")
                flat_rows.append(child)

    df = pd.DataFrame(flat_rows)
    if df.empty:
        return df

    if "parent_id" not in df.columns:
        df["parent_id"] = pd.NA

    date_cols = ["created_at", "updated_at", "filled_at", "submitted_at"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    num_cols = ["qty", "filled_qty", "limit_price", "stop_price"]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "created_at" in df.columns:
        df = df.sort_values("created_at", ascending=False)
    return df


def closed_trades_fifo_from_orders(
    orders: pd.DataFrame,
    debug: bool = False,
    debug_symbols: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """
    Build per-lot closed trades from an *already parent-linked* flat
    orders table (parents + legs), using parent_id to ensure that each
    exit leg closes its own parent lot.

    Matching priority for SELL legs:
      (1) parent_id match (strict)
      (2) unique qty match (only if exactly one open lot has the same
          original qty as SELL qty and can fully absorb it)
      (3) FIFO fallback

    Returns one row per matched slice with:
      symbol · side · qty · entry_time · entry_price · exit_time ·
      exit_price · exit_type · pnl_cash · pnl_pct · duration_sec ·
      entry_lot_id · exit_order_id · exit_parent_id
    """
    # --- helpers ------------------------------------------------------
    def _d(msg: str) -> None:
        if debug:
            print(f"[closed_trades_fifo_from_orders] {msg}")

    dbg_set = {s.strip().upper() for s in (debug_symbols or [])}

    def _dbg(sym: str, msg: str) -> None:
        if debug and (not dbg_set or sym.upper() in dbg_set):
            print(f"[closed_trades_fifo_from_orders] {sym}: {msg}")

    def _exit_type(order_type: Any) -> str:
        s = str(order_type or "").lower()
        if "stop" in s:
            return "SL"
        if "limit" in s:
            return "TP"
        return "MANUAL"

    BUY_SIDES = {"buy", "buy_to_open"}
    SELL_SIDES = {"sell", "sell_short", "sell_to_close"}

    # --- guards -------------------------------------------------------
    if orders is None or len(orders) == 0:
        return pd.DataFrame(
            columns=[
                "symbol",
                "side",
                "qty",
                "entry_time",
                "entry_price",
                "exit_time",
                "exit_price",
                "exit_type",
                "pnl_cash",
                "pnl_pct",
                "duration_sec",
                "entry_lot_id",
                "exit_order_id",
                "exit_parent_id",
            ]
        )

    # --- normalize minimal fields we need -----------------------------
    df = orders.copy()

    # Dtypes and canonical forms
    for c in ("filled_qty", "filled_avg_price", "qty", "limit_price", "stop_price"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    for c in ("status", "side", "order_type", "symbol"):
        if c in df.columns:
            df[c] = df[c].astype(str).str.lower()

    for c in ("filled_at", "updated_at", "submitted_at", "created_at"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], utc=True, errors="coerce")

    # 'exec_ts' may already exist from fetcher; recompute defensively
    ts = df.get("filled_at")
    for alt in ("updated_at", "submitted_at", "created_at"):
        if alt in df.columns:
            ts = ts.where(ts.notna(), df[alt]) if ts is not None else df[alt]
    df["exec_ts"] = pd.to_datetime(ts, utc=True, errors="coerce")

    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.upper()

    # Unique orders
    if "id" in df.columns:
        df = df.drop_duplicates(subset=["id"], keep="last")

    # --- restrict to actual fills ------------------------------------
    fills = df[
        df["side"].isin(BUY_SIDES | SELL_SIDES)
        & df["filled_qty"].gt(0)
        & df["filled_avg_price"].notna()
        & df["exec_ts"].notna()
    ].copy()

    if fills.empty:
        return pd.DataFrame(
            columns=[
                "symbol",
                "side",
                "qty",
                "entry_time",
                "entry_price",
                "exit_time",
                "exit_price",
                "exit_type",
                "pnl_cash",
                "pnl_pct",
                "duration_sec",
                "entry_lot_id",
                "exit_order_id",
                "exit_parent_id",
            ]
        )

    # Sort chronologically; buys before sells at the same ts
    fills = fills.sort_values(["symbol", "exec_ts", "side"]).reset_index(drop=True)

    # Per-symbol open lots; a lot is a parent BUY
    # lot dict: {"qty", "px", "t", "lot_id", "orig_qty"}
    lots_by_sym: Dict[str, List[Dict[str, Any]]] = {}

    closed_rows: List[Dict[str, Any]] = []

    # --- main pass ----------------------------------------------------
    for _, r in fills.iterrows():
        sym = r["symbol"]
        side = r["side"]
        qty = float(r["filled_qty"]) if pd.notna(r["filled_qty"]) else 0.0
        px = float(r["filled_avg_price"]) if pd.notna(r["filled_avg_price"]) else np.nan
        t = pd.to_datetime(r["exec_ts"], utc=True)
        oid = str(r.get("id") or "")
        pid_val = r.get("parent_id")
        parent_id = "" if (pd.isna(pid_val) or pid_val is None) else str(pid_val)

        lots_by_sym.setdefault(sym, [])

        if side in BUY_SIDES:
            # A BUY with no parent_id is a parent lot
            if parent_id:
                # Defensive: ignore buy-legs if any (rare); treat only parent buy
                _dbg(sym, f"ignoring BUY leg id={oid} with parent_id={parent_id}")
                continue
            lot_id = oid  # parent id is the entry lot id
            lots_by_sym[sym].append(
                {"qty": qty, "px": px, "t": t, "lot_id": lot_id, "orig_qty": qty}
            )
            _dbg(
                sym,
                f"BUY parent lot added lot_id={lot_id} qty={qty:.6f} @ {px:.6f} "
                f"t={t.isoformat()} open_lots={len(lots_by_sym[sym])}",
            )
            continue

        # SELL leg
        remaining = qty
        etype = _exit_type(r.get("order_type"))

        q = lots_by_sym[sym]

        def _consume(idx: int, amount: float) -> float:
            """Consume amount from lot index idx and append a closed slice."""
            lot = q[idx]
            take = min(lot["qty"], amount)
            entry_px = float(lot["px"])
            pnl_cash = (px - entry_px) * take
            duration = (t - lot["t"]).total_seconds()

            closed_rows.append(
                {
                    "symbol": sym,
                    "side": "long",
                    "entry_lot_id": lot.get("lot_id"),
                    "qty": take,
                    "entry_time": lot["t"],
                    "entry_price": entry_px,
                    "exit_time": t,
                    "exit_price": px,
                    "exit_type": etype,
                    "pnl_cash": pnl_cash,
                    "pnl_pct": (
                        (pnl_cash / (entry_px * take) * 100.0)
                        if entry_px > 0 and take > 0
                        else np.nan
                    ),
                    "duration_sec": duration,
                    "exit_order_id": oid,
                    "exit_parent_id": parent_id,
                }
            )

            lot["qty"] -= take
            if lot["qty"] <= 1e-9:
                q.pop(idx)
            return take

        # (1) Strict parent_id match
        if parent_id and remaining > 1e-9:
            idx_parent = next(
                (i for i, L in enumerate(q)
                 if L.get("lot_id") == parent_id and L["qty"] > 1e-9),
                -1,
            )
            if idx_parent >= 0:
                _dbg(sym, f"SELL parent-match exit_id={oid} -> lot={parent_id}")
                consumed = _consume(idx_parent, remaining)
                remaining -= consumed

        # (2) Unique equal-qty match (only if exactly one candidate)
        if (not parent_id) and remaining > 1e-9 and q:
            cand = [
                i for i, L in enumerate(q)
                if abs(float(L.get("orig_qty", 0.0)) - remaining) < 1e-9
                and L["qty"] >= remaining
            ]
            if len(cand) == 1:
                _dbg(
                    sym,
                    f"SELL unique qty-match exit_id={oid} qty={remaining:.6f} "
                    f"-> lot={q[cand[0]].get('lot_id')}",
                )
                consumed = _consume(cand[0], remaining)
                remaining -= consumed

        # (3) FIFO fallback
        while remaining > 1e-9 and q:
            _dbg(sym, f"SELL FIFO exit_id={oid} qty={remaining:.6f}")
            consumed = _consume(0, remaining)
            remaining -= consumed

        if remaining > 1e-9:
            _dbg(
                sym,
                f"WARNING: SELL not fully matched; remaining={remaining:.6f} "
                f"(no open lots). Data gap or prior flat.",
            )

    if not closed_rows:
        return pd.DataFrame(
            columns=[
                "symbol",
                "side",
                "qty",
                "entry_time",
                "entry_price",
                "exit_time",
                "exit_price",
                "exit_type",
                "pnl_cash",
                "pnl_pct",
                "duration_sec",
                "entry_lot_id",
                "exit_order_id",
                "exit_parent_id",
            ]
        )

    out = (
        pd.DataFrame(closed_rows)
        .sort_values("exit_time", ascending=False)
        .reset_index(drop=True)
    )
    return out


def realized_pnl_fifo_from_orders(orders: pd.DataFrame) -> float:
    """
    Realized P&L computed via **FIFO** using the order stream (parents +
    legs).

    Parameters
    ----------
    orders : pd.DataFrame
        Output of `fetch_all_orders(...)`.

    Returns
    -------
    float
        Sum of FIFO matched trade P&L (in cash), rounded to 2 decimals.
    """
    trades = closed_trades_fifo_from_orders(orders)
    if trades.empty:
        return 0.0
    return float(trades["pnl_cash"].sum().round(2))

BUY_SIDES  = {"buy", "buy_to_open"}
SELL_SIDES = {"sell", "sell_short", "sell_to_close"}
LIVE_STAT  = {"new", "accepted", "open", "held", "partially_filled"}
STOP_TYPES = {"stop", "stop_limit"}


def _expand_replacement_chain(orders_flat: pd.DataFrame, seed_ids: List[str]) -> set:
    """
    BFS-расширение: стартуем от набора seed_ids (id ног родителя плюс их replaces/replaced_by),
    добавляем все ордера, которые связаны через поля id/replaces/replaced_by.
    Возвращает множество всех id в цепочке (включая актуальные).
    """
    if orders_flat is None or orders_flat.empty or not seed_ids:
        return set()

    df = orders_flat.copy()
    # Приводим к строкам, NA-safe
    for c in ("id", "replaces", "replaced_by", "client_order_id"):
        if c in df.columns:
            df[c] = df[c].astype(str)

    seeds = {s for s in (str(x or "") for x in seed_ids) if s}
    seen: set = set()
    q = deque(seeds)

    while q:
        cur = q.popleft()
        if not cur or cur in seen:
            continue
        seen.add(cur)

        # Любые строки, где cur фигурирует как id/replaces/replaced_by/client_order_id
        rows = df[
            (df["id"] == cur)
            | (df.get("replaces", "") == cur)
            | (df.get("replaced_by", "") == cur)
            | (df.get("client_order_id", "") == cur)
        ]

        if rows.empty:
            continue

        # В граф добавляем все связанные поля
        for _, rr in rows.iterrows():
            for k in ("id", "replaces", "replaced_by", "client_order_id"):
                v = str(rr.get(k) or "")
                if v and v not in seen:
                    q.append(v)

    # Из seen нам интересны именно реальные order.id,
    # поэтому дополнительно вытащим те, что существуют как id в df
    chain_ids = set(df[df["id"].isin(list(seen))]["id"].unique())
    return chain_ids

def _live_tp_sl_from_chain(orders_flat: pd.DataFrame, chain_ids: set) -> tuple[float, float, str, str]:
    """
    Ищет LIVE sell-ордера в пределах replacement-цепочки.
    limit → TP, stop/stop_limit → SL. Берём самый «свежий» по updated/submitted/created.
    """
    tp = np.nan; sl = np.nan; tp_src = ""; sl_src = ""
    if not chain_ids or orders_flat is None or orders_flat.empty:
        return tp, sl, tp_src, sl_src

    df = orders_flat.copy()
    for c in ("status", "order_type", "side"):
        if c in df.columns:
            df[c] = df[c].astype(str).str.lower()
    for c in ("updated_at", "submitted_at", "created_at"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], utc=True, errors="coerce")

    live = df[
        df["id"].astype(str).isin(chain_ids)
        & df["side"].isin(SELL_SIDES)
        & df["status"].isin(LIVE_STAT)
    ].copy()

    if live.empty:
        return tp, sl, tp_src, sl_src

    live["_last_ts"] = live[["updated_at","submitted_at","created_at"]].max(axis=1)

    cand_tp = live[live["order_type"] == "limit"].sort_values("_last_ts", ascending=False)
    cand_sl = live[live["order_type"].isin(STOP_TYPES)].sort_values("_last_ts", ascending=False)

    if not cand_tp.empty and pd.notna(cand_tp.iloc[0].get("limit_price")):
        tp = float(cand_tp.iloc[0]["limit_price"]); tp_src = "chain"
    if not cand_sl.empty and pd.notna(cand_sl.iloc[0].get("stop_price")):
        sl = float(cand_sl.iloc[0]["stop_price"]);  sl_src = "chain"

    return tp, sl, tp_src, sl_src


def _now_utc() -> pd.Timestamp:
    """
    Return a tz-aware UTC Timestamp robustly across pandas versions.
    pandas>=2 returns tz-aware for utcnow(); older versions return naive.
    """
    ts = pd.Timestamp.utcnow()
    # pandas Timestamps expose .tz (alias of .tzinfo)
    return ts if getattr(ts, "tz", None) is not None else ts.tz_localize("UTC")


def _norm_orders_for_open(df: pd.DataFrame) -> pd.DataFrame:
    """Minimal normalization for open-positions logic."""
    x = df.copy()

    # Parent link column
    if "parent_id" not in x.columns and "parent_order_id" in x.columns:
        x["parent_id"] = x["parent_order_id"]
    elif "parent_id" not in x.columns:
        x["parent_id"] = pd.NA

    # Dtypes
    for c in ("filled_qty", "filled_avg_price", "limit_price", "stop_price", "qty"):
        if c in x.columns:
            x[c] = pd.to_numeric(x[c], errors="coerce")

    for c in ("status", "side", "order_type", "order_class", "symbol"):
        if c in x.columns:
            x[c] = x[c].astype(str).str.lower()

    # Timestamps → UTC and exec_ts
    for c in ("filled_at", "updated_at", "submitted_at", "created_at",
              "canceled_at", "replaced_at", "expires_at"):
        if c in x.columns:
            x[c] = pd.to_datetime(x[c], utc=True, errors="coerce")

    ts = x.get("filled_at")
    for alt in ("updated_at", "submitted_at", "created_at"):
        if alt in x.columns:
            ts = ts.where(ts.notna(), x[alt]) if ts is not None else x[alt]
    x["exec_ts"] = pd.to_datetime(ts, utc=True, errors="coerce")

    # Canonical symbol case
    if "symbol" in x.columns:
        x["symbol"] = x["symbol"].astype(str).str.lower()

    # Unique orders
    if "id" in x.columns:
        x = x.drop_duplicates(subset=["id"], keep="last")
    return x


def _time_window_from_orders(df: pd.DataFrame) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """Infer [start, end] from submitted/created; fallback: last 30d."""
    start = pd.NaT
    end   = pd.NaT
    for c in ("submitted_at", "created_at"):
        if c in df.columns and df[c].notna().any():
            s = pd.to_datetime(df[c], utc=True, errors="coerce")
            start = s.min() if pd.isna(start) else min(start, s.min())
            end   = s.max() if pd.isna(end)   else max(end,   s.max())
    if pd.isna(start) or pd.isna(end):
        end = _now_utc()
        start = end - pd.Timedelta(days=30)
    return start, end


def _build_leg_to_parent_map(nested_df: pd.DataFrame) -> Dict[str, str]:
    """
    From nested parents, collect all child ids (id, client_order_id,
    replaced_by, replaces) → parent_id.
    """
    m: Dict[str, str] = {}
    if nested_df is None or nested_df.empty:
        return m

    z = nested_df.copy()
    for c in ("order_class", "side"):
        if c in z.columns:
            z[c] = z[c].astype(str).str.lower()

    parents = z[z["order_class"].astype(str).str.lower().eq("bracket")
                & z["side"].astype(str).str.lower().isin(BUY_SIDES)]

    for _, pr in parents.iterrows():
        pid = str(pr.get("id") or "")
        legs = pr.get("legs") or []
        if not isinstance(legs, list):
            continue
        for L in legs:
            for k in ("id", "client_order_id", "replaced_by", "replaces"):
                v = str(L.get(k) or "")
                if v:
                    m[v] = pid
    return m


def _fill_missing_parent_ids_with_legs(orders_flat: pd.DataFrame,
                                       leg2parent: Dict[str, str]) -> pd.DataFrame:
    """
    For SELL rows with NA parent_id, infer the parent via leg ids.
    NA-safe (does not use 'or' on pd.NA).
    """
    if not leg2parent:
        return orders_flat
    df = orders_flat.copy()

    mask_sell = df["side"].isin(SELL_SIDES)
    mask_na   = df["parent_id"].isna()
    idx = df[mask_sell & mask_na].index
    if len(idx) == 0:
        return df

    def _infer_parent(row: pd.Series) -> str:
        for k in ("id", "client_order_id", "replaced_by", "replaces"):
            v = str(row.get(k) or "")
            if v and v in leg2parent:
                return leg2parent[v]
        return ""

    inferred = df.loc[idx].apply(_infer_parent, axis=1)
    for i, pid in inferred.items():
        if pid:
            df.at[i, "parent_id"] = pid
    return df


def _fifo_owner_first(df_symbol: pd.DataFrame,
                      parents_symbol: pd.DataFrame,
                      parent_child_ids: Dict[str, set]) -> Dict[str, float]:
    """
    Subtract filled SELLs with priority:
      (1) owner-first: if SELL id belongs to a parent → subtract there;
      (2) remainder → FIFO across remaining open lots.

    Returns {parent_id: qty_remaining}
    """
    # Build list of open parent lots (in time order)
    lots = []
    for _, p in parents_symbol.sort_values("exec_ts").iterrows():
        lots.append({"parent_id": str(p.get("id")), "qty_rem": float(p.get("filled_qty") or 0.0)})

    # Flatten child→parent id index
    id_to_parent: Dict[str, str] = {}
    for pid, ids in parent_child_ids.items():
        for cid in ids:
            id_to_parent[cid] = pid

    sells = df_symbol[(df_symbol["side"].isin(SELL_SIDES))
                      & (df_symbol["status"].isin({"filled", "partially_filled"}))
                      & df_symbol["filled_qty"].gt(0)
                      & df_symbol["exec_ts"].notna()] \
            .sort_values("exec_ts")

    for _, s in sells.iterrows():
        q = float(s.get("filled_qty") or 0.0)
        if q <= 0:
            continue

        sid = str(s.get("id") or "")
        owner = id_to_parent.get(sid, "")
        # NA-safe parent_id fallback
        pidv = s.get("parent_id")
        if not owner:
            owner = "" if pd.isna(pidv) else str(pidv)

        # (1) owner-first
        if owner:
            for lot in lots:
                if q <= 0:
                    break
                if lot["parent_id"] == owner and lot["qty_rem"] > 0:
                    take = min(lot["qty_rem"], q)
                    lot["qty_rem"] -= take
                    q -= take
                    break

        # (2) FIFO
        if q > 0:
            for lot in lots:
                if q <= 0:
                    break
                if lot["qty_rem"] <= 0:
                    continue
                take = min(lot["qty_rem"], q)
                lot["qty_rem"] -= take
                q -= take

    return {lot["parent_id"]: max(0.0, lot["qty_rem"]) for lot in lots}


def _live_tp_sl_from_legs(legs: List[Dict[str, Any]]) -> Tuple[float, float]:
    """Extract (tp_limit, sl_stop) from LIVE legs; NaN if absent."""
    tp = np.nan
    sl = np.nan
    if not isinstance(legs, list):
        return (tp, sl)
    for L in legs:
        st = str(L.get("status") or "").lower()
        typ = str(L.get("type") or L.get("order_type") or "").lower()
        if st not in LIVE_STAT:
            continue
        if typ == "limit":
            val = L.get("limit_price")
            if val is not None:
                tp = float(val)
        if typ in STOP_TYPES:
            val = L.get("stop_price")
            if val is not None:
                sl = float(val)
    return (tp, sl)


def _be_price(entry: float) -> float:
    tick = 0.0001 if entry < 1 else 0.01
    gap = max(0.01, tick)
    return round(entry - gap, 4 if tick < 0.01 else 2)


def build_open_positions_df(
    orders_df: pd.DataFrame,
    api,
    debug: bool = False,
    debug_symbols: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """
    Build open positions per *parent buy lot* with deterministic TP/SL
    linking. This version scopes replacement-chain lookups to the
    current symbol to avoid cross-symbol leakage of TP/SL.

    Key changes vs. previous version:
      • children_flat is taken from df_sym (not from global df)
      • replacement-chain expansion is performed on df_sym
      • live TP/SL extraction from chain is performed on df_sym

    Everything else (owner-first netting, reconciliation to broker
    snapshot, derived columns) is unchanged.
    """
    def _D(sym: Optional[str], msg: str) -> None:
        if not debug:
            return
        if debug_symbols:
            want = {s.strip().lower() for s in debug_symbols}
            if sym is not None and sym.lower() not in want:
                return
        prefix = f"[open_pos{':' + sym.upper() if sym else ''}]"
        print(prefix, msg)

    empty_cols = [
        "Parent ID", "Symbol", "Buy Date", "Buy Qty", "Buy Price", "Buy_Value",
        "Take_Profit_Price", "Stop_Loss_Price", "Holding_Period",
        "Current_Price", "Current Market Value", "Profit/Loss",
        "_avg_px_symbol", "TP_reach, %", "BrE",
    ]
    if orders_df is None or orders_df.empty:
        _D(None, "orders_df is empty → return empty schema")
        return pd.DataFrame(columns=empty_cols)

    # 1) Normalize flat orders (parents + legs)
    df = _norm_orders_for_open(orders_df)
    _D(None, f"normalized orders: rows={len(df)}")

    # 2) Time window inference (unchanged)
    start_ts, end_ts = _time_window_from_orders(df)
    _D(None, f"time window inferred: {start_ts} .. {end_ts}")

    # Fetch nested parents (for leg→parent map and owner-first netting)
    try:
        nested_df = fetch_all_orders(
            api,
            start=start_ts.isoformat(),
            end=end_ts.isoformat(),
            chunk_size=500,
            # nested=True,
            show_progress=False,
        )
    except Exception as e:
        _D(None, f"nested fetch failed: {e}")
        nested_df = pd.DataFrame()

    _D(None, f"nested_df rows={len(nested_df)} (with legs)")

    leg2parent = _build_leg_to_parent_map(nested_df)
    _D(None, f"leg2parent size={len(leg2parent)}")

    # 3) Fill missing SELL.parent_id via nested legs (NA-safe)
    df = _fill_missing_parent_ids_with_legs(df, leg2parent)
    _D(None, "filled SELL.parent_id from leg-map where possible")

    # 4) Parent buy lots
    buy_parents = df[
        df["parent_id"].isna()
        & df["side"].isin(BUY_SIDES)
        & df["filled_qty"].gt(0)
        & df["filled_avg_price"].notna()
        & df["exec_ts"].notna()
    ].sort_values(["symbol", "exec_ts"])

    if buy_parents.empty:
        return pd.DataFrame(columns=empty_cols)

    # Child-ids per parent for owner-first netting (from nested)
    parent_child_ids: Dict[str, set] = {}
    if not nested_df.empty:
        nd = nested_df.copy()
        if "order_class" in nd.columns:
            nd["order_class"] = nd["order_class"].astype(str).str.lower()
        if "side" in nd.columns:
            nd["side"] = nd["side"].astype(str).str.lower()
        parents_nested = nd[
            (nd["order_class"] == "bracket")
            & (nd["side"].isin(BUY_SIDES))
        ]
        for _, pr in parents_nested.iterrows():
            pid = str(pr.get("id") or "")
            ids = set()
            for L in (pr.get("legs") or []):
                for k in ("id", "client_order_id", "replaced_by", "replaces"):
                    v = str(L.get(k) or "")
                    if v:
                        ids.add(v)
            if pid:
                parent_child_ids[pid] = ids
    _D(None, f"parent_child_ids built for {len(parent_child_ids)} parents (nested)")

    # Legs cache per parent (used only for direct TP/SL from nested legs)
    legs_by_pid: Dict[str, List[Dict[str, Any]]] = {}
    if not nested_df.empty and "id" in nested_df.columns:
        for _, rr in nested_df.iterrows():
            pid = str(rr.get("id") or "")
            if pid:
                legs_by_pid[pid] = rr.get("legs") or []

    # 5) Broker snapshot for current price and avg entry (unchanged)
    pos_map: Dict[str, Dict[str, float]] = {}
    try:
        for p in api.list_positions():
            q = float(p.qty)
            if abs(q) < 1e-9:
                continue
            avg_px = float(p.avg_entry_price)
            cur_px = float(getattr(p, "current_price", 0.0)) or (
                float(p.market_value) / q if q else np.nan
            )
            pos_map[p.symbol.lower()] = {"qty": q, "avg_px": avg_px, "cur_px": cur_px}
    except Exception as e:
        _D(None, f"list_positions failed: {e}")
        pos_map = {}
    _D(None, f"positions snapshot: {len(pos_map)} symbols")

    out_rows: List[Dict[str, Any]] = []

    # 6) Per-symbol pass
    for sym, df_sym in df.groupby("symbol"):
        _D(sym, f"group rows={len(df_sym)}; parents={len(buy_parents[buy_parents['symbol'] == sym])}")

        parents_sym = buy_parents[buy_parents["symbol"] == sym]
        if parents_sym.empty:
            continue

        # Owner-first → FIFO netting on *this symbol's* slice (unchanged)
        fifo_map = _fifo_owner_first(df_sym, parents_sym, parent_child_ids)
        _D(sym, f"fifo_map pre-reconcile: { {k: round(v, 6) for k, v in fifo_map.items() if v>0} }")

        # Reconcile to broker snapshot for this symbol
        target_qty = float(pos_map.get(sym, {}).get("qty", 0.0))
        cur_qty = sum(fifo_map.values())
        if cur_qty > target_qty + 1e-9:
            diff = cur_qty - target_qty
            for _, p in parents_sym.sort_values("exec_ts").iterrows():
                pid = str(p.get("id"))
                r = fifo_map.get(pid, 0.0)
                if r <= 0 or diff <= 0:
                    continue
                cut = min(r, diff)
                fifo_map[pid] = r - cut
                diff -= cut
        if target_qty <= 1e-9:
            # Flat in broker snapshot → skip symbol entirely
            continue

        avg_px_symbol = pos_map.get(sym, {}).get("avg_px", np.nan)
        cur_px = pos_map.get(sym, {}).get("cur_px", np.nan)

        # 7) Build one row per OPEN parent lot
        for _, p in parents_sym.iterrows():
            pid = str(p.get("id"))
            qty_rem = float(fifo_map.get(pid, 0.0))
            if qty_rem <= 1e-9:
                continue

            buy_px = float(p.get("filled_avg_price") or np.nan)
            buy_dt = pd.to_datetime(p.get("exec_ts"), utc=True, errors="coerce")

            # --- TP/SL resolution (symbol-scoped) ---------------------
            # (a) Try direct from nested legs of this *parent*
            tp_val, sl_val = _live_tp_sl_from_legs(legs_by_pid.get(pid, []))

            # (b) If missing, build seeds only from this symbol slice and this parent
            if pd.isna(tp_val) or pd.isna(sl_val):
                seeds: List[str] = []

                # children_flat MUST be scoped to df_sym to avoid cross-symbol leakage
                children_flat = df_sym[df_sym["parent_id"].astype(str) == pid]

                # seeds from legs if nested exists (already used above)
                for L in (legs_by_pid.get(pid, []) or []):
                    for k in ("id", "replaces", "replaced_by", "client_order_id"):
                        v = str(L.get(k) or "")
                        if v:
                            seeds.append(v)

                # seeds from flat children of THIS symbol & parent
                if not children_flat.empty:
                    for _, rr in children_flat.iterrows():
                        for k in ("id", "replaces", "replaced_by", "client_order_id"):
                            v = str(rr.get(k) or "")
                            if v:
                                seeds.append(v)

                seeds = [s for s in dict.fromkeys(seeds) if s]  # de-dup

                # Replacement chain EXPANSION must be symbol-scoped: use df_sym
                df_scope = df_sym[
                    (df_sym["parent_id"].astype(str) == pid) | (df_sym["id"].astype(str) == pid)
                ]
                chain_ids = _expand_replacement_chain(df_scope, seeds) if seeds else set()

                if chain_ids:
                    # Live TP/SL extraction must also be symbol-scoped: use df_sym
                    tp2, sl2, tp_src, sl_src = _live_tp_sl_from_chain(df_sym, chain_ids)
                    if pd.isna(tp_val) and pd.notna(tp2):
                        tp_val = tp2
                    if pd.isna(sl_val) and pd.notna(sl2):
                        sl_val = sl2

            out_rows.append(
                {
                    "Parent ID": pid,
                    "Symbol": sym.upper(),
                    "Buy Date": buy_dt,
                    "Buy Qty": qty_rem,
                    "Buy Price": buy_px,
                    "Buy_Value": (qty_rem * buy_px) if pd.notna(buy_px) else np.nan,
                    "Take_Profit_Price": tp_val,
                    "Stop_Loss_Price": sl_val,
                    "Holding_Period": (_now_utc() - buy_dt).days if pd.notna(buy_dt) else np.nan,
                    "Current_Price": cur_px,
                    "Current Market Value": (qty_rem * cur_px) if pd.notna(cur_px) else np.nan,
                    "Profit/Loss": ((cur_px - avg_px_symbol) * qty_rem)
                                   if (pd.notna(cur_px) and pd.notna(avg_px_symbol))
                                   else np.nan,
                    "_avg_px_symbol": avg_px_symbol,
                }
            )

    # If nothing to show, return an empty schema (avoid KeyError in sort)
    if not out_rows:
        _D(None, "no open parents assembled → empty schema")
        return pd.DataFrame(columns=empty_cols)

    pos_df = (
        pd.DataFrame(out_rows)
        .sort_values(["Symbol", "Buy Date"])
        .reset_index(drop=True)
    )

    if pos_df.empty:
        _D(None, "no open parents after reconcile → empty result")
        return pos_df

    # 8) Derived columns (unchanged)
    den = pos_df["Take_Profit_Price"] - pos_df["Buy Price"]
    pos_df["TP_reach, %"] = np.where(
        (den > 0) & pos_df["Current_Price"].notna(),
        ((pos_df["Current_Price"] - pos_df["Buy Price"]) / den) * 100.0,
        np.nan,
    ).round(2)

    pos_df["BrE"] = pos_df.apply(
        lambda r: int(
            pd.notna(r["Stop_Loss_Price"])
            and np.isclose(r["Stop_Loss_Price"], _be_price(r["Buy Price"]), atol=1e-4, rtol=0)
        ),
        axis=1,
    )

    pos_df["Buy Date"] = (
        pd.to_datetime(pos_df["Buy Date"], utc=True)
        .dt.strftime("%Y-%m-%d %H:%M:%S")
    )

    if debug:
        snap = (
            pos_df.groupby("Symbol")[["Buy Qty", "Profit/Loss"]]
                  .sum(numeric_only=True)
                  .sort_values("Profit/Loss", ascending=False)
        )
        _D(None, "final open snapshot:\n" + snap.to_string())

    return pos_df


# ────────────────────────────────────────────────────────────────────
#  helper – # of trading days between two UTC timestamps
# ────────────────────────────────────────────────────────────────────
_nyse = mcal.get_calendar("XNYS")  # same holidays as Nasdaq


def _trading_days(start: pd.Timestamp, end: pd.Timestamp) -> int:
    """
    Inclusive of *start* date, exclusive of *end* date.
    Works even if both fall on non-trading days.
    """
    series = _trading_days_batch(
        pd.Series([start]),
        pd.Series([end]),
    )
    return int(series.iloc[0])


def _trading_days_batch(
    entries: pd.Series, exits: pd.Series
) -> pd.Series:
    """Vectorised trading-day span for aligned entry/exit timestamps."""
    if entries.empty:
        return pd.Series(dtype="int64")

    ny_tz = "America/New_York"
    ent = pd.to_datetime(entries, utc=True)
    ex = pd.to_datetime(exits, utc=True)

    ent_ny = ent.dt.tz_convert(ny_tz)
    ex_ny = ex.dt.tz_convert(ny_tz)

    start_date = ent_ny.min().normalize()
    end_date = ex_ny.max().normalize()
    sched = _nyse.schedule(
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
    )

    if sched.empty:
        zeros = np.zeros(len(entries), dtype="int64")
        return pd.Series(zeros, index=entries.index)

    sched_days = pd.DatetimeIndex(sched.index).tz_localize(ny_tz)
    sched_np = sched_days.view("int64")

    ent_days = ent_ny.dt.normalize().astype("int64")
    ex_days = ex_ny.dt.normalize().astype("int64")

    start_idx = np.searchsorted(sched_np, ent_days, side="left")
    end_idx = np.searchsorted(sched_np, ex_days, side="left")

    counts = end_idx - start_idx

    in_sched = np.in1d(ex_days, sched_np)
    before_close = ex_ny.dt.time < _nyse.close_time
    counts = counts - (in_sched & before_close).astype(np.int64)

    counts = np.clip(counts, 0, None)
    return pd.Series(counts, index=entries.index)


# ────────────────────────────────────────────────────────────────────
#  main KPI aggregator
# ────────────────────────────────────────────────────────────────────

def calculate_trade_metrics(
    trades: pd.DataFrame,
    debug: bool = False,
    include_manual: bool = False,
    collapse_by_exit: bool = False,
    use_pct: bool = False,
    value_weighted: bool = False,
) -> pd.DataFrame:
    """
    Compute the KPI block **per original entry lot** ("one trade") and
    classify exits as TP/SL when available.

    If `entry_lot_id` exists, collapse rows by that id so each lot
    contributes at most one row. Aggregation per lot:
      • qty           → sum of slice qty
      • entry_time    → first slice entry_time
      • entry_price   → first slice entry_price
      • exit_time     → last slice exit_time (max)
      • exit_price    → qty-weighted average of slice exit_price
      • pnl_cash      → sum of slice pnl_cash
      • pnl_pct       → pnl_cash / (entry_price * qty) * 100
      • duration_sec  → (exit_time − entry_time) seconds
      • exit_class    → qty-weighted **mode** of slice exit_type
                         (TP/SL/MANUAL/OTHER); falls back to 'UNKNOWN'

    Mean Stop / Mean Take are computed over trades whose `exit_class`
    is SL/TP respectively when such labels are present. If there are no
    TP/SL labels at all (e.g., fills-based view), we fall back to
    sign-based winners/losers by `pnl_cash`.

    When include_manual is True, a HYBRID bucket policy is used for
    performance ratios: label (TP/SL) is respected only if it agrees
    with pnl sign; otherwise sign decides. MANUAL exits are included via
    sign. When include_manual is False (default), MANUAL exits are
    excluded from winners/losers bucket metrics (R/R, breakeven, P/L
    ratio), but totals still include all trades.

    Additional options:
      • collapse_by_exit: if True and 'exit_order_id' exists, coalesce
        multiple slices of the same SELL order into a single row using
        coalesce_exit_slices() before computing metrics.
      • use_pct: compute Mean Stop/Take on pnl_pct (percent) instead of
        cash P/L. Helpful to normalize out lot-size differences.
      • value_weighted: compute cash Mean Stop/Take as value-weighted
        means (weights = qty * entry_price). Ignored if use_pct=True.
    """
    if trades.empty:
        return pd.DataFrame()

    def _d(msg: str) -> None:
        if debug:
            print(f"[calculate_trade_metrics] {msg}")

    df = trades.copy()

    # Optional: collapse multiple slices per SELL order into one row
    if collapse_by_exit and "exit_order_id" in df.columns:
        _d("collapsing by exit_order_id before metrics")
        try:
            df = coalesce_exit_slices(df)
        except Exception as e:
            _d(f"coalesce_exit_slices failed: {e}")

    _d(
        "input rows={} cols=[{}]".format(
            len(df), ", ".join(list(df.columns)[:10]) + ("…" if df.shape[1] > 10 else "")
        )
    )
    if debug:
        # quick peek at top few rows
        with pd.option_context("display.max_columns", None, "display.width", 200):
            print(df.head(3))

    # Ensure expected dtypes
    for c in ("qty", "entry_price", "exit_price", "pnl_cash"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in ("entry_time", "exit_time"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], utc=True, errors="coerce")

    # Pre-collapse totals for diagnostics
    total_pl_raw = float(df.get("pnl_cash", pd.Series(dtype=float)).sum())
    total_buy_raw = float((df.get("qty", 0) * df.get("entry_price", 0)).sum())
    if "exit_price" in df.columns and df["exit_price"].notna().any():
        total_sell_raw = float((df["qty"] * df["exit_price"]).sum())
    else:
        total_sell_raw = total_buy_raw + total_pl_raw
    _d(
        f"pre-collapse: total_pl={total_pl_raw:.6f} total_buy={total_buy_raw:.6f} total_sell={total_sell_raw:.6f}"
    )

    # Collapse to per‑lot trades if possible
    if "entry_lot_id" in df.columns:
        group_cols = []
        if "symbol" in df.columns:
            group_cols.append("symbol")
        group_cols.append("entry_lot_id")

        def _collapse(g: pd.DataFrame) -> pd.Series:
            g = g.copy()
            g.sort_values(["exit_time"], inplace=True)
            qty = float(g["qty"].sum(skipna=True))
            # Qty‑weighted entry price across slices so totals match matched buy basis
            if qty > 0 and "entry_price" in g.columns:
                w_entry = float((g["entry_price"] * g["qty"]).sum(skipna=True) / qty)
            else:
                w_entry = np.nan
            entry_time = pd.to_datetime(g["entry_time"].iloc[0], utc=True)
            exit_time = pd.to_datetime(g["exit_time"].max(), utc=True)
            if qty > 0 and "exit_price" in g.columns:
                w_exit = float((g["exit_price"] * g["qty"]).sum(skipna=True) / qty)
            else:
                w_exit = np.nan
            pnl_cash = float(g["pnl_cash"].sum(skipna=True))
            pnl_pct = (
                (pnl_cash / (w_entry * qty) * 100.0)
                if (pd.notna(w_entry) and w_entry and qty)
                else np.nan
            )
            duration_sec = (
                (exit_time - entry_time).total_seconds()
                if (pd.notna(exit_time) and pd.notna(entry_time))
                else np.nan
            )
            side = g["side"].iloc[0] if "side" in g.columns else "long"
            sym = g["symbol"].iloc[0] if "symbol" in g.columns else np.nan

            # qty-weighted majority exit type for the lot
            exit_class = "UNKNOWN"
            if "exit_type" in g.columns and g["exit_type"].notna().any():
                w = (
                    g.assign(_qty=g["qty"].fillna(0.0))
                    .groupby("exit_type")
                    ["_qty"].sum()
                    .sort_values(ascending=False)
                )
                if len(w) > 0:
                    exit_class = str(w.index[0]).upper()

            return pd.Series(
                {
                    "symbol": sym,
                    "side": side,
                    "qty": qty,
                    "entry_time": entry_time,
                    "entry_price": w_entry,
                    "exit_time": exit_time,
                    "exit_price": w_exit,
                    "pnl_cash": pnl_cash,
                    "pnl_pct": pnl_pct,
                    "duration_sec": duration_sec,
                    "exit_class": exit_class,
                }
            )

        df = (
            df.groupby(group_cols, as_index=False, sort=False)
            .apply(_collapse)
            .reset_index(drop=True)
        )

        # Diagnostics after collapse
        _d(f"post-collapse rows={len(df)}")

        # Identity checks
        total_pl = float(df["pnl_cash"].sum()) if "pnl_cash" in df.columns else 0.0
        total_buy = float((df.get("qty", 0) * df.get("entry_price", 0)).sum())
        if "exit_price" in df.columns and df["exit_price"].notna().any():
            total_sell = float((df["qty"] * df["exit_price"]).sum())
        else:
            total_sell = total_buy + total_pl

        _d(
            "post-collapse: total_pl={:.6f} total_buy={:.6f} total_sell={:.6f} diff(s-b)={:.6f}".format(
                total_pl, total_buy, total_sell, total_sell - total_buy
            )
        )

        # Label-based buckets (TP/SL) if present
        has_labels = ("exit_class" in df.columns and df["exit_class"].notna().any()) or (
            "exit_type" in df.columns and df["exit_type"].notna().any()
        )
        if "exit_class" not in df.columns:
            if "exit_type" in df.columns:
                df["exit_class"] = df["exit_type"].astype(str).str.upper()
            else:
                df["exit_class"] = "UNKNOWN"

        lab_losers = df[df["exit_class"] == "SL"]
        lab_winners = df[df["exit_class"] == "TP"]

        _d(
            "labels: losers(SL)={} winners(TP)={} mean_stop={:.6f} mean_take={:.6f}".format(
                len(lab_losers), len(lab_winners),
                float(lab_losers["pnl_cash"].mean()) if not lab_losers.empty else float("nan"),
                float(lab_winners["pnl_cash"].mean()) if not lab_winners.empty else float("nan"),
            )
        )

        # Sign-based buckets (independent cross-check)
        sign_losers = df[df["pnl_cash"] < 0]
        sign_winners = df[df["pnl_cash"] > 0]
        _d(
            "sign: losers(<0)={} winners(>0)={} mean_stop={:.6f} mean_take={:.6f}".format(
                len(sign_losers), len(sign_winners),
                abs(float(sign_losers["pnl_cash"].mean())) if not sign_losers.empty else float("nan"),
                abs(float(sign_winners["pnl_cash"].mean())) if not sign_winners.empty else float("nan"),
            )
        )

        # Per-symbol summary snapshot
        if debug:
            by_sym = (
                df.groupby("symbol")["pnl_cash"].agg(["count", "sum"]).sort_values("sum", ascending=False)
            )
            _d("per-symbol pnl snapshot:\n" + by_sym.to_string())

    else:
        # If no lot id, still derive a coarse exit_class if exit_type exists
        if "exit_type" in df.columns and df["exit_type"].notna().any():
            df["exit_class"] = df["exit_type"].astype(str).str.upper()
        else:
            df["exit_class"] = "UNKNOWN"

    # Holding period in TRADING days per (collapsed) trade
    df["holding_days"] = _trading_days_batch(
        df["entry_time"], df["exit_time"]
    )

    # ---------------- bucket selection for performance metrics ----------------
    # Helper to compute a robust bucket per row
    def _bucketize(frame: pd.DataFrame, policy: str = "hybrid") -> pd.Series:
        pnl = pd.to_numeric(frame.get("pnl_cash", 0), errors="coerce").fillna(0)
        lab = (
            frame.get("exit_class", pd.Series(index=frame.index, dtype=object))
            .astype("string")
            .str.upper()
        )

        # Default: sign bucket
        bucket = pd.Series(np.where(pnl > 0, "win", "loss"), index=frame.index)

        if policy == "sign":
            return bucket

        if policy == "label":
            mask_tp = lab == "TP"
            mask_sl = lab == "SL"
            # Use label where available; otherwise keep sign
            bucket = bucket.where(~(mask_tp | mask_sl), np.where(mask_tp, "win", "loss"))
            return bucket

        # HYBRID: use label only if it agrees with sign; else sign
        mask_tp = lab == "TP"
        mask_sl = lab == "SL"
        agree_tp = mask_tp & (pnl > 0)
        agree_sl = mask_sl & (pnl < 0)
        use_label = agree_tp | agree_sl
        return bucket.where(~use_label, np.where(agree_tp, "win", "loss"))

    # Build the sample used for winners/losers
    if include_manual:
        # Include MANUAL via HYBRID policy
        metrics_sample = df.copy()
        bucket = _bucketize(metrics_sample, policy="hybrid")
        metrics_sample = metrics_sample.assign(bucket=bucket)
        losers = metrics_sample[metrics_sample["bucket"] == "loss"]
        winners = metrics_sample[metrics_sample["bucket"] == "win"]
        _d(
            "bucket policy=HYBRID (include MANUAL): winners={} losers={}".format(
                len(winners), len(losers)
            )
        )
        if debug:
            tp_loss = ((metrics_sample.get("exit_class", "") == "TP") & (metrics_sample["pnl_cash"] < 0)).sum()
            sl_win = ((metrics_sample.get("exit_class", "") == "SL") & (metrics_sample["pnl_cash"] > 0)).sum()
            man_win = ((metrics_sample.get("exit_class", "") == "MANUAL") & (metrics_sample["pnl_cash"] > 0)).sum()
            man_loss = ((metrics_sample.get("exit_class", "") == "MANUAL") & (metrics_sample["pnl_cash"] < 0)).sum()
            total_labeled = (
                ((metrics_sample.get("exit_class", "") == "TP") | (metrics_sample.get("exit_class", "") == "SL")).sum()
            )
            label_agree = (
                ((metrics_sample.get("exit_class", "") == "TP") & (metrics_sample["pnl_cash"] > 0)).sum()
                + ((metrics_sample.get("exit_class", "") == "SL") & (metrics_sample["pnl_cash"] < 0)).sum()
            )
            agree_rate = round(100 * label_agree / total_labeled, 2) if total_labeled else np.nan
            _d(
                "confusion: TP&loss={} SL&win={} MANUAL wins={} MANUAL losses={} label_agree%={}".format(
                    tp_loss, sl_win, man_win, man_loss, agree_rate
                )
            )
    else:
        # Exclude MANUAL from performance buckets (still counted in totals)
        lab = df.get("exit_class", pd.Series(index=df.index, dtype=object)).astype("string").str.upper()
        metrics_sample = df[lab != "MANUAL"].copy()
        excl = len(df) - len(metrics_sample)
        _d(f"excluding MANUAL from performance metrics: {excl} rows")

        # Prefer labels when present; otherwise sign
        use_labels = ("exit_class" in metrics_sample.columns) and (
            metrics_sample["exit_class"].isin(["SL", "TP"]).any()
        )
        if use_labels:
            losers = metrics_sample[metrics_sample["exit_class"] == "SL"]
            winners = metrics_sample[metrics_sample["exit_class"] == "TP"]
            _d(
                "bucket policy=LABEL (manual excluded): winners={} losers={}".format(
                    len(winners), len(losers)
                )
            )
        else:
            losers = metrics_sample[metrics_sample["pnl_cash"] < 0]
            winners = metrics_sample[metrics_sample["pnl_cash"] > 0]
            _d(
                "bucket policy=SIGN (manual excluded): winners={} losers={}".format(
                    len(winners), len(losers)
                )
            )

    denom = int(len(losers) + len(winners))

    if use_pct:
        mean_stop = float(losers["pnl_pct"].mean()) if not losers.empty else 0.0
        mean_take = float(winners["pnl_pct"].mean()) if not winners.empty else 0.0
    elif value_weighted:
        lw = losers.assign(_w=losers["qty"] * losers["entry_price"]) if not losers.empty else losers
        ww = winners.assign(_w=winners["qty"] * winners["entry_price"]) if not winners.empty else winners
        mean_stop = float((lw["pnl_cash"] * lw["_w"]).sum() / lw["_w"].sum()) if (not losers.empty and lw["_w"].sum() != 0) else 0.0
        mean_take = float((ww["pnl_cash"] * ww["_w"]).sum() / ww["_w"].sum()) if (not winners.empty and ww["_w"].sum() != 0) else 0.0
    else:
        mean_stop = float(losers["pnl_cash"].mean()) if not losers.empty else 0.0
        mean_take = float(winners["pnl_cash"].mean()) if not winners.empty else 0.0

    mean_hp_stop = float(losers["holding_days"].mean()) if not losers.empty else 0.0
    mean_hp_take = float(winners["holding_days"].mean()) if not winners.empty else 0.0

    rr_ratio = abs(mean_take / mean_stop) if mean_stop else np.nan
    breakeven_pct = round(100 / (1 + rr_ratio), 2) if mean_stop else np.nan
    pl_ratio_pct = round((len(winners) / denom) * 100, 2) if denom else np.nan

    total_buy = (df["qty"] * df["entry_price"]).sum()
    if "exit_price" in df.columns and df["exit_price"].notna().any():
        total_sell = (df["qty"] * df["exit_price"]).sum()
    else:
        total_sell = total_buy + df["pnl_cash"].sum()

    ms_key = "Mean Stop %" if use_pct else "Mean Stop"
    mt_key = "Mean Take %" if use_pct else "Mean Take"

    result = {
        "Total Profit/Loss": df["pnl_cash"].sum(),
        "Total Buy": total_buy,
        "Total Sells": total_sell,
        ms_key: abs(mean_stop),
        mt_key: abs(mean_take),
        "Mean Holding Period Stop": round(mean_hp_stop, 2),
        "Mean Holding Period Take": round(mean_hp_take, 2),
        "R/R Ratio": round(rr_ratio, 2) if mean_stop else np.nan,
        "Breakeven %": breakeven_pct,
        "P/L Ratio %": pl_ratio_pct,
    }
    _d("final metrics: " + ", ".join(f"{k}={v}" for k, v in result.items()))
    return pd.DataFrame([result])


def coalesce_exit_slices(trades: pd.DataFrame) -> pd.DataFrame:
    """
    Collapse multiple partial fills that belong to the *same SELL order*
    into a single row. This is useful for fair TP/SL counts on the fills
    pipeline, where one SELL order can consume several entry lots and
    therefore produce many slice-rows.

    Grouping key: (symbol, exit_order_id)

    Aggregation:
      • qty           → sum
      • entry_price   → qty-weighted average
      • exit_price    → qty-weighted average
      • pnl_cash      → sum
      • entry_time    → earliest
      • exit_time     → latest
      • exit_type     → qty-weighted mode (TP/SL/MANUAL)
      • pnl_pct       → pnl_cash / (w_entry * qty)
      • duration_sec  → (exit_time − entry_time) seconds
    """
    if trades is None or trades.empty or "exit_order_id" not in trades.columns:
        return trades.copy() if trades is not None else pd.DataFrame()

    df = trades.copy()
    for c in ("qty", "entry_price", "exit_price", "pnl_cash"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in ("entry_time", "exit_time"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], utc=True, errors="coerce")

    df["symbol"] = df["symbol"].astype(str).str.upper()
    df["exit_order_id"] = df["exit_order_id"].astype(str)

    df["entry_num"] = df["entry_price"] * df["qty"]
    df["exit_num"] = df["exit_price"] * df["qty"]

    gb = df.groupby(["symbol", "exit_order_id"], sort=False)

    agg = gb.agg(
        qty=("qty", "sum"),
        entry_time=("entry_time", "min"),
        exit_time=("exit_time", "max"),
        pnl_cash=("pnl_cash", "sum"),
        entry_num=("entry_num", "sum"),
        exit_num=("exit_num", "sum"),
        side=("side", "first"),
    )

    agg["entry_price"] = np.where(
        agg["qty"] > 0, agg["entry_num"] / agg["qty"], np.nan
    )
    agg["exit_price"] = np.where(
        agg["qty"] > 0, agg["exit_num"] / agg["qty"], np.nan
    )
    agg["duration_sec"] = (
        agg["exit_time"] - agg["entry_time"]
    ).dt.total_seconds()
    has_basis = (
        (agg["qty"] > 0)
        & agg["entry_price"].notna()
        & (agg["entry_price"] != 0)
    )
    agg["pnl_pct"] = np.where(
        has_basis,
        agg["pnl_cash"] / (agg["entry_price"] * agg["qty"]) * 100.0,
        np.nan,
    )

    et = (
        df.assign(
            _w=df["qty"].fillna(0.0),
            exit_type=df["exit_type"].astype(str).str.upper(),
        )
        .groupby(
            ["symbol", "exit_order_id", "exit_type"], sort=False
        )["_w"]
        .sum()
        .reset_index()
        .sort_values(
            ["symbol", "exit_order_id", "_w"],
            ascending=[True, True, False],
        )
        .drop_duplicates(["symbol", "exit_order_id"])
        .set_index(["symbol", "exit_order_id"])["exit_type"]
    )

    agg["exit_type"] = et.reindex(agg.index).fillna("UNKNOWN")

    out = (
        agg.reset_index()
        .drop(columns=["entry_num", "exit_num"])
        .sort_values("exit_time", ascending=False)
        .reset_index(drop=True)
    )
    return out


# ---------------------------------------------------------------------
#  Summary KPI for open positions
# ---------------------------------------------------------------------

def summarise_open_positions(pos_df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a one-line summary table of the current floating P/L across
    *all* still-open parent orders.

    If no rows are supplied, the function yields an empty DataFrame with
    the expected column name so the caller can rely on it.
    """
    if pos_df.empty:
        return pd.DataFrame(columns=["Current Parents Total Profit/Loss"])

    total_pl = round(pos_df["Profit/Loss"].sum(), 2)

    return pd.DataFrame({"Current Parents Total Profit/Loss": [total_pl]})


def _price_from_positions(rest: REST) -> dict[str, float]:
    """
    Return {symbol: current_price} exactly as Alpaca uses in its UI.
    """
    return {p.symbol: float(p.current_price) for p in rest.list_positions()}


# ────────────────────────────────────────────────────────────────────
#  Activities (FILL) – source of truth for executions
# ────────────────────────────────────────────────────────────────────

def fetch_all_fills(
    api_key: str,
    secret_key: str,
    base_url: str,
    start: str,
    end: str,
    page_size: int = 100,
    show_progress: bool = True,
) -> pd.DataFrame:
    
    endpoint = f"{base_url.rstrip('/')}/v2/account/activities"

    headers = {
        "accept": "application/json",
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": secret_key,
    }
    params: Dict[str, Any] = {
        "activity_types": "FILL",
        "after": start,
        "until": end,
        "direction": "asc",
        "page_size": page_size,
    }

    rows: List[dict] = []
    page_token = None
    page = 0

    while True:
        if page_token:
            params["page_token"] = page_token
        else:
            params.pop("page_token", None)

        r = requests.get(endpoint, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        chunk = r.json() or []

        if show_progress:
            page += 1
            sys.stdout.write(
                f"\ractivities page: {page:<3} got: {len(chunk):<3} "
                f"total: {len(rows) + len(chunk):<6}"
            )
            sys.stdout.flush()

        if not chunk:
            break

        rows.extend(chunk)
        page_token = r.headers.get("cb-after")
        if not page_token and isinstance(chunk, list) and chunk:
            page_token = chunk[-1].get("id")
        if not page_token:
            break

        # time.sleep(0.05)

    print()  # newline after progress output

    if not rows:
        return pd.DataFrame(columns=["id", "symbol", "side", "qty", "price", "ts", "seq"])

    recs = []
    for i, row in enumerate(rows):
        if str(row.get("activity_type", "")).upper() != "FILL":
            continue
        recs.append(
            {
                "id": row.get("id"),
                "order_id": row.get("order_id"),
                "symbol": str(row.get("symbol", "")).upper(),
                "side": str(row.get("side", "")).lower(),
                "qty": float(row.get("qty", 0.0)),
                "price": float(row.get("price", 0.0)),
                "ts": pd.to_datetime(
                    row.get("transaction_time") or row.get("date"),
                    utc=True,
                    errors="coerce",
                ),
                "seq": i,
            }
        )

    df = pd.DataFrame(recs)
    df = df.dropna(subset=["ts"])
    # Preserve true execution order within each symbol: ts + seq
    df = df.sort_values(["symbol", "ts", "seq"]).reset_index(drop=True)
    return df



def closed_trades_fifo_from_fills(
    fills: pd.DataFrame,
    orders: Optional[pd.DataFrame] = None,
    debug: bool = False,
    debug_symbols: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """
    Rebuild closed trades using FIFO directly from execution activities
    (FILL). Returns one row per matched slice:

    If `orders` is provided, SELL fills with a matching `order_id` are
    annotated: 'SL' for stop/stop_limit, 'TP' for limit; otherwise 'MANUAL'.

    symbol · side · qty · entry_time · entry_price · exit_time ·
    exit_price · exit_type · pnl_cash · pnl_pct · duration_sec
    """
    def _d(msg: str) -> None:
        if debug:
            print(f"[closed_trades_fifo_from_fills] {msg}")

    if not debug:
        debug = bool(int(os.getenv("CH_DEBUG", "0")))
    if debug_symbols is None:
        env_syms = os.getenv("CH_DEBUG_SYMBOLS", "")
        debug_symbols = {s.strip().upper() for s in env_syms.split(",") if s.strip()}
    else:
        debug_symbols = {s.strip().upper() for s in debug_symbols}

    def _dbg(sym: str, msg: str) -> None:
        if debug and (not debug_symbols or sym.upper() in debug_symbols):
            print(f"[closed_trades_fifo_from_fills] {sym}: {msg}")

    if fills is None or fills.empty:
        _d("input is empty; returning empty trades frame")
        return pd.DataFrame(
            columns=[
                "symbol",
                "side",
                "qty",
                "entry_time",
                "entry_price",
                "exit_time",
                "exit_price",
                "exit_type",
                "pnl_cash",
                "pnl_pct",
                "duration_sec",
            ]
        )

    # Build order_exit_map if orders is provided
    order_exit_map: Dict[str, str] = {}
    if orders is not None and not orders.empty:
        od = orders.copy()
        for c in ("order_type", "side", "id"):
            if c in od.columns:
                od[c] = od[c].astype(str).str.lower()
        if "order_type" in od.columns and "id" in od.columns:
            def _classify(ot: str) -> str:
                ot = str(ot or "").lower()
                if "stop" in ot:
                    return "SL"
                if "limit" in ot:
                    return "TP"
                return "MANUAL"
            od_sells = od[od.get("side", "").eq("sell")]
            order_exit_map = {
                str(r["id"]): _classify(r["order_type"]) for _, r in od_sells.iterrows()
            }
        _d(f"order_exit_map size={len(order_exit_map)}")

    # Build canonical replacement map so replaced SELL orders collapse to a single id
    canonical_id: Dict[str, str] = {}
    replace_next: Dict[str, str] = {}
    if orders is not None and not orders.empty:
        # Use original (lowercased) copy 'od' from above
        if all(c in od.columns for c in ("id", "replaced_by")):
            for _, rr in od[["id", "replaced_by"]].iterrows():
                oid0 = str(rr["id"]) if pd.notna(rr["id"]) else ""
                oid1 = str(rr["replaced_by"]) if pd.notna(rr["replaced_by"]) else ""
                if oid0 and oid1:
                    replace_next[oid0] = oid1

        def _canon(oid: str) -> str:
            cur = oid
            seen: set[str] = set()
            while cur in replace_next and cur not in seen:
                seen.add(cur)
                cur = replace_next[cur]
            return cur

        keys = set(order_exit_map.keys()) | set(replace_next.keys())
        for k in keys:
            canonical_id[k] = _canon(k)
        _d(f"replace chains: {len(replace_next)}; canonical ids: {len(canonical_id)}")
    else:
        canonical_id = {}

    _d(
        "input rows={} cols=[{}]".format(
            len(fills), ", ".join(list(fills.columns)[:10]) + ("…" if fills.shape[1] > 10 else "")
        )
    )
    if debug:
        with pd.option_context("display.max_columns", None, "display.width", 200):
            print(fills.head(3))

    # Basic ledger totals within the window
    try:
        f = fills.copy()
        f["side"] = f["side"].astype(str).str.lower()
        f["symbol"] = f["symbol"].astype(str).str.upper()
        f["qty"] = pd.to_numeric(f["qty"], errors="coerce").fillna(0.0)
        f["price"] = pd.to_numeric(f.get("price", np.nan), errors="coerce")

        buys = f[f["side"] == "buy"]
        sells = f[f["side"] == "sell"]
        buy_qty = float(buys["qty"].sum())
        sell_qty = float(sells["qty"].sum())
        buy_val = float((buys["qty"] * buys["price"]).sum())
        sell_val = float((sells["qty"] * sells["price"]).sum())
        ts_min = pd.to_datetime(f["ts"], utc=True, errors="coerce").min()
        ts_max = pd.to_datetime(f["ts"], utc=True, errors="coerce").max()
        _d(
            "ledger window: {} → {} | buys: q={:.6f} v={:.6f} | sells: q={:.6f} v={:.6f} | net_q={:.6f}".format(
                ts_min, ts_max, buy_qty, buy_val, sell_qty, sell_val, buy_qty - sell_qty
            )
        )
        if debug:
            bysym = (
                f.groupby(["symbol", "side"])  # counts per side
                ["qty"].sum().unstack("side").fillna(0.0).sort_index()
            )
            _d("per-symbol qty snapshot (window):\n" + bysym.to_string())
    except Exception as e:
        _d(f"ledger diagnostics failed: {e}")

    # Ensure true execution order within each symbol
    if "seq" in fills.columns:
        fills = fills.sort_values(["symbol", "ts", "seq"]).reset_index(
            drop=True
        )
    else:
        fills = fills.sort_values(["symbol", "ts"]).reset_index(drop=True)

    from collections import deque

    out_rows: List[Dict[str, Any]] = []
    lots: Dict[str, deque] = {}
    lot_counters: Dict[str, int] = {}
    last_action: Dict[str, str] = {}
    orphan_qty = {}
    orphan_val = {}

    for _, r in fills.iterrows():
        s = str(r["symbol"]).upper()
        sd = str(r["side"]).lower()
        q = float(r["qty"])
        px = float(r["price"])
        t = pd.to_datetime(r["ts"], utc=True)

        lots.setdefault(s, deque())

        if sd == "buy":
            # Consolidate contiguous buy fills into a single entry lot until the
            # next SELL for this symbol to avoid inflated trade counts.
            if last_action.get(s) == "buy" and len(lots[s]) > 0:
                lot = lots[s][-1]
                q0 = float(lot["q"])
                px0 = float(lot["px"])
                q1 = q0 + q
                px1 = (px0 * q0 + px * q) / q1 if q1 > 0 else 0.0
                _dbg(
                    s,
                    (
                        f"BUY consolidate: was q={q0:.6f}@{px0:.6f} + add {q:.6f}@{px:.6f} → q={q1:.6f}@{px1:.6f}"
                    ),
                )
                lot["q"] = q1
                lot["px"] = px1
                # Keep earliest entry timestamp lot["t"]
            else:
                lot_counters[s] = lot_counters.get(s, 0) + 1
                lot_id = f"{s}:{lot_counters[s]:06d}"
                lots[s].append({"q": q, "px": px, "t": t, "lot_id": lot_id})
            last_action[s] = "buy"
            continue

        # sell -> consume FIFO
        # Determine exit_type for this sell fill if possible
        oid_fill_raw = str(r.get("order_id") or "") if isinstance(r, pd.Series) else ""
        oid_fill = canonical_id.get(oid_fill_raw, oid_fill_raw)
        etype = order_exit_map.get(oid_fill, "MANUAL")
        if debug and etype != "MANUAL":
            _dbg(s, f"SELL exit_type from order_id={oid_fill_raw} → {etype} (canon={oid_fill})")

        rem = q
        if rem > 1e-9 and not lots[s]:
            orphan_qty[s] = orphan_qty.get(s, 0.0) + rem
            orphan_val[s] = orphan_val.get(s, 0.0) + rem * px
            _dbg(s, f"ORPHAN SELL qty={rem:.6f} @ {px:.6f} (no open lots in window)")
            last_action[s] = "sell"
            rem = 0.0
            continue
        while rem > 1e-9 and lots[s]:
            lot = lots[s][0]
            take = min(lot["q"], rem)

            pnl = (px - lot["px"]) * take
            dur = (t - lot["t"]).total_seconds()

            out_rows.append(
                {
                    "symbol": s,
                    "side": "long",
                    "entry_lot_id": lot["lot_id"],
                    "qty": take,
                    "entry_time": lot["t"],
                    "entry_price": float(lot["px"]),
                    "exit_time": t,
                    "exit_price": px,
                    "exit_order_id": oid_fill,
                    "exit_type": etype,
                    "pnl_cash": pnl,
                    "pnl_pct": (
                        (pnl / (lot["px"] * take) * 100.0) if lot["px"] else np.nan
                    ),
                    "duration_sec": dur,
                }
            )
            _dbg(
                s,
                (
                    f"SELL consume lot_id={lot['lot_id']} take={take:.6f} entry@{float(lot['px']):.6f} "
                    f"exit@{px:.6f} pnl={pnl:.6f} left_in_lot={lot['q'] - take:.6f}"
                ),
            )

            lot["q"] -= take
            rem -= take
            if lot["q"] <= 1e-9:
                lots[s].popleft()
        # After consuming from lots, mark last action as sell
        last_action[s] = "sell"

    # --- end-of-pass diagnostics ---
    matched_buy_basis = sum(r["qty"] * r["entry_price"] for r in out_rows) if out_rows else 0.0
    matched_sell_value = sum(r["qty"] * r["exit_price"] for r in out_rows) if out_rows else 0.0
    matched_pnl = sum(r["pnl_cash"] for r in out_rows) if out_rows else 0.0
    _d(
        "matched totals: buy_basis={:.6f} sell_value={:.6f} pnl={:.6f} (sell - buy = {:.6f})".format(
            matched_buy_basis, matched_sell_value, matched_pnl, matched_sell_value - matched_buy_basis
        )
    )

    # Orphan sells (likely closing pre-start inventory)
    if orphan_qty:
        o_df = pd.DataFrame(
            {"qty": orphan_qty, "value": orphan_val}
        ).sort_values("value", ascending=False)
        _d("orphan sells summary (ignored due to no lots in window):\n" + o_df.to_string())

    # Leftover open lots at end of window
    leftover_rows = []
    from collections import deque as _dq
    for s, qd in lots.items():
        if not isinstance(qd, _dq) or not qd:
            continue
        for L in qd:
            leftover_rows.append({"symbol": s, "qty": float(L["q"]), "basis": float(L["q"]) * float(L["px"])})
    if leftover_rows:
        left_df = pd.DataFrame(leftover_rows).groupby("symbol").sum(numeric_only=True).sort_values("basis", ascending=False)
        _d("leftover open lots at end of window (qty, basis):\n" + left_df.to_string())

    out = (
        pd.DataFrame(out_rows)
        .sort_values("exit_time", ascending=False)
        .reset_index(drop=True)
    )
    return out



def realized_wac_and_open_cost_from_fills(fills: pd.DataFrame):
    """
    Alpaca-style realized P/L and open cost using Weighted Average Cost
    (WAC).

    Returns:
      (realized_pl: float, open_cost_by_symbol: Dict[str, float])
    """
    if fills is None or fills.empty:
        return 0.0, {}

    # Preserve actual execution order across all symbols
    if "seq" in fills.columns:
        fills = fills.sort_values(["ts", "seq"]).reset_index(drop=True)
    else:
        fills = fills.sort_values(["ts"]).reset_index(drop=True)

    realized = 0.0
    pos: Dict[str, tuple] = {}  # sym -> (qty, avg_cost)

    for _, r in fills.iterrows():
        s = str(r["symbol"]).upper()
        sd = str(r["side"]).lower()
        q = float(r["qty"])
        px = float(r["price"])

        q0, a0 = pos.get(s, (0.0, 0.0))
        if sd == "buy":
            q1 = q0 + q
            a1 = (a0 * q0 + px * q) / q1 if q1 > 0 else 0.0
            pos[s] = (q1, a1)
        else:
            # realized measured vs. current average cost (WAC)
            realized += (px - a0) * q
            q1 = q0 - q
            pos[s] = (q1, a0) if q1 > 1e-9 else (0.0, 0.0)

    open_cost = {sym: q * a for sym, (q, a) in pos.items() if q > 1e-9}
    return round(realized, 2), open_cost


# ---------------------------------------------------------------------
#  Convenience wrappers for realized P&L and closed trades from fills
# ---------------------------------------------------------------------

def realized_pnl_wac_from_api(
    api_key: str,
    secret_key: str,
    base_url: str,
    start: str,
    end: str,
    show_progress: bool = False,
) -> float:
    """
    Realized P&L computed the same way Alpaca reports it (Weighted
    Average Cost, WAC) for the window [`start`, `end`], using execution
    activities (FILL) fetched from the API.

    This matches the broker's Realized P&L to the cent, provided the
    FILL activity feed is complete for the window.
    """
    fills = fetch_all_fills(
        api_key, secret_key, base_url, start, end, show_progress=show_progress
    )
    realized, _ = realized_wac_and_open_cost_from_fills(fills)
    return realized



def closed_trades_fifo_from_api(
    api_key: str,
    secret_key: str,
    base_url: str,
    start: str,
    end: str,
    show_progress: bool = False,
) -> pd.DataFrame:
    """
    Convenience wrapper: fetch execution activities (FILL) in the window
    and rebuild FIFO closed trades from **fills** (not orders). Useful
    for trade‑level analytics with exact execution matching.
    """
    fills = fetch_all_fills(
        api_key, secret_key, base_url, start, end, show_progress=show_progress
    )
    return closed_trades_fifo_from_fills(fills, orders=None)



def alpaca_day_tiles(rest: REST) -> pd.DataFrame:
    """
    Exact 'day' tiles as Alpaca shows:

        day_total       = equity - last_equity
        day_unrealized  = sum(position.unrealized_intraday_pl)
        day_realized    = day_total - day_unrealized

    Also returns the lifetime unrealized (sum of
    position.unrealized_pl) for reference.
    """
    acct = rest.get_account()
    equity = float(getattr(acct, "equity", 0.0) or 0.0)
    last_equity = float(getattr(acct, "last_equity", 0.0) or 0.0)
    day_total = round(equity - last_equity, 2)

    day_unrl = 0.0
    lifetime_unrl = 0.0
    for p in rest.list_positions():
        ui = getattr(p, "unrealized_intraday_pl", 0.0) or 0.0
        ul = getattr(p, "unrealized_pl", 0.0) or 0.0
        day_unrl += float(ui)
        lifetime_unrl += float(ul)

    day_unrl = round(day_unrl, 2)
    day_rlzd = round(day_total - day_unrl, 2)

    return pd.DataFrame(
        [
            {
                "equity": equity,
                "last_equity": last_equity,
                "day_total_pl": day_total,
                "day_unrealized_pl": day_unrl,
                "day_realized_pl": day_rlzd,
                "lifetime_unrealized_pl (positions sum)": round(
                    lifetime_unrl, 2
                ),
            }
        ]
    )


# ────────────────────────────────────────────────────────────────────
#  Activities (ALL) – cash ledger + equity reconciliation
# ────────────────────────────────────────────────────────────────────

def fetch_all_activities(
    api_key: str,
    secret_key: str,
    base_url: str,
    start: str,
    end: str,
    page_size: int = 100,
    show_progress: bool = True,
) -> pd.DataFrame:
    """
    Download ALL account activities (not just FILL) between `start` and
    `end`. This includes FILL, JNLC (cash journal), FEE, DIV/SDIV, INT,
    etc.

    Returns a DataFrame with normalized columns:
        id · activity_type · symbol · side · qty · price · amount · ts
    Where:
        - amount is signed cash impact. For FILL, if not provided by API,
          we compute it as ± qty * price (buys negative, sells positive).
    """
    url = f"{base_url.rstrip('/')}/v2/account/activities"
    headers = {
        "accept": "application/json",
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": secret_key,
    }
    params: Dict[str, Any] = {
        # no activity_types filter → return everything
        "after": start,
        "until": end,
        "direction": "asc",
        "page_size": page_size,
    }

    rows: List[dict] = []
    page_token = None
    page = 0

    while True:
        if page_token:
            params["page_token"] = page_token
        elif "page_token" in params:
            del params["page_token"]

        r = requests.get(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        chunk = r.json() or []

        if show_progress:
            page += 1
            sys.stdout.write(
                (
                    f"\ractivities (ALL) page: {page:<3}  got: {len(chunk):<3}  "
                    f"total: {len(rows) + len(chunk):<6}"
                )
            )
            sys.stdout.flush()

        if not chunk:
            break

        rows.extend(chunk)

        # Use cb-after if present; otherwise fall back to last id
        page_token = r.headers.get("cb-after")
        if not page_token and isinstance(chunk, list) and chunk:
            page_token = chunk[-1].get("id")

        if not page_token:
            break

        time.sleep(0.05)

    print()  # newline after progress

    if not rows:
        return pd.DataFrame(
            columns=[
                "id",
                "activity_type",
                "symbol",
                "side",
                "qty",
                "price",
                "amount",
                "ts",
            ]
        )

    def _to_float(val) -> float:
        try:
            s = str(val).replace("$", "").replace(",", "").strip()
            return float(s)
        except Exception:
            return np.nan

    recs: List[Dict[str, Any]] = []
    for i, row in enumerate(rows):
        at = str(row.get("activity_type", "")).upper()
        sym = str(row.get("symbol", "") or "").upper()
        side = str(row.get("side", "") or "").lower()
        qty = float(row.get("qty", 0) or 0)
        price = (
            float(row.get("price", np.nan))
            if row.get("price", None) is not None
            else np.nan
        )

        # Prefer 'net_amount' then 'amount'; compute for FILL if missing
        amount = row.get("net_amount", None)
        if amount is None:
            amount = row.get("amount", None)
        amount_f = _to_float(amount)

        if at == "FILL" and (amount_f is None or np.isnan(amount_f)):
            sign = -1.0 if side == "buy" else 1.0
            amount_f = sign * qty * (price if pd.notna(price) else 0.0)

        ts = pd.to_datetime(
            row.get("transaction_time") or row.get("date"),
            utc=True,
            errors="coerce",
        )

        recs.append(
            {
                "id": row.get("id"),
                "activity_type": at,
                "symbol": sym,
                "side": side,
                "qty": qty,
                "price": price,
                "amount": amount_f,
                "ts": ts,
                "seq": i,
            }
        )

    df = pd.DataFrame(recs)
    # Preserve actual ledger order (asc): ts then seq
    df = df.dropna(subset=["ts"]).sort_values(["ts", "seq"]).reset_index(
        drop=True
    )
    return df



def reconcile_equity(
    rest: REST, fills: pd.DataFrame, activities: pd.DataFrame
) -> Dict[str, pd.DataFrame]:
    """
    Full-account reconciliation:
      equity_now  ≈  starting_cash_at_START
                    + external_cashflows_since_START
                    + realized_wac_since_START
                    + current_unrealized_pl

    Returns a dict with:
      • 'summary'          → one-row snapshot of the identity above
      • 'open_cost_cmp'    → per-symbol open-cost: API vs WAC (delta)
      • 'activities_totals'→ totals by activity_type (cash impact)
    """
    # --- realized (WAC) + open-cost (WAC) from fills ---
    realized_wac, open_cost_wac = realized_wac_and_open_cost_from_fills(
        fills if fills is not None else pd.DataFrame()
    )

    # --- current positions: unrealized and API open cost ---
    unrealized_now = 0.0
    api_open_cost: Dict[str, float] = {}
    for p in rest.list_positions():
        q = float(p.qty)
        if abs(q) < 1e-12:
            continue
        api_open_cost[p.symbol] = q * float(p.avg_entry_price)
        unrealized_now += float(getattr(p, "unrealized_pl", 0.0) or 0.0)

    # --- account equity/cash ---
    acct = rest.get_account()
    equity_now = float(getattr(acct, "equity", 0.0) or 0.0)
    cash_now = float(getattr(acct, "cash", 0.0) or 0.0)

    # --- external cashflows since START (all activities EXCEPT FILL) ---
    external_cash = 0.0
    activities_totals = pd.DataFrame(columns=["activity_type", "amount"])
    if activities is not None and not activities.empty:
        activities_totals = (
            activities.groupby("activity_type")["amount"]
            .sum()
            .round(2)
            .sort_values(ascending=False)
            .to_frame()
            .reset_index()
        )
        external_cash = float(
            activities.loc[activities["activity_type"] != "FILL", "amount"]
            .fillna(0.0)
            .sum()
        )

    # implied starting cash at START that makes identity balance
    start_cash_implied = round(
        equity_now - (realized_wac + unrealized_now + external_cash), 2
    )

    summary = pd.DataFrame(
        [
            {
                "equity_now": round(equity_now, 2),
                "cash_now": round(cash_now, 2),
                "unrealized_now": round(unrealized_now, 2),
                "realized_wac_since_START": round(realized_wac, 2),
                "external_cashflows_since_START": round(external_cash, 2),
                "implied_starting_cash_at_START": start_cash_implied,
                "check_sum": round(
                    start_cash_implied
                    + external_cash
                    + realized_wac
                    + unrealized_now,
                    2,
                ),
            }
        ]
    )

    cmp = (
        pd.Series(api_open_cost, name="api")
        .to_frame()
        .join(pd.Series(open_cost_wac, name="wac"), how="outer")
        .fillna(0.0)
    )
    cmp["delta"] = (cmp["wac"] - cmp["api"]).round(2)
    cmp = (
        cmp.reset_index()
        .rename(columns={"index": "symbol"})
        .sort_values("delta", ascending=False)
    )

    return {
        "summary": summary,
        "open_cost_cmp": cmp,
        "activities_totals": activities_totals,
    }
