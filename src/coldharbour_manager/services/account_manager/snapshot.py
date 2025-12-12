"""Snapshot helpers for the AccountManager runtime."""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Dict, Optional, Set, TYPE_CHECKING

import pandas as pd

from coldharbour_manager.services.account_manager.core_logic.account_analytics import (
    build_lot_portfolio,
)
from coldharbour_manager.services.account_manager import trades
from coldharbour_manager.services.account_manager.db import fetch_latest_prices
from coldharbour_manager.services.account_manager.utils import (
    _is_at_break_even,
    _utcnow,
)


def _aggregate_lots(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate fill lots by Parent ID so callers see one row per parent."""
    if df.empty or "Parent ID" not in df.columns:
        return df

    valid = df[df["Parent ID"].notna() & (df["Parent ID"] != "")].copy()
    if valid.empty:
        return df

    # Pre-calc weighted value for accurate Avg Price reconstruction.
    # Use Remaining Qty because it represents the held portion.
    # Ensure columns exist and fill NaNs to avoid math errors.
    if "Buy Price" in valid.columns and "Remaining Qty" in valid.columns:
        valid["_w_val"] = valid["Buy Price"].fillna(0.0) * valid["Remaining Qty"].fillna(0.0)
    else:
        valid["_w_val"] = 0.0

    aggs = {
        "Symbol": "first",
        "Buy Date": "first",
        "Buy Qty": "sum",
        "Remaining Qty": "sum",
        "_w_val": "sum",
        "Current Market Value": "sum",
        "Profit/Loss": "sum",
        "Current_Price": "first",
        "Take_Profit_Price": "first",
        "Stop_Loss_Price": "first",
        "Take_Profit_ID": "first",
        "Stop_Loss_ID": "first",
        "Source": "first",
        "Avg Entry (API)": "first",
        "_avg_px_symbol": "first",
        # Preserve TP_reach when possible; the first entry suffices.
        "TP_reach, %": "first",
        "Days_To_Expire": "first",
    }

    actual_aggs = {k: v for k, v in aggs.items() if k in valid.columns}

    grouped = valid.groupby("Parent ID", as_index=False).agg(actual_aggs)

    # Re-calculate weighted average Buy Price
    if "_w_val" in grouped.columns and "Remaining Qty" in grouped.columns:
        grouped["Buy Price"] = grouped.apply(
            lambda r: r["_w_val"] / r["Remaining Qty"] 
            if r["Remaining Qty"] and r["Remaining Qty"] > 1e-9 else 0.0, 
            axis=1
        )
        grouped.drop(columns=["_w_val"], inplace=True)
    else:
        grouped["Buy Price"] = 0.0

    return grouped

if TYPE_CHECKING:
    from coldharbour_manager.services.account_manager.runtime import AccountManager


def _safe_float(value: Any) -> Optional[float]:
    """Convert values to float or return None for invalid input."""
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(result):
        return None
    return result


def _normalize_leg_id(value: Any) -> Optional[str]:
    """Return str id or None for leg identifier fields."""
    if value is None or pd.isna(value):
        return None
    return str(value)


def _build_side_map(positions: list[Any]) -> Dict[str, str]:
    """Map live positions to their long/short side."""
    side_map: Dict[str, str] = {}
    for pos in positions:
        symbol = getattr(pos, "symbol", None)
        if not symbol:
            continue
        try:
            qty = float(getattr(pos, "qty", 0.0) or 0.0)
        except Exception:
            qty = 0.0
        side_map[str(symbol).upper()] = "short" if qty < 0 else "long"
    return side_map


def _build_live_row(
    mgr: "AccountManager",
    entry: pd.Series,
    now_ts: datetime,
    side_map: Dict[str, str],
    price_cache: Dict[str, float],
    symbol_hint: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Translate a lot row to match the live schema."""
    parent_raw = entry.get("Parent ID")
    parent_id = str(parent_raw).strip() if parent_raw not in (None, "") else ""
    if not parent_id:
        return None

    symbol_value = symbol_hint or entry.get("Symbol")
    if symbol_value is None:
        return None
    symbol = str(symbol_value).upper()

    filled_at_ts = pd.to_datetime(
        entry.get("Buy Date"), utc=True, errors="coerce"
    )
    filled_at = (
        filled_at_ts.to_pydatetime()
        if pd.notna(filled_at_ts)
        else None
    )

    qty_val = _safe_float(entry.get("Remaining Qty"))
    qty = qty_val if qty_val is not None else 0.0

    avg_fill = _safe_float(entry.get("Buy Price"))
    avg_px_symbol = (
        _safe_float(entry.get("Avg Entry (API)"))
        or _safe_float(entry.get("_avg_px_symbol"))
        or avg_fill
    )
    sl_px = _safe_float(entry.get("Stop_Loss_Price"))
    tp_px = _safe_float(entry.get("Take_Profit_Price"))
    mkt_px = _safe_float(entry.get("Current_Price"))
    if mkt_px is None:
        mkt_px = price_cache.get(symbol)
        # Fall back to Timescale rows when the live tick is missing.
        # Leave None if the lookup still fails.

    buy_value = _safe_float(entry.get("Buy_Value"))
    mkt_value = _safe_float(entry.get("Current Market Value"))
    profit_loss = _safe_float(entry.get("Profit/Loss"))
    profit_loss_lot = (
        mkt_value - buy_value
        if buy_value is not None and mkt_value is not None
        else None
    )
    tp_sl_reach = _safe_float(entry.get("TP_reach, %"))

    sl_child = _normalize_leg_id(entry.get("Stop_Loss_ID"))
    tp_child = _normalize_leg_id(entry.get("Take_Profit_ID"))

    days_expire = _safe_float(entry.get("Days_To_Expire"))

    return {
        "parent_id": parent_id,
        "symbol": symbol,
        "side": side_map.get(symbol, "long"),
        "filled_at": filled_at,
        "qty": qty,
        "avg_fill": avg_fill,
        "avg_px_symbol": avg_px_symbol,
        "sl_child": sl_child,
        "sl_px": sl_px,
        "tp_child": tp_child,
        "tp_px": tp_px,
        "mkt_px": mkt_px,
        "moved_flag": "—",
        "buy_value": buy_value,
        "holding_days": None,
        "days_to_expire": days_expire,
        "mkt_value": mkt_value,
        "profit_loss": profit_loss,
        "profit_loss_lot": profit_loss_lot,
        "tp_sl_reach_pct": tp_sl_reach,
        "updated_at": now_ts,
    }


def refresh_row_metrics(row: Dict[str, Any], now_ts: datetime) -> None:
    """Compute KPIs for a row: values, P&L, reach %, holding days."""
    qty = _safe_float(row.get("qty")) or 0.0
    entry_px = _safe_float(row.get("avg_fill")) or 0.0
    basis_raw = row.get("avg_px_symbol")
    basis_px = _safe_float(basis_raw)
    if basis_px is None:
        basis_px = entry_px

    mkt_px = row.get("mkt_px")
    tp_px = row.get("tp_px")
    sl_px = row.get("sl_px")

    row["buy_value"] = qty * entry_px
    try:
        row["holding_days"] = (now_ts - row["filled_at"]).days
    except Exception:
        row["holding_days"] = None

    if mkt_px is None or qty == 0:
        row["mkt_value"] = None
        row["profit_loss"] = None
        row["profit_loss_lot"] = None
        row["tp_sl_reach_pct"] = None
        return

    row["mkt_value"] = qty * mkt_px
    row["profit_loss"] = qty * (mkt_px - basis_px)
    row["profit_loss_lot"] = row["mkt_value"] - row["buy_value"]

    target_px = tp_px if row["profit_loss_lot"] >= 0 else sl_px
    if target_px and not math.isclose(target_px, entry_px):
        ratio = (mkt_px - entry_px) / (target_px - entry_px) * 100
        row["tp_sl_reach_pct"] = round(ratio, 2)
    else:
        row["tp_sl_reach_pct"] = None


def _maybe_mark_moved_flag(
    mgr: "AccountManager", row: Dict[str, Any]
) -> None:
    """Tag row moved_flag if stop and entry are known."""
    sl_px = row.get("sl_px")
    entry = row.get("avg_fill")
    if sl_px is None or entry is None:
        return
    try:
        row["moved_flag"] = (
            "OK"
            if _is_at_break_even(
                row.get("side", "long"),
                float(entry),
                float(sl_px),
                mgr.c.MIN_STOP_GAP,
            )
            else "—"
        )
    except Exception:
        pass


async def initial_snapshot(mgr: "AccountManager") -> None:
    """Build a snapshot of all open parents with KPIs pre-filled."""
    log = mgr.log.with_module("snapshot")
    log.info("Bootstrap: scanning live positions …")

    try:
        positions = mgr.rest.list_positions()
    except Exception:
        log.exception("Snapshot: failed to list live positions")
        positions = []

    side_map = _build_side_map(positions)
    prev_ids = set(mgr.state.keys())

    try:
        orders_df = await trades.sync_orders(mgr)
    except Exception:
        log.exception("Snapshot: failed to fetch orders")
        orders_df = pd.DataFrame()

    open_df = build_lot_portfolio(orders_df, api=mgr.rest)
    open_df = _aggregate_lots(open_df)
    if open_df is None or open_df.empty:
        for pid in list(prev_ids):
            await mgr._delete(pid, skip_closed=True)
        log.info("Bootstrap stored 0 open parents.")
        mgr._snapshot_last_complete = _utcnow()
        mgr._snapshot_ready = True
        return

    symbol_series = (
        open_df["Symbol"]
        if "Symbol" in open_df.columns
        else pd.Series(dtype=str)
    )
    sym_set = {
        str(sym).upper()
        for sym in symbol_series.dropna().unique()
    }
    price_cache = await fetch_latest_prices(mgr.ts_repo, list(sym_set))

    now_utc = _utcnow()
    kept = 0
    new_ids: Set[str] = set()

    for _, entry in open_df.iterrows():
        row = _build_live_row(
            mgr, entry, now_utc, side_map, price_cache=price_cache
        )
        if row is None:
            continue
        parent_id = row["parent_id"]
        symbol = row["symbol"]
        try:
            _maybe_mark_moved_flag(mgr, row)
            refresh_row_metrics(row, now_utc)
            mgr.sym2pid.setdefault(symbol, set()).add(parent_id)
            await mgr._upsert(row, force_push=True)
            kept += 1
            new_ids.add(parent_id)
        except Exception:
            log.exception(
                "Snapshot: failed to upsert parent %s", parent_id
            )

    gone = prev_ids - new_ids
    for pid in gone:
        await mgr._delete(pid, skip_closed=True)

    log.info("Bootstrap stored %d open parents.", kept)
    mgr._trace(
        "snapshot:open_df",
        rows=int(open_df.shape[0]),
        kept=kept,
        symbols=len(sym_set),
    )
    mgr._snapshot_last_complete = _utcnow()
    mgr._snapshot_ready = True


async def refresh_symbol_snapshot(
    mgr: "AccountManager", symbol: str
) -> None:
    """Rebuild the cache for one symbol from orders."""
    if not symbol:
        return
    sym = str(symbol).upper()
    if not getattr(mgr, "_snapshot_ready", False):
        return
    if sym in getattr(mgr, "_sym_refresh_inflight", set()):
        return

    mgr._sym_refresh_inflight.add(sym)
    try:
        log = mgr.log.with_module("symbol_snapshot")
        try:
            positions = mgr.rest.list_positions()
        except Exception:
            log.exception("Symbol snapshot: failed to list positions")
            positions = []

        side_map = _build_side_map(positions)
        try:
            orders_df = await trades.sync_orders(mgr)
        except Exception:
            log.exception("Symbol snapshot: failed to fetch orders")
            orders_df = pd.DataFrame()

        if "symbol" in orders_df.columns:
            orders_df = orders_df[
                orders_df["symbol"]
                .astype(str)
                .str.upper()
                .eq(sym)
            ].copy()
        else:
            orders_df = orders_df.copy()
        open_df = build_lot_portfolio(orders_df, api=mgr.rest)
        open_df = _aggregate_lots(open_df)
        if open_df is None or open_df.empty:
            for pid in list(mgr.sym2pid.get(sym, set())):
                await mgr._delete(pid, skip_closed=True)
            return

        if "Symbol" in open_df.columns:
            open_df = open_df[
                open_df["Symbol"]
                .astype(str)
                .str.upper()
                .eq(sym)
            ].copy()

        price_cache = await fetch_latest_prices(mgr.ts_repo, [sym])

        now_utc = _utcnow()
        new_ids: Set[str] = set()
        prev_ids = set(mgr.sym2pid.get(sym, set()))

        if open_df is not None and not open_df.empty:
            for _, entry in open_df.iterrows():
                row = _build_live_row(
                    mgr,
                    entry,
                    now_utc,
                    side_map,
                    price_cache=price_cache,
                    symbol_hint=sym,
                )
                if row is None:
                    continue

                parent_id = row["parent_id"]
                try:
                    _maybe_mark_moved_flag(mgr, row)
                    refresh_row_metrics(row, now_utc)
                    mgr.sym2pid.setdefault(sym, set()).add(parent_id)
                    await mgr._upsert(row, force_push=True)
                    new_ids.add(parent_id)
                except Exception:
                    mgr.log.exception(
                        "Symbol snapshot: failed to upsert parent %s",
                        parent_id,
                    )

        gone = prev_ids - new_ids
        for pid in list(gone):
            await mgr._delete(pid, skip_closed=True)
    except Exception:
        log.exception("symbol snapshot failed for %s", sym)
    finally:
        mgr._sym_refresh_inflight.discard(sym)
