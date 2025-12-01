"""Snapshot helpers for the AccountManager runtime."""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Dict, Set, TYPE_CHECKING

import pandas as pd

from cold_harbour.core.account_utils import build_open_positions_df
from cold_harbour.services.account_manager.utils import (
    _is_at_break_even,
    _last_trade_px,
    _utcnow,
)

if TYPE_CHECKING:
    from cold_harbour.services.account_manager.runtime import AccountManager


def refresh_row_metrics(row: Dict[str, Any], now_ts: datetime) -> None:
    """Compute KPIs for a row: values, P&L, reach %, holding days."""
    qty = row.get("qty", 0)
    entry_px = row.get("avg_fill", 0.0)
    basis_px = (
        row.get("avg_px_symbol")
        if row.get("avg_px_symbol") is not None
        else entry_px
    )
    mkt_px = row.get("mkt_px")
    tp_px = row.get("tp_px")
    sl_px = row.get("sl_px")

    if mkt_px is None or qty == 0:
        return

    row["buy_value"] = qty * entry_px
    row["mkt_value"] = qty * mkt_px
    row["profit_loss"] = qty * (mkt_px - basis_px)
    row["profit_loss_lot"] = row["mkt_value"] - row["buy_value"]

    try:
        row["holding_days"] = (now_ts - row["filled_at"]).days
    except Exception:
        row["holding_days"] = None

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
    mgr.log.info("Bootstrap: scanning live positions …")
    pos_live = {p.symbol: p._raw for p in mgr.rest.list_positions()}
    side_map: Dict[str, str] = {}
    for sym, raw in pos_live.items():
        try:
            q = float(raw.get("qty", 0) or 0.0)
        except Exception:
            q = 0.0
        side_map[str(sym).upper()] = "short" if q < 0 else "long"

    if not pos_live:
        mgr.log.info("No live positions → nothing to bootstrap.")
        mgr._snapshot_last_complete = _utcnow()
        mgr._snapshot_ready = True
        return

    symbols = list(pos_live)
    mgr._trace(
        "snapshot:positions", count=len(pos_live), symbols=symbols[:25]
    )

    orders_df = mgr._pull_orders()

    sym_set = {str(s).upper() for s in symbols}
    orders_df = orders_df[
        orders_df["symbol"].astype(str).str.upper().isin(sym_set)
    ].copy()

    open_df = build_open_positions_df(orders_df, mgr.rest)
    prev_ids = set(mgr.state.keys())
    new_ids: Set[str] = set()

    if open_df is None or open_df.empty:
        for pid in list(prev_ids):
            await mgr._delete(pid, skip_closed=True)
        mgr.log.info("Bootstrap stored 0 open parents.")
        mgr._snapshot_last_complete = _utcnow()
        mgr._snapshot_ready = True
        return

    now_utc = _utcnow()
    kept = 0

    for _, r in open_df.iterrows():
        try:
            parent_id = (
                str(r["Parent ID"])
                if pd.notna(r.get("Parent ID"))
                else None
            )
            if not parent_id:
                continue
            symbol = (
                str(r["Symbol"]).upper()
                if pd.notna(r.get("Symbol"))
                else None
            )
            if not symbol:
                continue

            filled_at_ts = pd.to_datetime(
                r.get("Buy Date"), utc=True, errors="coerce"
            )
            qty = int(float(r.get("Buy Qty") or 0))
            avg_fill = (
                float(r.get("Buy Price"))
                if pd.notna(r.get("Buy Price"))
                else None
            )
            avg_px_symbol = (
                float(r.get("_avg_px_symbol"))
                if "_avg_px_symbol" in r
                and pd.notna(r.get("_avg_px_symbol"))
                else avg_fill
            )
            sl_px = (
                float(r.get("Stop_Loss_Price"))
                if pd.notna(r.get("Stop_Loss_Price"))
                else None
            )
            tp_px = (
                float(r.get("Take_Profit_Price"))
                if pd.notna(r.get("Take_Profit_Price"))
                else None
            )
            mkt_px = (
                float(r.get("Current_Price"))
                if pd.notna(r.get("Current_Price"))
                else None
            )
            if mkt_px is None:
                mkt_px = _last_trade_px(mgr.rest, symbol)

            row: Dict[str, Any] = {
                "parent_id": parent_id,
                "symbol": symbol,
                "side": side_map.get(symbol, "long"),
                "filled_at": (
                    filled_at_ts.to_pydatetime()
                    if pd.notna(filled_at_ts)
                    else None
                ),
                "qty": qty,
                "avg_fill": avg_fill,
                "avg_px_symbol": avg_px_symbol,
                "sl_child": None,
                "sl_px": sl_px,
                "tp_child": None,
                "tp_px": tp_px,
                "mkt_px": mkt_px,
                "moved_flag": "—",
                "buy_value": None,
                "holding_days": None,
                "mkt_value": None,
                "profit_loss": None,
                "profit_loss_lot": None,
                "tp_sl_reach_pct": None,
                "updated_at": now_utc,
            }

            mgr.sym2pid.setdefault(symbol, set()).add(parent_id)
            _maybe_mark_moved_flag(mgr, row)
            refresh_row_metrics(row, now_utc)
            await mgr._upsert(row, force_push=True)
            kept += 1
            new_ids.add(parent_id)
        except Exception:
            mgr.log.exception(
                "Snapshot: failed to upsert parent %s", r.get("Parent ID")
            )

    gone = prev_ids - new_ids
    for pid in gone:
        await mgr._delete(pid, skip_closed=True)

    mgr.log.info("Bootstrap stored %d open parents.", kept)
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
        orders_df = mgr._pull_orders()
        if orders_df.empty:
            for pid in list(mgr.sym2pid.get(sym, set())):
                await mgr._delete(pid, skip_closed=True)
            return

        df_sym = orders_df[
            orders_df["symbol"].astype(str).str.upper() == sym
        ].copy()
        if df_sym.empty:
            for pid in list(mgr.sym2pid.get(sym, set())):
                await mgr._delete(pid, skip_closed=True)
            return

        open_df = build_open_positions_df(df_sym, mgr.rest)
        now_utc = _utcnow()
        new_ids: Set[str] = set()

        def _side_for_symbol(s: str) -> str:
            try:
                pos = mgr.rest.get_position(s)
                q = float(getattr(pos, "qty", 0.0) or 0.0)
                return "short" if q < 0 else "long"
            except Exception:
                return "long"

        if open_df is not None and not open_df.empty:
            for _, r in open_df.iterrows():
                parent_id = str(r.get("Parent ID") or "")
                if not parent_id:
                    continue

                filled_at_ts = pd.to_datetime(
                    r.get("Buy Date"), utc=True, errors="coerce"
                )
                qty = int(float(r.get("Buy Qty") or 0))
                avg_fill = (
                    float(r.get("Buy Price"))
                    if pd.notna(r.get("Buy Price"))
                    else None
                )
                avg_px_symbol = (
                    float(r.get("_avg_px_symbol"))
                    if (
                        "_avg_px_symbol" in r
                        and pd.notna(r.get("_avg_px_symbol"))
                    )
                    else avg_fill
                )
                sl_px = (
                    float(r.get("Stop_Loss_Price"))
                    if pd.notna(r.get("Stop_Loss_Price"))
                    else None
                )
                tp_px = (
                    float(r.get("Take_Profit_Price"))
                    if pd.notna(r.get("Take_Profit_Price"))
                    else None
                )
                mkt_px = (
                    float(r.get("Current_Price"))
                    if pd.notna(r.get("Current_Price"))
                    else None
                )
                if mkt_px is None:
                    mkt_px = _last_trade_px(mgr.rest, sym)

                row: Dict[str, Any] = {
                    "parent_id": parent_id,
                    "symbol": sym,
                    "side": _side_for_symbol(sym),
                    "filled_at": (
                        filled_at_ts.to_pydatetime()
                        if pd.notna(filled_at_ts)
                        else None
                    ),
                    "qty": qty,
                    "avg_fill": avg_fill,
                    "avg_px_symbol": avg_px_symbol,
                    "sl_child": None,
                    "sl_px": sl_px,
                    "tp_child": None,
                    "tp_px": tp_px,
                    "mkt_px": mkt_px,
                    "moved_flag": "—",
                    "buy_value": None,
                    "holding_days": None,
                    "mkt_value": None,
                    "profit_loss": None,
                    "profit_loss_lot": None,
                    "tp_sl_reach_pct": None,
                    "updated_at": now_utc,
                }

                try:
                    _maybe_mark_moved_flag(mgr, row)
                    mgr.sym2pid.setdefault(sym, set()).add(parent_id)
                    refresh_row_metrics(row, now_utc)
                    await mgr._upsert(row, force_push=True)
                    new_ids.add(parent_id)
                except Exception:
                    mgr.log.exception(
                        "Symbol snapshot: failed to upsert parent %s",
                        parent_id,
                    )

        gone = set(mgr.sym2pid.get(sym, set())) - new_ids
        for pid in list(gone):
            await mgr._delete(pid, skip_closed=True)
    except Exception:
        mgr.log.exception("symbol snapshot failed for %s", sym)
    finally:
        mgr._sym_refresh_inflight.discard(sym)
