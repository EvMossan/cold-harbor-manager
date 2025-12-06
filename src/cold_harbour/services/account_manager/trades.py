"""Trade helpers factored out from the runtime module."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from functools import partial
from typing import Any, Dict, Optional, TYPE_CHECKING

from zoneinfo import ZoneInfo

import pandas as pd

from cold_harbour.core.account_analytics import build_closed_trades_df_lot
from cold_harbour.services.account_manager import snapshot
from cold_harbour.services.account_manager.utils import (
    _is_at_break_even,
    _is_flat,
    _json_safe,
    _utcnow,
)
from cold_harbour.services.account_manager.loader import (
    load_activities_from_db,
    load_orders_from_db,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from cold_harbour.services.account_manager.runtime import AccountManager


def _normalize_orders_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare a raw orders table before merging into the cache."""
    if df.empty:
        return df
    if "parent_id" not in df.columns and "parent_order_id" in df.columns:
        df = df.rename(columns={"parent_order_id": "parent_id"})
    if "parent_id" not in df.columns:
        df["parent_id"] = pd.NA
    if "id" in df.columns:
        df = df.drop_duplicates(subset=["id"])
    return df


async def sync_orders(
    mgr: "AccountManager", *, max_lookback_days: int = 365
) -> pd.DataFrame:
    """Fetch new orders, merge with cache, and return the full history."""
    now_ts = _utcnow()
    last_sync = mgr._last_orders_sync_time
    log = mgr.log.with_module("sync_orders")
    if last_sync is None or mgr._orders_cache.empty:
        start_ts = now_ts - timedelta(days=max_lookback_days)
        log.info(
            "Initial Load: Downloading full history lookback=%d days",
            max_lookback_days,
        )
    else:
        start_ts = last_sync - timedelta(minutes=30)
        delta = now_ts - last_sync
        minutes = int(delta.total_seconds() / 60) + 30
        log.info(
            "Incremental Load: Checking last %d minutes lookback=30m overlap",
            minutes,
        )
    end_ts = now_ts + timedelta(minutes=1)
    fetched = await load_orders_from_db(
        mgr.repo,
        mgr.c.ACCOUNT_SLUG,
        days_back=max_lookback_days,
    )
    new_df = _normalize_orders_frame(fetched)
    merged_df = pd.concat(
        [mgr._orders_cache, new_df], ignore_index=True
    )
    time_col = (
        "updated_at"
        if "updated_at" in merged_df.columns
        else "created_at"
        if "created_at" in merged_df.columns
        else None
    )
    if time_col:
        merged_df = merged_df.sort_values(time_col)
    if "id" in merged_df.columns:
        merged_df = merged_df.drop_duplicates(
            subset=["id"], keep="last"
        )
    mgr._orders_cache = merged_df.reset_index(drop=True)
    mgr._last_orders_sync_time = now_ts
    return mgr._orders_cache


async def _sync_avg_px_symbol(
    mgr: "AccountManager", symbol: str
) -> Optional[float]:
    """Refresh broker WAC basis for a symbol and update live rows."""
    try:
        pos = mgr.rest.get_position(symbol)
        basis = float(getattr(pos, "avg_entry_price", 0.0) or 0.0)
        if basis <= 0:
            return None
    except Exception:
        return None

    now_ts = _utcnow()
    for pid in list(mgr.sym2pid.get(symbol, set())):
        row = mgr.state.get(pid)
        if not row:
            continue
        if row.get("avg_px_symbol") == basis:
            continue
        old = row.copy()
        row["avg_px_symbol"] = basis
        snapshot.refresh_row_metrics(row, now_ts)
        mgr._log_if_changed(old, row)
        await mgr._upsert(row)
    return basis


async def _sync_closed_trades(mgr: "AccountManager") -> None:
    """Recompute closed trades from a safe start anchor."""
    log = mgr.log.with_module("closed_trades")
    orders_df = await sync_orders(mgr)
    if orders_df.empty:
        return

    try:
        activities_df = await load_activities_from_db(
            mgr.repo, mgr.c.ACCOUNT_SLUG
        )
    except Exception:  # pragma: no cover - logging safeguard
        log.exception("Closed trades: failed to fetch activities")
        activities_df = pd.DataFrame()

    if "activity_type" in activities_df.columns:
        fills_df = activities_df[
            activities_df["activity_type"] == "FILL"
        ].copy()
    else:
        fills_df = pd.DataFrame()

    trades = build_closed_trades_df_lot(fills_df, orders_df)
    if trades.empty:
        return

    trades = trades.copy()
    if "symbol" in trades.columns:
        trades["symbol"] = trades["symbol"].astype(str).str.upper()
    if "side" in trades.columns:
        trades["side"] = trades["side"].astype(str).str.lower()

    for c in ("entry_time", "exit_time"):
        if c in trades.columns:
            trades[c] = pd.to_datetime(
                trades[c], utc=True, errors="coerce"
            )

    trades["return_pct"] = trades.get("pnl_pct")

    for idc in ("entry_lot_id", "exit_order_id"):
        if idc not in trades.columns:
            trades[idc] = None
        trades[idc] = trades[idc].astype(str)
    if "exit_parent_id" not in trades.columns:
        trades["exit_parent_id"] = None
    trades["exit_parent_id"] = trades["exit_parent_id"].replace(
        "", pd.NA
    )

    cols = [
        "entry_lot_id",
        "exit_order_id",
        "exit_parent_id",
        "symbol",
        "side",
        "qty",
        "entry_time",
        "entry_price",
        "exit_time",
        "exit_price",
        "exit_type",
        "pnl_cash",
        "pnl_cash_fifo",
        "diff_pnl",
        "pnl_pct",
        "return_pct",
        "duration_sec",
    ]
    trades = trades.reindex(columns=cols)

    def _py(v: Any) -> Any:
        if isinstance(v, pd.Timestamp):
            return v.to_pydatetime(warn=False)
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        return v

    values = [
        tuple(_py(trades.loc[i, c]) for c in cols)
        for i in trades.index
    ]
    columns = ",".join(cols)
    placeholders = ",".join(
        f"${i}" for i in range(1, len(cols) + 1)
    )

    conflict_target = "entry_lot_id, exit_order_id"
    update_cols = [
        c for c in cols if c not in ("entry_lot_id", "exit_order_id")
    ]
    update_clause = ", ".join(
        f"{c}=EXCLUDED.{c}" for c in update_cols
    )

    await mgr._db_executemany(
        f"""
            INSERT INTO {mgr.tbl_closed} ({columns})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_target}) DO UPDATE SET
                {update_clause};
            """,
        values,
    )
    await mgr._db_execute(
        "SELECT pg_notify($1, 'refresh');", mgr.closed_channel
    )

    mgr._closed_last_sync = _utcnow()


async def _record_closed_trade_now(
    mgr: "AccountManager", row: Dict[str, Any], exit_type: str = "MANUAL"
) -> None:
    """Persist a finished position into the closed table."""
    if not row:
        return

    symbol = str(row["symbol"]).upper()
    side = row.get("side", "long")
    qty_entry = abs(row.get("entry_qty") or row.get("qty") or 0)
    if not qty_entry:
        return

    entry_ts = row["filled_at"]
    entry_px = row["avg_fill"]

    exit_ts = _utcnow()
    exit_px = row.get("mkt_px") or entry_px
    pnl_cash = (
        (exit_px - entry_px) * qty_entry
        if side.lower() == "long"
        else (entry_px - exit_px) * qty_entry
    )
    ret_pct = (
        (pnl_cash / (entry_px * qty_entry) * 100) if qty_entry else None
    )
    duration = (exit_ts - entry_ts).total_seconds() if entry_ts else None

    cols = (
        "symbol",
        "side",
        "qty",
        "entry_time",
        "entry_price",
        "exit_time",
        "exit_price",
        "exit_type",
        "pnl_cash",
        "return_pct",
        "duration_sec",
    )
    vals = (
        symbol,
        side,
        qty_entry,
        entry_ts,
        entry_px,
        exit_ts,
        exit_px,
        exit_type,
        pnl_cash,
        ret_pct,
        duration,
    )

    placeholders = ", ".join(f"${i}" for i in range(1, len(cols) + 1))
    await mgr._db_execute(
        f"""
            INSERT INTO {mgr.tbl_closed} ({','.join(cols)})
            VALUES ({placeholders})
            ON CONFLICT DO NOTHING;
            """,
        *vals,
    )
    await mgr._db_execute(
        "SELECT pg_notify($1, 'refresh');", mgr.closed_channel
    )
    try:
        ny = ZoneInfo("America/New_York")
        sess_date = exit_ts.astimezone(ny).date()
        mgr._pending_realised.append(
            (sess_date, float(pnl_cash or 0.0))
        )
        cutoff = sess_date - timedelta(days=2)
        mgr._pending_realised = [
            (d, v) for d, v in mgr._pending_realised if d >= cutoff
        ]
    except Exception:
        pass


async def _on_trade_update(mgr: "AccountManager", data: Any) -> None:
    """Handle a single Alpaca trade_updates message."""
    event = str(getattr(data, "event", "")).lower()
    order = getattr(data, "order", None)

    if order is None:
        odict: Dict[str, Any] = {}
    else:
        model_dump = getattr(order, "model_dump", None)
        if callable(model_dump):
            odict = model_dump()
        else:
            odict = getattr(order, "_raw", {})

    oid = str(getattr(order, "id", "") or odict.get("id") or "")
    parent = str(odict.get("parent_order_id") or "") or None

    symbol_raw = odict.get("symbol")
    symbol = str(symbol_raw).upper() if symbol_raw else None

    otype = odict.get("type") or odict.get("order_type")
    intent = str(odict.get("position_intent", "")).lower()
    now_ts = _utcnow()

    is_bracket_parent = (
        odict.get("order_class") == "bracket"
        and parent is None
        and intent.endswith("_to_open")
    )

    mgr._trace(
        "trade_update:begin",
        event=event,
        oid=oid,
        parent=parent,
        symbol=symbol,
        otype=otype,
        intent=intent,
        is_bracket_parent=is_bracket_parent,
        stream_fields={
            "position_qty": _json_safe(
                getattr(data, "position_qty", None)
            ),
            "price": _json_safe(getattr(data, "price", None)),
            "qty": _json_safe(getattr(data, "qty", None)),
            "status": getattr(order, "status", None),
        },
    )

    if symbol:
        try:
            asyncio.create_task(mgr._refresh_symbol_snapshot(symbol))
        except Exception:
            pass

    if (
        symbol
        and not is_bracket_parent
        and parent is None
        and odict.get("order_class") != "bracket"
    ):
        if oid not in mgr.state and event in {"fill", "partial_fill"}:
            try:
                pos_qty = float(
                    getattr(data, "position_qty", 0) or 0
                )
            except Exception:
                pos_qty = 0.0
            if _is_flat(pos_qty):
                last_px = float(
                    getattr(data, "price", 0)
                    or odict.get("filled_avg_price")
                    or 0
                )
                for pid in list(mgr.sym2pid.get(symbol, set())):
                    await mgr._delete(
                        pid,
                        skip_closed=False,
                        exit_px=last_px,
                        exit_type="MANUAL",
                    )
                if symbol in mgr.sym2pid:
                    mgr.sym2pid[symbol].clear()
                else:
                    try:
                        await asyncio.to_thread(
                            _sync_closed_trades, mgr
                        )
                    except Exception:
                        pass
                return

            fill_qty = float(
                odict.get("filled_qty")
                or getattr(data, "qty", 0)
                or 0.0
            )
            fill_price = float(
                odict.get("filled_avg_price")
                or getattr(data, "price", 0)
                or 0
            )

            try:
                pos = mgr.rest.get_position(symbol)
                avg_px_symbol = float(
                    getattr(pos, "avg_entry_price", fill_price)
                )
            except Exception:
                avg_px_symbol = fill_price

            filled_at_ts = pd.to_datetime(
                odict.get("filled_at") or odict.get("submitted_at"),
                utc=True,
                errors="coerce",
            )
            filled_at = (
                filled_at_ts.to_pydatetime()
                if pd.notna(filled_at_ts)
                else None
            )

            row = dict(
                parent_id=oid,
                symbol=symbol,
                side=(
                    "long"
                    if str(getattr(order, "side", "buy")).lower()
                    == "buy"
                    else "short"
                ),
                filled_at=filled_at,
                qty=fill_qty,
                avg_fill=fill_price,
                avg_px_symbol=avg_px_symbol,
                entry_qty=fill_qty,
                sl_child=None,
                sl_px=None,
                tp_child=None,
                tp_px=None,
                mkt_px=fill_price,
                moved_flag="—",
                updated_at=now_ts,
            )
            mgr._trace(
                "trade_update:simple_first_fill:create_row", row=row
            )
            snapshot.refresh_row_metrics(row, now_ts)
            await mgr._upsert(row, force_push=True)
            mgr.sym2pid.setdefault(symbol, set()).add(oid)
            return

        if oid in mgr.state and event in {"fill", "partial_fill"}:
            row = mgr.state[oid]
            old = row.copy()
            row["qty"] = float(
                odict.get("filled_qty")
                or getattr(data, "qty", 0)
                or row["qty"]
            )
            row["avg_fill"] = float(
                odict.get("filled_avg_price")
                or getattr(data, "price", 0)
                or row["avg_fill"]
            )
            row["updated_at"] = now_ts
            try:
                await _sync_avg_px_symbol(mgr, symbol)
            except Exception:
                pass
            snapshot.refresh_row_metrics(row, now_ts)
            mgr._trace(
                "trade_update:simple_update", before=old, after=row
            )
            mgr._log_if_changed(old, row)
            await mgr._upsert(row, force_push=True)

            if _is_flat(row["qty"]):
                await mgr._delete(oid)
            return

    if (
        (parent is None and intent.endswith("_to_close"))
        or (parent and parent not in mgr.state)
    ):
        key = parent or oid
        if otype in {"stop", "stop_limit"}:
            mgr._pending_legs.setdefault(key, {}).update(
                {
                    "sl_child": oid,
                    "sl_px": float(
                        odict.get("stop_price")
                        or odict.get("limit_price")
                        or 0
                    ),
                }
            )
        elif otype == "limit":
            mgr._pending_legs.setdefault(key, {}).update(
                {
                    "tp_child": oid,
                    "tp_px": float(
                        odict.get("limit_price") or 0
                    ),
                }
            )
        mgr._trace(
            "trade_update:cache_leg_before_parent",
            key=key,
            cached=mgr._pending_legs.get(key, {}),
        )
        return

    if (
        is_bracket_parent
        and oid not in mgr.state
        and event in {"partial_fill", "fill"}
    ):
        fill_qty = float(
            odict.get("filled_qty")
            or getattr(data, "qty", 0)
            or 0.0
        )
        fill_price = float(
            odict.get("filled_avg_price")
            or getattr(data, "price", 0)
            or 0
        )
        try:
            pos = mgr.rest.get_position(symbol)
            avg_px_symbol = float(
                getattr(pos, "avg_entry_price", fill_price)
            )
        except Exception:
            avg_px_symbol = fill_price

        filled_at_ts = pd.to_datetime(
            odict.get("filled_at") or odict.get("submitted_at"),
            utc=True,
            errors="coerce",
        )
        filled_at = (
            filled_at_ts.to_pydatetime()
            if pd.notna(filled_at_ts)
            else None
        )

        row = dict(
            parent_id=oid,
            symbol=symbol,
            side=(
                "long"
                if str(getattr(order, "side", "buy")).lower()
                == "buy"
                else "short"
            ),
            filled_at=filled_at,
            qty=fill_qty,
            avg_fill=fill_price,
            avg_px_symbol=avg_px_symbol,
            entry_qty=fill_qty,
            sl_child=None,
            sl_px=None,
            tp_child=None,
            tp_px=None,
            mkt_px=fill_price,
            moved_flag="—",
            updated_at=now_ts,
        )

        if oid in mgr._pending_legs:
            row.update(mgr._pending_legs.pop(oid))

        if row.get("sl_px") is not None:
            row["moved_flag"] = (
                "OK"
                if _is_at_break_even(
                    row.get("side", "long"),
                    row["avg_fill"],
                    row["sl_px"],
                    mgr.c.MIN_STOP_GAP,
                )
                else "—"
            )

        mgr._trace("trade_update:bracket_first_fill:create_row", row=row)
        snapshot.refresh_row_metrics(row, now_ts)
        await mgr._upsert(row, force_push=True)
        mgr.sym2pid.setdefault(symbol, set()).add(oid)
        return

    if parent and parent in mgr.state:
        row = mgr.state[parent]
        old = row.copy()
        if oid in mgr._pending_legs:
            row.update(mgr._pending_legs.pop(oid))
        mgr._trace(
            "trade_update:child_leg_update:before",
            event=event,
            otype=otype,
            before=old,
        )

        if event in {"new", "accepted", "pending_new", "replaced", "held"}:
            if otype in {"stop", "stop_limit"}:
                row["sl_child"] = oid
                row["sl_px"] = float(
                    odict.get("stop_price")
                    or odict.get("limit_price")
                    or row.get("sl_px")
                )
            elif otype == "limit":
                row["tp_child"] = oid
                if odict.get("limit_price") is not None:
                    row["tp_px"] = float(odict["limit_price"])

            if row.get("sl_px") is not None:
                row["moved_flag"] = (
                    "OK"
                    if _is_at_break_even(
                        row.get("side", "long"),
                        row["avg_fill"],
                        row["sl_px"],
                        mgr.c.MIN_STOP_GAP,
                    )
                    else "—"
                )

        if event in {"fill", "canceled", "expired"}:
            if event == "fill":
                if oid == row.get("sl_child"):
                    row["_exit_type"] = "SL"
                elif oid == row.get("tp_child"):
                    row["_exit_type"] = "TP"
            if oid == row.get("sl_child"):
                row.update(
                    {"sl_child": None, "sl_px": None, "moved_flag": "—"}
                )
            elif oid == row.get("tp_child"):
                row.update({"tp_child": None, "tp_px": None})

        row["updated_at"] = now_ts
        mgr._trace("trade_update:child_leg_update:after", after=row)

        if not row.get("sl_child") and not row.get("tp_child"):
            mgr._trace(
                "trade_update:both_legs_gone_closing",
                parent=parent,
                snapshot=row,
            )
            await _record_closed_trade_now(
                mgr, row, exit_type=row.pop("_exit_type", "MANUAL")
            )
            await mgr._delete(parent, skip_closed=True)
        else:
            snapshot.refresh_row_metrics(row, now_ts)
            mgr._log_if_changed(old, row)
            await mgr._upsert(row, force_push=True)
        return

    if oid in mgr.state:
        row = mgr.state[oid]
        old = row.copy()

        if event in {"partial_fill", "fill"}:
            row["qty"] = float(
                odict.get("filled_qty")
                or getattr(data, "qty", 0)
                or row["qty"]
            )
            row["avg_fill"] = float(
                odict.get("filled_avg_price")
                or getattr(data, "price", 0)
                or row["avg_fill"]
            )
            row["moved_flag"] = (
                "OK"
                if _is_at_break_even(
                    row.get("side", "long"),
                    row["avg_fill"],
                    row.get("sl_px"),
                    mgr.c.MIN_STOP_GAP,
                )
                else "—"
            )
            row["updated_at"] = now_ts
            if symbol:
                try:
                    await _sync_avg_px_symbol(mgr, symbol)
                except Exception:
                    pass
            snapshot.refresh_row_metrics(row, now_ts)
            mgr._log_if_changed(old, row)
            await mgr._upsert(row, force_push=True)

            if _is_flat(row["qty"]):
                await _record_closed_trade_now(mgr, row)
                await mgr._delete(oid, skip_closed=True)
            return

        if event in {"canceled", "expired", "done_for_day"}:
            await mgr._delete(oid)
            return


async def _run_trade_stream(mgr: "AccountManager") -> None:
    """Run the trade_updates stream with a legacy fallback."""
    try:
        from alpaca.trading.stream import TradingStream  # type: ignore

        ws_url = mgr.c.ALPACA_BASE_URL.rstrip("/")
        if ws_url.startswith("http"):
            ws_url = ws_url.replace("http", "ws", 1)
        if not ws_url.endswith("/stream"):
            ws_url = f"{ws_url}/stream"

        stream = TradingStream(
            mgr.c.API_KEY,
            mgr.c.SECRET_KEY,
            paper=("paper" in mgr.c.ALPACA_BASE_URL),
            url_override=ws_url,
        )
        stream.subscribe_trade_updates(
            partial(_on_trade_update, mgr)  # type: ignore[arg-type]
        )
        await stream.run()
    except asyncio.CancelledError:
        raise
    except Exception as primary_exc:
        mgr.log.debug(
            "TradingStream unavailable/failed (%s), falling back …",
            primary_exc,
        )
        try:
            from alpaca_trade_api.common import URL  # type: ignore
            from alpaca_trade_api.stream import Stream  # type: ignore

            stream = Stream(
                mgr.c.API_KEY,
                mgr.c.SECRET_KEY,
                base_url=URL(mgr.c.ALPACA_BASE_URL),
            )
            stream.subscribe_trade_updates(
                partial(_on_trade_update, mgr)  # type: ignore[arg-type]
            )
            await stream._run_forever()  # type: ignore[attr-defined]
        except asyncio.CancelledError:
            raise
        except Exception as secondary_exc:
            mgr.log.exception(
                "Both TradingStream and fallback Stream failed: %s",
                secondary_exc,
            )
