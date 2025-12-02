"""Lightweight runtime worker helpers for AccountManager."""

from __future__ import annotations

import asyncio
import json
import math
from typing import TYPE_CHECKING, Dict, Iterable

import msgpack
import pandas as pd
import zmq.asyncio

from cold_harbour.core.account_analytics import calculate_metrics
from cold_harbour.services.account_manager import snapshot
from cold_harbour.services.account_manager.utils import (
    _json_safe,
    _utcnow,
)

if TYPE_CHECKING:
    from .runtime import AccountManager


async def price_listener(mgr: "AccountManager") -> None:
    """Listen to ZMQ ticks and refresh parents on price moves."""
    ctx = zmq.asyncio.Context.instance()
    sock = ctx.socket(zmq.SUB)
    endpoint = mgr.c.ZMQ_SIP_STREAM_ENDPOINT
    sock.connect(endpoint)
    sock.setsockopt(zmq.SUBSCRIBE, b"")
    log = mgr.log.with_module("price_listener")
    log.info("ZMQ price stream connected %s", endpoint)

    threshold = mgr.UI_PUSH_PCT_THRESHOLD

    while True:
        try:
            raw = await sock.recv()
            data = msgpack.unpackb(raw, raw=False)
            kind = next(iter(data))
            if kind != "trade":
                continue
            pkt = data[kind]
            sym = pkt.get("symbol") or pkt.get("S")
            px = pkt.get("price") or pkt.get("p")
            conds_raw = pkt.get("conditions") or pkt.get("c")
            if isinstance(conds_raw, (list, tuple, set)):
                conds = [str(x).strip() for x in conds_raw if x is not None]
            elif isinstance(conds_raw, str):
                conds = [
                    s.strip()
                    for s in conds_raw.split("-")
                    if s is not None
                ]
            else:
                conds = []

            if mgr._cond_exclude and (
                (not conds and "" in mgr._cond_exclude)
                or any(c in mgr._cond_exclude for c in conds)
            ):
                continue
            if sym is None or px is None:
                continue

            sym = str(sym).upper()
            parent_ids = mgr.sym2pid.get(sym, set())
            if not parent_ids:
                continue

            ts_utc = _utcnow()
            mgr._last_price_tick = ts_utc
            px_f = float(px)

            for pid in list(parent_ids):
                row = mgr.state.get(pid)
                if not row:
                    continue

                last_push = row.get("_last_pushed_px")
                pct_move = (
                    (abs(px_f - last_push) / last_push * 100)
                    if last_push
                    else 100.0
                )

                row["mkt_px"] = px_f
                row["updated_at"] = ts_utc

                if pct_move >= threshold:
                    snapshot.refresh_row_metrics(row, ts_utc)
                    await mgr._upsert(row)
                else:
                    row["_dirty_px"] = True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("price listener error: %s", exc)
            await asyncio.sleep(0.1)


async def db_worker(mgr: "AccountManager") -> None:
    """Flush rows marked dirty in regular intervals."""
    await asyncio.sleep(0)
    log = mgr.log.with_module("db_worker")

    while True:
        try:
            now_ts = _utcnow()
            dirty_ids = [
                pid
                for pid, r in mgr.state.items()
                if r.get("_dirty_px")
            ]
            for pid in dirty_ids:
                row = mgr.state.get(pid)
                if not row:
                    continue
                row.pop("_dirty_px", None)
                snapshot.refresh_row_metrics(row, now_ts)
                await mgr._upsert(row)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("db_worker error: %s", exc)
        finally:
            await asyncio.sleep(mgr.FLUSH_INTERVAL_S)


async def snapshot_loop(mgr: "AccountManager") -> None:
    """Rebuild in-memory state from scratch on a timer."""
    log = mgr.log.with_module("snapshot_loop")
    while True:
        await mgr._sleep_until_boundary(mgr.SNAPSHOT_SEC)
        async with mgr._snap_lock:
            try:
                mgr._maybe_reset_orders_cache()
                await mgr._initial_snapshot()
            except Exception:
                log.exception("snapshot refresh failed")


async def closed_trades_worker(mgr: "AccountManager") -> None:
    """Incrementally sync closed trades at an interval."""
    log = mgr.log.with_module("closed_trades_worker")
    while True:
        try:
            await mgr._sync_closed_trades()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("closed_trades_worker error: %s", exc)
        finally:
            await mgr._sleep_until_boundary(mgr.CLOSED_SYNC_SEC)


async def metrics_worker(mgr: "AccountManager") -> None:
    """Compute account metrics and persist a single JSON document."""
    interval = max(30, int(mgr.CLOSED_SYNC_SEC))
    log = mgr.log.with_module("metrics_worker")
    while True:
        try:
            payload = await _build_metrics_payload(mgr)
            metrics_payload = json.dumps(payload, default=_json_safe)
            await mgr._db_execute(
                f"""
                    INSERT INTO {mgr.tbl_metrics} (id, metrics_payload)
                    VALUES ($1, $2)
                    ON CONFLICT (id) DO UPDATE SET
                        metrics_payload = EXCLUDED.metrics_payload,
                        updated_at = now()
                    """,
                "latest",
                metrics_payload,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("metrics_worker error: %s", exc)
        finally:
            await asyncio.sleep(interval)


def _rename_columns(
    df: pd.DataFrame, mapping: Dict[str, str]
) -> pd.DataFrame:
    """Return a copy of df with selected columns renamed."""
    if df.empty:
        return df
    cols = {k: v for k, v in mapping.items() if k in df.columns}
    return df.rename(columns=cols)


def _safe_float(value: object) -> float | None:
    """Convert to float or None when not finite."""
    try:
        val = float(value)
    except Exception:
        return None
    if math.isnan(val) or math.isinf(val):
        return None
    return val


def _extract_sharpe(row: Dict[str, object], windows: Iterable[int]) -> dict:
    """Build a Sharpe payload from an equity row."""
    result: Dict[str, float] = {}
    for win in windows:
        key = f"sharpe_{win}"
        if key not in row:
            continue
        val = _safe_float(row.get(key))
        if val is None:
            continue
        result[f"Sharpe {win}d"] = val
    return result


async def _build_metrics_payload(
    mgr: "AccountManager",
) -> Dict[str, object]:
    """Load source tables, compute KPIs, and add returns/Sharpe."""
    closed_rows = await mgr._db_fetch(
        f"SELECT * FROM {mgr.tbl_closed}"
    )
    open_rows = await mgr._db_fetch(
        f"SELECT * FROM {mgr.tbl_live}"
    )
    activity_rows = await mgr._db_fetch(
        f"SELECT * FROM {mgr.tbl_cash_flows}"
    )

    closed_df = pd.DataFrame(closed_rows)
    open_df = pd.DataFrame(open_rows)
    activities_df = pd.DataFrame(activity_rows)

    open_df = _rename_columns(
        open_df,
        {
            "profit_loss": "Profit/Loss",
            "mkt_value": "Current Market Value",
            "avg_fill": "Buy Price",
            "qty": "Buy Qty",
        },
    )

    activities_df = _rename_columns(
        activities_df,
        {
            "amount": "net_amount",
            "type": "activity_type",
        },
    )
    if not activities_df.empty and "activity_type" in activities_df.columns:
        activities_df["activity_type"] = activities_df[
            "activity_type"
        ].astype(str).str.upper()

    metrics_df = calculate_metrics(
        closed_df, open_df, activities_df
    )
    metrics = (
        metrics_df.iloc[:, 0].to_dict()
        if not metrics_df.empty
        else {}
    )

    # Normalise numeric-looking strings to floats for the UI renderer.
    def _coerce_number(val: object) -> object:
        if isinstance(val, (int, float)):
            return val
        if isinstance(val, str):
            s = val.strip()
            if s.startswith("%"):
                s = s[1:]
            s = s.replace(",", "")
            try:
                return float(s)
            except Exception:
                return val
        return val

    metrics = {k: _coerce_number(v) for k, v in metrics.items()}

    equity_row = await mgr._db_fetchrow(
        f"""
            SELECT *
              FROM {mgr.tbl_equity}
          ORDER BY date DESC
             LIMIT 1
        """
    )
    if equity_row:
        total_return = _safe_float(equity_row.get("cumulative_return"))
        if total_return is not None:
            metrics["Total Return %"] = total_return * 100.0

        sharpe_vals = _extract_sharpe(
            equity_row, (10, 21, 63, 126, 252)
        )
        metrics.update(sharpe_vals)

    return metrics


async def ui_heartbeat_worker(mgr: "AccountManager") -> None:
    """Emit periodic heartbeat notifications for the UI."""
    log = mgr.log.with_module("ui_heartbeat")
    while True:
        try:
            payload = json.dumps(
                {
                    "op": "heartbeat",
                    "version": _utcnow().isoformat(),
                }
            )
            await mgr._db_execute(
                "SELECT pg_notify($1, $2);",
                mgr.pos_channel,
                payload,
            )
        except Exception as exc:
            log.debug("ui_heartbeat note: %s", exc)
        finally:
            await mgr._sleep_until_boundary(mgr.UI_SNAPSHOT_SEC)
