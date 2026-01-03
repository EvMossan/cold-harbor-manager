"""Equity helpers shared by AccountManager runtime tasks."""

from __future__ import annotations

import asyncio
import math
import os
from typing import TYPE_CHECKING, Optional, Any, Set

import numpy as np
import pandas as pd
import datetime as dt
from alpaca_trade_api.rest import TimeFrame, TimeFrameUnit

from coldharbour_manager.services.account_manager.core_logic.equity import (
    rebuild_equity_series_async,
    update_today_row_async,
)
from coldharbour_manager.services.account_manager.utils import _utcnow
from coldharbour_manager.services.account_manager.loader import (
    load_activities_from_db,
)

if TYPE_CHECKING:
    from coldharbour_manager.services.account_manager.runtime import AccountManager


async def _bootstrap_cash_flows(
    mgr: "AccountManager",
    max_pages: int | None = None,
) -> int:
    """Load all account activities and persist them into cash flows."""

    await mgr._db_execute(
        f"""
            CREATE TABLE IF NOT EXISTS {mgr.tbl_cash_flows} (
                id          text PRIMARY KEY,
                ts          timestamptz,
                amount      real,
                type        text,
                description text,
                updated_at  timestamptz DEFAULT now()
            );
            """
    )

    if max_pages is not None:
        mgr.log.debug("flows bootstrap ignoring max_pages=%s", max_pages)

    try:
        activities = await load_activities_from_db(
            mgr.repo, mgr.c.ACCOUNT_SLUG
        )
    except Exception:
        mgr.log.exception("flows bootstrap failed to fetch activities")
        activities = pd.DataFrame()

    if activities.empty:
        mgr.log.info("flows bootstrap: nothing to ingest")
        return 0

    records: list[tuple[Any, Any, Any, Any, Any]] = []
    if "id" not in activities.columns and "activity_id" in activities.columns:
        activities = activities.rename(columns={"activity_id": "id"})

    seen_ids: set[str] = set()
    time_cols = [
        "exec_time",
        "transaction_time",
        "activity_time",
        "date",
        "timestamp",
        "created_at",
    ]
    amount_cols = ["net_amount", "cash", "amount"]

    def _float_or_none(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            result = float(value)
        except (TypeError, ValueError):
            return None
        if math.isnan(result):
            return None
        return result

    for _, row in activities.iterrows():
        raw_id = row.get("id")
        if pd.isna(raw_id) or raw_id is None:
            continue
        row_id = str(raw_id).strip()
        if not row_id or row_id in seen_ids:
            continue
        seen_ids.add(row_id)

        raw_type = row.get("activity_type") or row.get("type")
        
        ts_val = None
        for col in time_cols:
            if col not in activities.columns:
                continue
            candidate = row.get(col)
            if pd.isna(candidate):
                continue
            ts = pd.to_datetime(candidate, utc=True, errors="coerce")
            if pd.notna(ts):
                ts_val = ts.to_pydatetime()
                break

        amount = None
        for col in amount_cols:
            if col not in activities.columns:
                continue
            amount = _float_or_none(row.get(col))
            if amount is not None:
                break
        if amount is None:
            amount = 0.0

        type_val = raw_type or ""
        desc_val = row.get("description") or row.get("symbol") or ""
        records.append(
            (
                row_id,
                ts_val,
                amount,
                str(type_val) if type_val not in (None, "") else "",
                str(desc_val) if desc_val not in (None, "") else "",
            )
        )

    if not records:
        return 0

    await mgr._db_executemany(
        f"""
            INSERT INTO {mgr.tbl_cash_flows}
                (id, ts, amount, type, description)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (id) DO UPDATE SET
                ts = EXCLUDED.ts,
                amount = EXCLUDED.amount,
                type = EXCLUDED.type,
                description = EXCLUDED.description,
                updated_at = now()
            """,
        records,
    )
    return len(records)


async def _rebuild_equity_full(mgr: "AccountManager") -> None:
    """Rebuild daily equity using full ledger replay."""
    cfg = {
        "CONN_STRING_POSTGRESQL": mgr.c.POSTGRESQL_LIVE_SQLALCHEMY,
        "API_KEY": mgr.c.API_KEY,
        "SECRET_KEY": mgr.c.SECRET_KEY,
        "ALPACA_BASE_URL": mgr.c.ALPACA_BASE_URL,
        "TABLE_ACCOUNT_CLOSED": mgr.tbl_closed,
        "TABLE_ACCOUNT_POSITIONS": mgr.tbl_live,
        "TABLE_ACCOUNT_EQUITY_FULL": mgr.tbl_equity,
        "TABLE_MARKET_SCHEDULE": mgr.tbl_market_schedule,
        "ACCOUNT_SLUG": mgr.c.ACCOUNT_SLUG,
        "CASH_FLOW_TYPES": os.getenv(
            "CASH_FLOW_TYPES",
            (
                "CSD,CSW,JNLC,ACATC,ACATS,FEE,CFEE,DIV,DIVCGL,DIVCGS,"
                "DIVNRA,DIVROC,DIVTXEX,DIVWH,INT,INTPNL"
            ),
        ),
    }
    try:
        df = await rebuild_equity_series_async(cfg, repo=mgr.repo)
        start_date = str(df["date"].min()) if df is not None and not df.empty else "-"
        end_date = str(df["date"].max()) if df is not None and not df.empty else "-"
        mgr.log.info(
            "equity rebuild: %d days from %s to %s",
            len(df.index) if df is not None else 0,
            start_date,
            end_date,
        )
    except Exception as exc:
        mgr.log.warning("equity rebuild failed: %s", exc)


async def _equity_intraday_backfill(mgr: "AccountManager") -> None:
    """
    Rebuild intraday equity by marking positions to market relative to
    the previous close and applying flows.

    Intraday equity(t) = prev_equity + delta_series(t) + flows(t).
    Open positions use yesterday's close when carried overnight.
    Intraday trades compare to their entry price, and realized PnL is
    tallied after exit.
    External flows update the balance without affecting cumulative returns.
    """
    try:
        now_dt = _utcnow()
        start_utc, session_date = mgr._session_start_utc(now_dt)
        now_ts = pd.Timestamp(now_dt)
        floor_min = now_ts.floor("min")
        last_complete = (floor_min - pd.Timedelta(seconds=5)).floor("min")
        cutoff = last_complete if last_complete >= start_utc else start_utc
        now_pydt = now_ts.to_pydatetime()

        # 1. Fetch Previous Close Equity (Anchor)
        prev_row = await mgr._db_fetchrow(
            f"""
                SELECT deposit, cumulative_return, date
                  FROM {mgr.tbl_equity}
                 WHERE date < $1
              ORDER BY date DESC
                 LIMIT 1
                """,
            session_date,
        )
        prev_equity = float(prev_row.get("deposit") or 0.0) if prev_row else 0.0
        anchor_date = prev_row.get("date") if prev_row else None
        
        try:
            prev_cum_ret = float(prev_row.get("cumulative_return") or 0.0)
            if not math.isfinite(prev_cum_ret): prev_cum_ret = 0.0
        except Exception:
            prev_cum_ret = 0.0

        idx = pd.date_range(start_utc, cutoff, freq="min", tz="UTC")
        if idx.empty: idx = pd.DatetimeIndex([start_utc])

        # 2. Fetch Closed Trades (Today)
        closed_rows = await mgr._db_fetch(
            f"""
                SELECT symbol, qty, side,
                       entry_time AT TIME ZONE 'UTC' AS entry_time,
                       exit_time  AT TIME ZONE 'UTC' AS exit_time,
                       entry_price, exit_price
                  FROM {mgr.tbl_closed}
                 WHERE exit_time::date = $1
                """,
            session_date,
        )
        closed_today = pd.DataFrame(closed_rows)

        # 3. Fetch Open Positions
        open_rows = await mgr._db_fetch(
            f"""
                SELECT symbol, qty,
                       filled_at AT TIME ZONE 'UTC' AS filled_at,
                       COALESCE(avg_px_symbol, avg_fill) AS basis
                  FROM {mgr.tbl_live}
                 WHERE qty <> 0
                """
        )
        open_df = pd.DataFrame(open_rows)
        open_syms = set(
            open_df.get("symbol", pd.Series(dtype=str))
            .dropna()
            .astype(str)
            .str.upper()
        )

        # 4. Fetch flows split into PnL vs other cash movements
        pnl_types = {
            "DIV",
            "DIVCGL",
            "DIVCGS",
            "DIVNRA",
            "DIVROC",
            "DIVTXEX",
            "DIVWH",
            "INT",
            "INTPNL",
            "FEE",
            "CFEE",
        }
        try:
            flow_rows = await mgr._db_fetch(
                f"""
                    SELECT ts AT TIME ZONE 'UTC' AS ts, amount, type
                      FROM {mgr.tbl_cash_flows}
                     WHERE ts >= $1 AND ts <= $2
                       AND (type NOT ILIKE 'FILL%' AND type NOT ILIKE 'PARTIAL_FILL%')
                  ORDER BY ts
                    """,
                start_utc.to_pydatetime(),
                now_pydt,
            )
            pnl_flows_data = []
            external_flows_data = []
            for row in flow_rows:
                atype = str(row.get("type") or "").upper()
                if atype in pnl_types:
                    pnl_flows_data.append(row)
                else:
                    external_flows_data.append(row)
            pnl_flows = pd.DataFrame(pnl_flows_data)
            ext_flows = pd.DataFrame(external_flows_data)
        except Exception:
            pnl_flows = pd.DataFrame(columns=["ts", "amount", "type"])
            ext_flows = pd.DataFrame(columns=["ts", "amount", "type"])

        # 5. Fetch Prices & Build Reference Map
        cutoff_ts = cutoff.to_pydatetime()
        syms = sorted(
            set(open_df.get("symbol", pd.Series(dtype=str)).dropna().unique())
            | set(closed_today.get("symbol", pd.Series(dtype=str)).dropna().unique())
        )
        
        px_cols: dict[str, pd.Series] = {}
        ref_prices: dict[str, float] = {}
        bar_by_symbol: dict[str, tuple[float, dt.datetime]] = {}

        if syms:
            for sym in syms:
                pre_px: float | None = None

                # Ensures the P&L delta starts where DB equity left off.
                if anchor_date:
                    try:
                        def _fetch_anchor_close() -> float | None:
                            bars = (
                                mgr.rest.get_bars(
                                    sym,
                                    TimeFrame(1, TimeFrameUnit.Day),
                                    start=anchor_date.isoformat(),
                                    end=anchor_date.isoformat(),
                                    adjustment="raw",
                                )
                                .df
                            )
                            if not bars.empty:
                                return float(bars.iloc[-1]["close"])
                            return None

                        pre_px = await asyncio.to_thread(_fetch_anchor_close)
                    except Exception as exc:
                        mgr.log.debug(
                            "Failed to fetch anchor close for %s: %s",
                            sym,
                            exc,
                        )

                if pre_px is None:
                    pre_px_row = await mgr._ts_fetchrow(
                        """
                            SELECT close
                              FROM public.alpaca_bars_1min
                             WHERE symbol = $1 AND timestamp < $2
                          ORDER BY timestamp DESC
                             LIMIT 1
                        """,
                        sym,
                        start_utc.to_pydatetime(),
                    )
                    pre_px = (
                        float(pre_px_row.get("close"))
                        if pre_px_row and pre_px_row.get("close") is not None
                        else None
                    )

                if pre_px:
                    ref_prices[sym] = pre_px

                bars_rows = await mgr._ts_fetch(
                    """
                        SELECT timestamp AT TIME ZONE 'UTC' AS ts, close
                          FROM public.alpaca_bars_1min
                        WHERE symbol = $1
                          AND timestamp >= $2
                          AND timestamp <= $3
                      ORDER BY timestamp
                    """,
                    sym,
                    start_utc.to_pydatetime(),
                    cutoff_ts,
                )
                bars = pd.DataFrame(bars_rows)

                if not bars.empty:
                    bars = (
                        bars.assign(ts=pd.to_datetime(bars["ts"], utc=True))
                        .drop_duplicates("ts")
                        .sort_values("ts")
                    )
                    last_bar = bars.iloc[-1]
                    try:
                        bar_close = float(last_bar["close"])
                        bar_ts = pd.to_datetime(
                            last_bar["ts"], utc=True
                        ).to_pydatetime()
                        bar_by_symbol[str(sym).upper()] = (
                            bar_close,
                            bar_ts,
                        )
                    except Exception:
                        pass

                    series = (
                        bars.set_index("ts")["close"]
                        .reindex(idx)
                        .ffill()
                    )

                    if pre_px:
                        series = series.fillna(pre_px)
                    else:
                        series = series.fillna(method="bfill")

                    px_cols[sym] = series
                elif pre_px:
                    px_cols[sym] = pd.Series(pre_px, index=idx)
                    try:
                        bar_by_symbol[str(sym).upper()] = (
                            float(pre_px),
                            start_utc.to_pydatetime(),
                        )
                    except Exception:
                        pass

        # 5b. Update bar reference price for open positions (DB + in-memory)
        if bar_by_symbol and open_syms:
            params = []
            for sym_key, bar in bar_by_symbol.items():
                if sym_key not in open_syms:
                    continue
                px, ts = bar
                params.append((px, ts, sym_key))
            if params:
                await mgr._db_executemany(
                    f"""
                        UPDATE {mgr.tbl_live}
                           SET bar_px = $1,
                               bar_ts = $2,
                               updated_at = now()
                         WHERE UPPER(symbol) = $3
                    """,
                    params,
                )
                for sym_key, bar in bar_by_symbol.items():
                    if sym_key not in open_syms:
                        continue
                    px, ts = bar
                    for pid in list(mgr.sym2pid.get(sym_key, set())):
                        row = mgr.state.get(pid)
                        if not row:
                            continue
                        row["bar_px"] = px
                        row["bar_ts"] = ts

        # 6. Calculate PnL Delta Series
        delta_series = pd.Series(0.0, index=idx)

        # A. Open Positions
        for _, row in open_df.iterrows():
            sym = str(row.get("symbol") or "")
            if not sym or sym not in px_cols: continue
            
            qty = float(row.get("qty") or 0.0)
            if qty == 0: continue
            
            basis = float(row.get("basis") or 0.0)
            filled_at = pd.to_datetime(row.get("filled_at"), utc=True)
            
            if filled_at < start_utc:
                if sym in ref_prices:
                    ref_px = ref_prices[sym]
                else:
                    ref_px = px_cols[sym].iloc[0] if not px_cols[sym].empty else basis
            else:
                ref_px = basis # Intraday entry
                
            filled_floor = max(filled_at.floor("min"), start_utc)
            
            diff = px_cols[sym] - ref_px
            mask = idx >= filled_floor
            delta_series = delta_series.add(diff.where(mask, 0.0) * qty, fill_value=0.0)

        # B. Closed Positions
        if not closed_today.empty:
            for _, row in closed_today.iterrows():
                sym = str(row.get("symbol") or "")
                if not sym or sym not in px_cols: continue
                
                qty = float(row.get("qty") or 0.0)
                if qty == 0: continue
                
                side = str(row.get("side") or "long").lower()
                signed = -qty if side.startswith("short") else qty
                
                entry_px = float(row.get("entry_price") or 0.0)
                exit_px = float(row.get("exit_price") or 0.0)
                
                entry_time = pd.to_datetime(row.get("entry_time"), utc=True)
                exit_time = pd.to_datetime(row.get("exit_time"), utc=True)
                
                if entry_time < start_utc:
                    if sym in ref_prices:
                        ref_px = ref_prices[sym]
                    else:
                        ref_px = entry_px 
                else:
                    ref_px = entry_px 
                
                entry_floor = max(entry_time.floor("min"), start_utc)
                exit_floor = min(exit_time.floor("min"), cutoff_ts)
                
                # 1. While Open
                if entry_floor < exit_floor:
                    diff = px_cols[sym] - ref_px
                    mask_open = (idx >= entry_floor) & (idx < exit_floor)
                    delta_series = delta_series.add(
                        diff.where(mask_open, 0.0) * signed, fill_value=0.0
                    )
                
                # 2. After Close
                realized_amt = (exit_px - ref_px) * signed
                mask_closed = idx >= exit_floor
                delta_series = delta_series.add(
                    pd.Series(realized_amt, index=idx).where(mask_closed, 0.0), 
                    fill_value=0.0
                )

        # 7. Add Flows (split PnL vs external deposits/withdrawals)
        if pnl_flows is not None and not pnl_flows.empty:
            flow_floor = (
                pd.to_datetime(pnl_flows["ts"], utc=True)
                .dt.floor("min")
            )
            fser_pnl = (
                pnl_flows.assign(_m=flow_floor)
                .groupby("_m")["amount"]
                .sum()
                .reindex(idx, fill_value=0.0)
                .cumsum()
            )
            delta_series = delta_series.add(fser_pnl, fill_value=0.0)

        # Track external flows separately for balance but not returns.
        ext_series = pd.Series(0.0, index=idx)
        if ext_flows is not None and not ext_flows.empty:
            flow_floor_ext = (
                pd.to_datetime(ext_flows["ts"], utc=True)
                .dt.floor("min")
            )
            fser_ext = (
                ext_flows.assign(_m=flow_floor_ext)
                .groupby("_m")["amount"]
                .sum()
                .reindex(idx, fill_value=0.0)
                .cumsum()
            )
            ext_series = ext_series.add(fser_ext, fill_value=0.0)

        # 8. Final Series Construction
        deposit_series = (
            prev_equity + delta_series + ext_series
        ).astype(float)

        step = max(1, int(mgr.EQUITY_INTRADAY_SEC))
        tick_idx = pd.date_range(
            start_utc, cutoff, freq=f"{step}s", tz="UTC"
        )
        if tick_idx.empty:
            tick_idx = pd.DatetimeIndex([cutoff])

        dep_ticks = deposit_series.reindex(
            tick_idx, method="ffill"
        ).ffill().astype(float)

        profit_ticks = (
            prev_equity + delta_series
        ).reindex(tick_idx, method="ffill").ffill().astype(float)

        base_cap = prev_equity if prev_equity != 0.0 else 1.0
        daily_ret = profit_ticks / base_cap - 1.0
        cum_ticks = (1.0 + prev_cum_ret) * (1.0 + daily_ret) - 1.0
        peak_ticks = dep_ticks.cummax().replace(0.0, np.nan)
        dd_ticks = (dep_ticks / peak_ticks) - 1.0
        dd_ticks = dd_ticks.fillna(0.0)

        records = [
            {
                "ts": ts.to_pydatetime(),
                "dep": float(dep_ticks.iat[i]),
                "cum": float(cum_ticks.iat[i]),
                "dd": (
                    float(dd_ticks.iat[i])
                    if math.isfinite(dd_ticks.iat[i])
                    else 0.0
                ),
                "rr": 0.0,
            }
            for i, ts in enumerate(tick_idx)
        ]

        await mgr._ensure_intraday_table()
        params = [
            (r["ts"], r["dep"], r["cum"], r["dd"], r["rr"])
            for r in records
        ]
        await mgr._db_executemany(
            f"""
                INSERT INTO {mgr.tbl_equity_intraday}
                    (ts, deposit, cumulative_return, drawdown, realised_return)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (ts) DO UPDATE SET
                    deposit = EXCLUDED.deposit,
                    cumulative_return = EXCLUDED.cumulative_return,
                    drawdown = EXCLUDED.drawdown,
                    realised_return = EXCLUDED.realised_return,
                    updated_at = now()
                """,
            params,
        )

    except Exception as exc:
        mgr.log.warning("equity_intraday_backfill failed: %s", exc)


async def _equity_worker(mgr: "AccountManager") -> None:
    while True:
        try:
            await _rebuild_equity_full(mgr)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            mgr.log.debug("equity_worker note: %s", exc)
        finally:
            await mgr._sleep_until_boundary(mgr.EQUITY_INTRADAY_SEC)


async def _cash_flows_worker(mgr: "AccountManager") -> None:
    while True:
        try:
            await mgr._ensure_intraday_table()
            got = await _bootstrap_cash_flows(mgr, max_pages=3)
            if got:
                mgr.log.info("cash_flows ingested %d activities", got)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            mgr.log.debug("cash_flows_worker note: %s", exc)
        finally:
            await asyncio.sleep(300)


async def _equity_intraday_worker(mgr: "AccountManager") -> None:
    while True:
        try:
            await _equity_intraday_backfill(mgr)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            mgr.log.debug("equity_intraday_worker note: %s", exc)
        finally:
            await mgr._sleep_until_boundary(
                mgr.EQUITY_INTRADAY_SEC, offset=5.0
            )
