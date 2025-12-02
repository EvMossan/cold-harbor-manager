"""Equity helpers shared by AccountManager runtime tasks."""

from __future__ import annotations

import asyncio
import math
import os
from typing import TYPE_CHECKING, Optional, Any

import numpy as np
import pandas as pd

from cold_harbour.core.account_analytics import fetch_all_activities
from cold_harbour.core.equity import (
    rebuild_equity_series_async,
    update_today_row_async,
)
from cold_harbour.services.account_manager.utils import _utcnow

if TYPE_CHECKING:
    from cold_harbour.services.account_manager.runtime import AccountManager


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
        mgr.log.debug(
            "flows bootstrap ignoring max_pages=%s", max_pages
        )

    try:
        activities = fetch_all_activities(mgr.rest)
    except Exception:
        mgr.log.exception(
            "flows bootstrap failed to fetch activities"
        )
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

        ts_val = None
        for col in time_cols:
            if col not in activities.columns:
                continue
            candidate = row.get(col)
            if pd.isna(candidate):
                continue
            ts = pd.to_datetime(
                candidate, utc=True, errors="coerce"
            )
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

        type_val = row.get("activity_type") or row.get("type") or ""
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
        mgr.log.info("flows bootstrap: nothing to ingest")
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
    mgr.log.info(
        "flows bootstrap upserted %d rows (%d pages)",
        len(records),
        1,
    )
    return len(records)


async def _rebuild_equity_full(mgr: "AccountManager") -> None:
    """Rebuild daily equity using full cash-flows history."""
    cfg = {
        "CONN_STRING_POSTGRESQL": mgr.c.POSTGRESQL_LIVE_SQLALCHEMY,
        "API_KEY": mgr.c.API_KEY,
        "SECRET_KEY": mgr.c.SECRET_KEY,
        "ALPACA_BASE_URL": mgr.c.ALPACA_BASE_URL,
        "TABLE_ACCOUNT_CLOSED": mgr.tbl_closed,
        "TABLE_ACCOUNT_POSITIONS": mgr.tbl_live,
        "TABLE_ACCOUNT_EQUITY_FULL": mgr.tbl_equity,
        "TABLE_MARKET_SCHEDULE": mgr.tbl_market_schedule,
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
        mgr.log.info(
            "equity rebuild: %d days from %s to %s",
            len(df.index) if df is not None else 0,
            (
                str(df["date"].min())
                if df is not None and not df.empty
                else "-"
            ),
            (
                str(df["date"].max())
                if df is not None and not df.empty
                else "-"
            ),
        )
    except Exception as exc:
        mgr.log.warning("equity rebuild failed: %s", exc)


async def _equity_intraday_backfill(mgr: "AccountManager") -> None:
    """Rebuild intraday deposits for the current trading day only."""
    try:
        now_dt = _utcnow()
        start_utc, session_date = mgr._session_start_utc(now_dt)
        now_ts = pd.Timestamp(now_dt)
        floor_min = now_ts.floor("min")
        last_complete = (floor_min - pd.Timedelta(seconds=5)).floor("min")
        cutoff = last_complete if last_complete >= start_utc else start_utc
        now_pydt = now_ts.to_pydatetime()

        prev_row = await mgr._db_fetchrow(
            f"""
                SELECT deposit, unrealised_pl, cumulative_return
                  FROM {mgr.tbl_equity}
                 WHERE date < $1
              ORDER BY date DESC
                 LIMIT 1
                """,
            session_date,
        )
        if not prev_row:
            return

        prev_deposit = float(prev_row.get("deposit") or 0.0)
        prev_unreal = float(prev_row.get("unrealised_pl") or 0.0)
        try:
            prev_cum_ret = float(prev_row.get("cumulative_return") or 0.0)
            if not math.isfinite(prev_cum_ret):
                prev_cum_ret = 0.0
        except Exception:
            prev_cum_ret = 0.0

        idx = pd.date_range(start_utc, cutoff, freq="min", tz="UTC")
        if idx.empty:
            idx = pd.DatetimeIndex([start_utc])

        closed_rows = await mgr._db_fetch(
            f"""
                SELECT symbol, qty, side,
                       entry_time AT TIME ZONE 'UTC' AS entry_time,
                       exit_time  AT TIME ZONE 'UTC' AS exit_time,
                       entry_price, pnl_cash
                  FROM {mgr.tbl_closed}
                 WHERE exit_time::date = $1
                """,
            session_date,
        )
        closed_today = pd.DataFrame(closed_rows)

        open_rows = await mgr._db_fetch(
            f"""
                SELECT symbol, qty,
                       filled_at AT TIME ZONE 'UTC' AS filled_at,
                       avg_fill AS basis,
                       profit_loss,
                       profit_loss_lot
                  FROM {mgr.tbl_live}
                 WHERE qty <> 0
                """
        )
        open_df = pd.DataFrame(open_rows)

        try:
            raw_types = os.getenv(
                "CASH_FLOW_TYPES",
                (
                    "CSD,CSW,JNLC,ACATC,ACATS,FEE,CFEE,DIV,DIVCGL,"
                    "DIVCGS,DIVNRA,DIVROC,DIVTXEX,DIVWH,INT,INTPNL"
                ),
            )
            pats = [
                t.strip().upper()
                for t in raw_types.split(",")
                if t.strip()
            ]
            where_types = (
                " OR ".join([f"type ILIKE '{p}%'" for p in pats])
                or "FALSE"
            )
            flow_rows = await mgr._db_fetch(
                f"""
                    SELECT ts AT TIME ZONE 'UTC' AS ts, amount
                      FROM {mgr.tbl_cash_flows}
                     WHERE ts >= $1 AND ts <= $2
                       AND ({where_types})
                  ORDER BY ts
                    """,
                start_utc.to_pydatetime(),
                now_pydt,
            )
            flows = pd.DataFrame(flow_rows)
        except Exception:
            flows = pd.DataFrame(columns=["ts", "amount"])

        cutoff_ts = cutoff.to_pydatetime()
        if not closed_today.empty:
            closed_today = closed_today[
                pd.to_datetime(closed_today["exit_time"], utc=True)
                <= cutoff_ts
            ]

        if closed_today.empty:
            rcum = pd.Series(0.0, index=idx)
        else:
            exit_floor = pd.to_datetime(
                closed_today["exit_time"], utc=True
            ).dt.floor("min")
            rcum = (
                closed_today.assign(_m=exit_floor)
                .groupby("_m")["pnl_cash"]
                .sum()
                .reindex(idx, fill_value=0.0)
                .cumsum()
            )

        syms = sorted(
            set(
                open_df.get("symbol", pd.Series(dtype=str))
                .dropna()
                .unique()
            )
            | set(
                closed_today.get("symbol", pd.Series(dtype=str))
                .dropna()
                .unique()
            )
        )
        px_cols: dict[str, pd.Series] = {}
        if syms:
            for sym in syms:
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
                pre_px_row = await mgr._ts_fetchrow(
                    """
                        SELECT close
                          FROM public.alpaca_bars_1min
                         WHERE symbol = $1 AND timestamp <= $2
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
                bars = pd.DataFrame(bars_rows)
                if (bars is None or bars.empty) and pre_px is not None:
                    bars = pd.DataFrame(
                        {
                            "ts": [start_utc.to_pydatetime()],
                            "close": [float(pre_px)],
                        }
                    )
                elif not bars.empty and pre_px is not None:
                    first_ts = pd.to_datetime(bars["ts"].iloc[0], utc=True)
                    if first_ts > start_utc:
                        seed = pd.DataFrame(
                            {
                                "ts": [start_utc.to_pydatetime()],
                                "close": [float(pre_px)],
                            }
                        )
                        bars = pd.concat([seed, bars], ignore_index=True)

                if not bars.empty:
                    series = (
                        bars.assign(
                            ts=pd.to_datetime(bars["ts"], utc=True)
                        )
                        .drop_duplicates("ts")
                        .set_index("ts")["close"]
                        .reindex(idx)
                        .ffill()
                    )
                    px_cols[sym] = series

        upl = pd.Series(0.0, index=idx)
        for _, row in open_df.iterrows():
            sym = str(row.get("symbol") or "")
            if not sym or sym not in px_cols:
                continue
            qty = float(row.get("qty") or 0.0)
            if qty == 0:
                continue
            basis = float(row.get("basis") or 0.0)
            filled_at = pd.to_datetime(row.get("filled_at"), utc=True)
            filled_floor = max(filled_at.floor("min"), start_utc)
            diff = px_cols[sym] - basis
            mask = idx >= filled_floor
            upl = upl.add(diff.where(mask, 0.0) * qty, fill_value=0.0)

        if not closed_today.empty:
            for _, row in closed_today.iterrows():
                sym = str(row.get("symbol") or "")
                if not sym or sym not in px_cols:
                    continue
                qty = float(row.get("qty") or 0.0)
                if qty == 0:
                    continue
                side = str(row.get("side") or "long").lower()
                signed = -qty if side.startswith("short") else qty
                basis = float(row.get("entry_price") or 0.0)
                entry = pd.to_datetime(row.get("entry_time"), utc=True)
                exit_ = pd.to_datetime(row.get("exit_time"), utc=True)
                entry_floor = max(entry.floor("min"), start_utc)
                exit_floor = min(exit_.floor("min"), cutoff_ts)
                if entry_floor >= exit_floor:
                    continue
                diff = px_cols[sym] - basis
                mask = (idx >= entry_floor) & (idx < exit_floor)
                upl = upl.add(
                    diff.where(mask, 0.0) * signed, fill_value=0.0
                )

        if flows is not None and not flows.empty:
            flow_floor = pd.to_datetime(flows["ts"], utc=True).dt.floor(
                "min"
            )
            fser = (
                flows.assign(_m=flow_floor)
                .groupby("_m")["amount"]
                .sum()
                .reindex(idx, fill_value=0.0)
                .cumsum()
            )
        else:
            fser = pd.Series(0.0, index=idx)

        deposit_series = (
            prev_deposit + rcum + (upl - prev_unreal) + fser
        ).astype(float)

        step = max(1, int(mgr.EQUITY_INTRADAY_SEC))
        tick_idx = pd.date_range(
            start_utc, cutoff, freq=f"{step}s", tz="UTC"
        )
        if tick_idx.empty:
            tick_idx = pd.DatetimeIndex([cutoff])

        dep_ticks = deposit_series.reindex(tick_idx, method="ffill")
        dep_ticks = dep_ticks.ffill().astype(float)
        base_cap = prev_deposit if prev_deposit != 0.0 else 1.0
        daily_ret = dep_ticks / base_cap - 1.0
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
            await update_today_row_async(mgr.cfg, repo=mgr.repo)
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
