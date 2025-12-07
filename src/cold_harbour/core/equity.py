"""
Equity Core Analytics (Strict UTC / Ledger Replay).

Rebuilds the account equity curve using a strict Ledger Replay method:
Equity(t) = Cash(t) + Sum(Qty(t) * Price(t)).

Data Sources:
  - Activities: account_activities.raw_activities_{slug} (Source of Truth)
  - Prices: Alpaca Historical API
  - Schedule: market_schedule (Trading days source of truth)
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
import math
from typing import Dict, List, Optional, Any, Set
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from alpaca_trade_api.rest import REST, TimeFrame

from cold_harbour.infrastructure.db import AsyncAccountRepository

log = logging.getLogger("CoreEquity")


# ─────────────────────────────────────────────────────────────────────────────
#  Math Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _autocorr_penalty(x: np.ndarray) -> float:
    """Calculate penalty factor for Smart Sharpe based on autocorrelation."""
    if len(x) < 2:
        return 1.0
    mu = x.mean()
    if np.allclose(x, mu):
        return 1.0
    num = np.sum((x[:-1] - mu) * (x[1:] - mu))
    den = np.sum((x - mu) ** 2)
    if den == 0:
        return 1.0
    return math.sqrt(1 + 2 * abs(num / den))


def _rolling_sharpe(
    r: pd.Series,
    win: int,
    rf_daily: float,
    periods: int,
    smart: bool
) -> pd.Series:
    """Compute rolling Sharpe/Smart Sharpe ratio."""
    k = math.sqrt(periods)

    def calc(arr: np.ndarray) -> Optional[float]:
        if len(arr) < win:
            return np.nan
        ex = arr - rf_daily
        sd = arr.std(ddof=1)
        if sd == 0 or np.isnan(sd):
            return np.nan
        if smart:
            sd *= _autocorr_penalty(ex)
        if sd == 0:
            return np.nan
        return (ex.mean() / sd) * k

    return r.rolling(win, min_periods=win).apply(
        lambda a: calc(a), raw=True
    )


# ─────────────────────────────────────────────────────────────────────────────
#  Ledger Replay Logic (Strict UTC)
# ─────────────────────────────────────────────────────────────────────────────

def _calculate_daily_ledger_utc(
    activities_df: pd.DataFrame,
    price_df: pd.DataFrame,
    valid_sessions: Optional[Set[dt.date]] = None
) -> pd.DataFrame:
    """
    Reconstruct daily equity using Ledger Replay in UTC.

    Uses 'valid_sessions' (from market_schedule) to determine calculation
    days. If a day is a holiday (not in valid_sessions), it is skipped
    entirely, preventing zero-price valuation crashes.
    """
    if activities_df.empty:
        return pd.DataFrame()

    # 1. Normalize dates to UTC Date (no time)
    if activities_df['transaction_time'].dt.tz is None:
        activities_df['transaction_time'] = (
            activities_df['transaction_time'].dt.tz_localize("UTC")
        )
    else:
        activities_df['transaction_time'] = (
            activities_df['transaction_time'].dt.tz_convert("UTC")
        )

    activities_df['ledger_date'] = (
        activities_df['transaction_time'].dt.date
    )

    # 2. Setup Loop Range (UTC)
    min_date = activities_df['ledger_date'].min()
    today = dt.datetime.now(dt.timezone.utc).date()

    if valid_sessions:
        # Create a range, then intersect with valid trading sessions
        raw_range = pd.date_range(min_date, today, freq='D').date
        all_dates = sorted([d for d in raw_range if d in valid_sessions])
    else:
        # Fallback to Business Days if schedule is missing
        log.warning(
            "No market schedule provided; falling back to Business Days."
        )
        all_dates = pd.date_range(min_date, today, freq='B').date

    # 3. State
    current_cash = 0.0
    inventory: Dict[str, float] = {}
    daily_snapshots = {}

    # Define sets for flow classification
    # Dividends/Fees/Interest are part of Performance (PnL), not External Flow
    pnl_activity_types = {
        'DIV', 'DIVCGL', 'DIVCGS', 'DIVNRA', 'DIVROC',
        'DIVTXEX', 'DIVWH', 'INT', 'INTPNL', 'FEE', 'CFEE'
    }

    # Group activities by date
    acts_by_day = activities_df.groupby('ledger_date')
    activity_days = sorted(acts_by_day.groups.keys())
    act_day_idx = 0

    # 4. Replay
    for day in all_dates:
        daily_external_flow = 0.0

        # A. Apply all activities that happened since the last processed
        #    trading day up to (and including) the current trading 'day'.
        while act_day_idx < len(activity_days):
            act_date = activity_days[act_day_idx]
            if act_date > day:
                break

            # Process this batch
            day_acts = acts_by_day.get_group(act_date)
            for _, row in day_acts.iterrows():
                act_type = str(row.get('activity_type', '')).upper()
                symbol = str(row.get('symbol', '')).upper()
                qty = float(row.get('qty') or 0.0)
                price = float(row.get('price') or 0.0)
                net_amt_db = float(row.get('net_amount') or 0.0)

                if act_type in ('FILL', 'PARTIAL_FILL'):
                    side = str(row.get('side', '')).lower()
                    # Fills exchange Cash for Inventory (no impact on Equity total)
                    trade_val = abs(qty * price)
                    if side == 'buy':
                        current_cash -= trade_val
                        if symbol:
                            inventory[symbol] = (
                                inventory.get(symbol, 0.0) + qty
                            )
                    elif side in ('sell', 'sell_short'):
                        current_cash += trade_val
                        if symbol:
                            inventory[symbol] = (
                                inventory.get(symbol, 0.0) - qty
                            )
                    if symbol and abs(inventory[symbol]) < 1e-9:
                        inventory.pop(symbol, None)
                else:
                    # Non-trade flows affect cash directly
                    current_cash += net_amt_db

                    # Classify as External Flow vs PnL
                    # Deposits/Withdrawals (CSD, CSW) are External -> subtracted from Return calc
                    # Dividends/Fees are PnL -> kept in Return calc
                    if act_type not in pnl_activity_types:
                        daily_external_flow += net_amt_db

            act_day_idx += 1

        # B. Mark to Market (Only on this valid trading day)
        market_value = 0.0

        if day in price_df.index:
            day_prices = price_df.loc[day]
            for sym, qty in inventory.items():
                if sym in day_prices and pd.notna(day_prices[sym]):
                    market_value += qty * float(day_prices[sym])

        # C. Record Snapshot
        # 'net_flows' is crucial for calculating returns accurately (TWR)
        # preventing deposits from looking like massive profit.
        daily_snapshots[day] = {
            'deposit': current_cash + market_value,
            'cash_balance': current_cash,
            'market_value': market_value,
            'net_flows': daily_external_flow
        }

    return pd.DataFrame.from_dict(daily_snapshots, orient='index')


def _fetch_daily_closes_utc(
    rest: REST,
    symbols: List[str],
    start: dt.date,
    end: dt.date
) -> pd.DataFrame:
    """Fetch daily closes, indexed by UTC Date."""
    if not symbols:
        return pd.DataFrame()

    chunk_size = 50
    all_closes = []

    start_str = start.strftime("%Y-%m-%d")
    end_str = (end + dt.timedelta(days=1)).strftime("%Y-%m-%d")

    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i:i + chunk_size]
        try:
            bars = rest.get_bars(
                chunk,
                TimeFrame.Day,
                start=start_str,
                end=end_str,
                adjustment='all'
            ).df

            if bars.empty:
                continue

            # Normalize index to UTC Date
            bars.index = bars.index.normalize().date

            if 'symbol' in bars.columns:
                pivoted = bars.pivot_table(
                    index=bars.index,
                    columns='symbol',
                    values='close'
                )
            else:
                pivoted = pd.DataFrame(
                    bars['close'].values,
                    index=bars.index,
                    columns=chunk
                )
            all_closes.append(pivoted)

        except Exception as e:
            log.warning(f"Price fetch error for chunk: {e}")
            continue

    if not all_closes:
        return pd.DataFrame()

    final = pd.concat(all_closes, axis=1)
    # Forward fill to handle gaps
    return final.sort_index().ffill()


# ─────────────────────────────────────────────────────────────────────────────
#  DB Async Helpers
# ─────────────────────────────────────────────────────────────────────────────

async def _ensure_table_async(
    repo: AsyncAccountRepository,
    tbl: str,
    windows: List[int]
) -> None:
    """Idempotently create equity table with minimal columns."""
    base_cols = [
        "date               date PRIMARY KEY",
        "deposit            double precision",
        "cash_balance       double precision",
        "market_value       double precision",
        "daily_return       double precision",
        "cumulative_return  double precision",
        "drawdown           double precision",
        "net_flows          double precision",
        "updated_at         timestamptz DEFAULT now()",
    ]
    sharpe_cols = []
    for w in windows:
        sharpe_cols.extend([
            f"sharpe_{w}                 double precision",
            f"smart_sharpe_{w}           double precision",
        ])

    schema = tbl.split(".", 1)[-2] if "." in tbl else None
    cols_sql = ",\n    ".join(base_cols + sharpe_cols)
    ddl = f"CREATE TABLE IF NOT EXISTS {tbl} (\n    {cols_sql}\n);"

    async with repo.pool.acquire() as conn:
        async with conn.transaction():
            if schema:
                await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            await conn.execute(ddl)

            # Ensure minimal columns exist
            await conn.execute(
                f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS "
                "drawdown double precision"
            )
            await conn.execute(
                f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS "
                "cash_balance double precision"
            )
            await conn.execute(
                f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS "
                "market_value double precision"
            )
            await conn.execute(
                f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS "
                "net_flows double precision"
            )


async def _save_equity_async(
    repo: AsyncAccountRepository,
    table: str,
    df: pd.DataFrame,
    windows: List[int]
) -> None:
    """Save cleaned equity dataframe to DB."""
    if df.empty:
        return

    await _ensure_table_async(repo, table, windows)

    # Handle Index
    df_reset = df.reset_index()
    if 'index' in df_reset.columns and 'date' not in df_reset.columns:
        df_reset.rename(columns={'index': 'date'}, inplace=True)
    df_reset = df_reset.dropna(subset=['date'])

    records = df_reset.to_dict(orient='records')
    if not records:
        return

    # Filter columns
    valid_cols = {
        'date', 'deposit', 'cash_balance', 'market_value',
        'daily_return', 'cumulative_return',
        'drawdown', 'net_flows'
    }
    for w in windows:
        valid_cols.add(f"sharpe_{w}")
        valid_cols.add(f"smart_sharpe_{w}")

    cols = [c for c in df_reset.columns if c in valid_cols]
    if 'date' not in cols:
        log.error("Missing 'date' column in equity save payload!")
        return

    placeholders = ", ".join(f"${i+1}" for i in range(len(cols)))
    updates = ", ".join(f"{c}=EXCLUDED.{c}" for c in cols if c != "date")

    sql = f"""
        INSERT INTO {table} ({','.join(cols)})
        VALUES ({placeholders})
        ON CONFLICT (date) DO UPDATE SET
        {updates}, updated_at = now()
    """

    data_tuples = []
    for rec in records:
        row = []
        for c in cols:
            val = rec.get(c)
            if isinstance(val, (np.generic)):
                val = val.item()
            if pd.isna(val):
                val = None
            row.append(val)
        data_tuples.append(tuple(row))

    await repo.executemany(sql, data_tuples)


# ─────────────────────────────────────────────────────────────────────────────
#  Main Async Entrypoint
# ─────────────────────────────────────────────────────────────────────────────

async def rebuild_equity_series_async(
    cfg: Dict,
    *,
    start: dt.date | None = None,
    windows: List[int] | None = None,
    rf_pct: float = 0.0,
    periods: int = 252,
    repo: Optional[AsyncAccountRepository] = None,
) -> pd.DataFrame:
    """
    Async recompute of equity using Ledger Replay.
    Consults 'market_schedule' to avoid calculation on holidays.
    """
    if repo is None:
        repo = await AsyncAccountRepository.create(
            cfg["CONN_STRING_POSTGRESQL"]
        )
        own_repo = True
    else:
        own_repo = False

    try:
        rest = REST(cfg["API_KEY"], cfg["SECRET_KEY"], cfg["ALPACA_BASE_URL"])
        t_eq = cfg.get("TABLE_ACCOUNT_EQUITY_FULL", "account_equity_full")
        slug = cfg.get("ACCOUNT_SLUG", "default")
        t_sched = cfg.get("TABLE_MARKET_SCHEDULE", "market_schedule")

        windows = sorted(
            set(windows or cfg.get("EQUITY_WINDOWS", [10, 21, 63, 126, 252]))
        )

        # Target Raw Table
        raw_table_name = (
            f"account_activities.raw_activities_{slug}".replace("-", "_")
        )
        log.info(f"[{slug}] Rebuilding equity from: {raw_table_name}")

        # 1. Fetch Market Schedule
        valid_sessions: Set[dt.date] = set()
        try:
            sched_rows = await repo.fetch(
                f"SELECT session_date FROM {t_sched}"
            )
            for r in sched_rows:
                d = r.get("session_date")
                if d:
                    valid_sessions.add(d)
            log.info(
                f"[{slug}] Loaded {len(valid_sessions)} valid sessions."
            )
        except Exception as e:
            log.warning(
                f"[{slug}] Could not load schedule ({e}). Check holidays."
            )

        # 2. Fetch Raw Activities
        sql = f"""
            SELECT
                transaction_time,
                activity_type,
                net_amount,
                symbol,
                qty,
                price,
                side
            FROM {raw_table_name}
            ORDER BY transaction_time ASC
        """

        try:
            rows = await repo.fetch(sql)
        except Exception as e:
            log.error(f"[{slug}] DB Error fetching activities: {e}")
            return pd.DataFrame()

        if not rows:
            log.warning(f"[{slug}] No activities in {raw_table_name}")
            return pd.DataFrame()

        activities_df = pd.DataFrame(rows)
        activities_df['net_amount'] = (
            pd.to_numeric(activities_df['net_amount']).fillna(0.0)
        )
        activities_df['qty'] = (
            pd.to_numeric(activities_df['qty']).fillna(0.0)
        )
        activities_df['price'] = (
            pd.to_numeric(activities_df['price']).fillna(0.0)
        )

        # 3. Fetch Prices
        all_symbols = activities_df['symbol'].dropna().unique().tolist()
        all_symbols = [s for s in all_symbols if s and len(s) < 12]

        log.info(f"[{slug}] Fetching prices for {len(all_symbols)} symbols")
        start_date = activities_df['transaction_time'].min().date()
        end_date = dt.datetime.now(dt.timezone.utc).date()

        price_df = await asyncio.to_thread(
            _fetch_daily_closes_utc, rest, all_symbols, start_date, end_date
        )

        # 4. Ledger Replay
        log.info(f"[{slug}] Replaying ledger...")
        equity_df = await asyncio.to_thread(
            _calculate_daily_ledger_utc,
            activities_df,
            price_df,
            valid_sessions
        )

        if equity_df.empty:
            return equity_df

        # 5. Metrics Calculation (Adjusted for Net Flows)
        equity_df['deposit'] = equity_df['deposit'].astype(float)
        equity_df['prev_deposit'] = equity_df['deposit'].shift(1)
        equity_df['net_flows'] = equity_df['net_flows'].fillna(0.0)

        # CORRECT FORMULA: Daily Return = (Profit) / Start_Capital
        # Profit = End_Capital - Start_Capital - Net_Inflow
        equity_df['daily_profit'] = (
            equity_df['deposit'] - equity_df['prev_deposit'] - equity_df['net_flows']
        )

        equity_df['daily_return'] = np.where(
            equity_df['prev_deposit'] > 0,
            equity_df['daily_profit'] / equity_df['prev_deposit'],
            0.0
        )

        equity_df['cumulative_return'] = (
            (1 + equity_df['daily_return']).cumprod() - 1
        )

        peak = equity_df['deposit'].cummax()
        equity_df['drawdown'] = np.where(
            peak > 0,
            (equity_df['deposit'] / peak) - 1.0,
            0.0
        )

        # Sharpe
        rf_daily = (rf_pct / 100.0) / periods if rf_pct else 0.0
        for w in windows:
            equity_df[f"sharpe_{w}"] = _rolling_sharpe(
                equity_df["daily_return"], w, rf_daily, periods, False
            )
            equity_df[f"smart_sharpe_{w}"] = _rolling_sharpe(
                equity_df["daily_return"], w, rf_daily, periods, True
            )

        # Save
        equity_df.index.name = 'date'
        log.info(f"[{slug}] Saving {len(equity_df)} rows to {t_eq}")
        await _save_equity_async(repo, t_eq, equity_df, windows)

        return equity_df.reset_index()

    except Exception as e:
        log.exception(f"[{slug}] Fatal error in equity rebuild: {e}")
        return pd.DataFrame()
    finally:
        if own_repo:
            await repo.close()


# ─────────────────────────────────────────────────────────────────────────────
#  Synchronous Wrappers
# ─────────────────────────────────────────────────────────────────────────────

def rebuild_equity_series(cfg: Dict, **kwargs) -> pd.DataFrame:
    return asyncio.run(rebuild_equity_series_async(cfg, **kwargs))


async def update_today_row_async(
    cfg: Dict, repo: Optional[AsyncAccountRepository] = None
) -> None:
    await rebuild_equity_series_async(cfg, repo=repo)


def update_today_row(cfg: Dict) -> None:
    asyncio.run(update_today_row_async(cfg))