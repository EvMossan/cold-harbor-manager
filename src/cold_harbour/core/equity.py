# cold_harbour/equity.py
from __future__ import annotations
import datetime as dt, math
from typing import List, Optional, Dict, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from alpaca_trade_api.rest import REST, TimeFrame
from zoneinfo import ZoneInfo
from cold_harbour.core.account_utils import trading_session_date
from cold_harbour.core.account_start import earliest_activity_date, earliest_activity_date_async
from cold_harbour.infrastructure.db import AsyncAccountRepository
import asyncio


# ────────────────────────────────────────────────────────────────
# Helpers that need no config
# ────────────────────────────────────────────────────────────────
def _autocorr_penalty(x: np.ndarray) -> float:
    if len(x) < 2:
        return 1.0
    mu  = x.mean()
    num = np.sum((x[:-1] - mu) * (x[1:] - mu))
    den = np.sum((x - mu) ** 2)
    return 1.0 if den == 0 else math.sqrt(1 + 2 * abs(num / den))


def _rolling_sharpe(r: pd.Series, win: int,
                    rf_daily: float, periods: int,
                    smart: bool) -> pd.Series:
    k = math.sqrt(periods)

    def calc(arr: np.ndarray) -> Optional[float]:
        if len(arr) < win:
            return np.nan
        ex, sd = arr - rf_daily, arr.std(ddof=1)
        if sd == 0:
            return np.nan
        if smart:
            sd *= _autocorr_penalty(ex)
        return ex.mean() / sd * k

    return r.rolling(win, min_periods=win)\
            .apply(lambda a: calc(a.values))


# ────────────────────────────────────────────────────────────────
# Flow alignment helpers
# ────────────────────────────────────────────────────────────────
def _align_flows_to_business_days(
    flows: pd.Series,
    index: pd.DatetimeIndex,
) -> pd.Series:
    """Align cash flows to a provided date index.

    Each flow is rolled forward to the first date in ``index`` that is
    >= its timestamp. Weekends/holidays slide to the next trading day,
    and flows beyond the index end are skipped (handled on next rebuild
    when the index extends).
    """
    if flows.empty:
        return pd.Series(0.0, index=index, dtype=float)

    idx_dates = pd.DatetimeIndex(index.normalize())
    if idx_dates.empty:
        return pd.Series(dtype=float)

    flows_norm = pd.Series(
        flows.astype(float).values,
        index=pd.to_datetime(flows.index).normalize(),
    ).sort_index()

    result = pd.Series(0.0, index=idx_dates, dtype=float)

    for when, amount in flows_norm.items():
        pos = idx_dates.searchsorted(when, side="left")
        if pos >= len(idx_dates):
            continue
        result.iloc[pos] += float(amount)

    return result


def _trading_index_from_schedule(
    engine,
    schedule_table: Optional[str],
    start: dt.date,
    end: dt.date,
) -> pd.DatetimeIndex:
    """Return trading-date index [start, end] using the schedule table.

    Falls back to ``pd.bdate_range`` when the table is missing/empty.
    """
    if not schedule_table:
        return pd.bdate_range(start, end, name="date")

    try:
        df = pd.read_sql(
            text(
                f"""
                SELECT session_date
                  FROM {schedule_table}
                 WHERE session_date BETWEEN :start AND :end
              ORDER  BY session_date
                """
            ),
            engine,
            params={"start": start, "end": end},
        )
    except Exception:
        return pd.bdate_range(start, end, name="date")

    if df.empty:
        return pd.bdate_range(start, end, name="date")

    dates = pd.to_datetime(df["session_date"])
    return pd.DatetimeIndex(dates, name="date")


def _calculate_daily_ledger(
    activities_df: pd.DataFrame,
    price_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Reconstruct daily equity using a strict Ledger Replay method.
    Equity(t) = Cash(t) + Sum(Qty(t) * Price(t))
    """
    if activities_df.empty:
        return pd.DataFrame()

    # 1. Prepare Data
    df = activities_df.copy()
    # Normalize to NY date
    df['date'] = df['transaction_time'].dt.tz_convert(
        'America/New_York'
    ).dt.normalize()
    df = df.sort_values('transaction_time')

    # 2. Initialize State
    current_cash = 0.0
    inventory = {}  # { 'AAPL': 10.0 }
    daily_snapshots = {}

    min_date = df['date'].min()
    max_date = dt.date.today() # Replay up to today
    
    # Use business days
    all_dates = pd.date_range(min_date, max_date, freq='B')

    activities_by_day = df.groupby('date')

    # 3. Replay Loop
    for day in all_dates:
        # A. Apply Cash and Inventory Changes
        if day in activities_by_day.groups:
            day_acts = activities_by_day.get_group(day)

            for _, row in day_acts.iterrows():
                # Update Cash (Net Amount includes fees, fills cost, etc.)
                net_amt = float(row.get('net_amount') or 0.0)
                current_cash += net_amt

                # Update Inventory (Only for Fills)
                act_type = str(row.get('activity_type', '')).upper()
                if act_type in ('FILL', 'PARTIAL_FILL'):
                    symbol = str(row.get('symbol', '')).upper()
                    qty = float(row.get('qty') or 0.0)
                    side = str(row.get('side', '')).lower()

                    if symbol:
                        current_qty = inventory.get(symbol, 0.0)
                        if side == 'buy':
                            current_qty += qty
                        elif side in ('sell', 'sell_short'):
                            current_qty -= qty

                        if abs(current_qty) < 1e-9:
                            if symbol in inventory:
                                del inventory[symbol]
                        else:
                            inventory[symbol] = current_qty

        # B. Calculate Mark-to-Market Value
        market_value = 0.0
        # Find latest available price (ffill)
        price_date = day
        # Lookback up to 5 days for prices if missing
        for _ in range(5):
            if price_date in price_df.index:
                break
            price_date -= pd.Timedelta(days=1)

        if price_date in price_df.index:
            daily_prices = price_df.loc[price_date]
            for symbol, qty in inventory.items():
                price = 0.0
                try:
                    val = daily_prices.get(symbol)
                    if pd.notna(val):
                        price = float(val)
                except Exception:
                    pass
                market_value += qty * price

        # C. Record Snapshot
        total_equity = current_cash + market_value
        daily_snapshots[day] = {
            'deposit': total_equity,
            'realised_pl': 0.0,
            'unrealised_pl': 0.0,
            'cumulative_return': 0.0,
            'daily_return': 0.0,
            'drawdown': 0.0,
            'net_flows': 0.0
        }

    return pd.DataFrame.from_dict(daily_snapshots, orient='index')


# ────────────────────────────────────────────────────────────────
# DDL
# ────────────────────────────────────────────────────────────────
def _ensure_table(engine,
                  tbl: str,
                  windows: List[int]) -> None:
    """
    Create the equity table (or noop if it already exists), now including
    the cumulative_return column and persistent drawdown column.
    """
    base_cols = [
        "date               date PRIMARY KEY",
        "deposit            double precision",
        "realised_pl        double precision",
        "unrealised_pl      double precision",
        "realised_return    double precision",
        "unrealised_return  double precision",
        "daily_return       double precision",
        "cumulative_return  double precision",
        "drawdown           double precision",
        "net_flows          double precision",
        "updated_at         timestamptz DEFAULT now()"
    ]
    sharpe_cols = []
    for w in windows:
        sharpe_cols += [
            f"sharpe_{w}                 double precision",
            f"smart_sharpe_{w}           double precision",
            f"realised_sharpe_{w}        double precision",
            f"realised_smart_sharpe_{w}  double precision",
            f"unrealised_sharpe_{w}      double precision",
            f"unrealised_smart_sharpe_{w} double precision",
        ]

    # Create schema if table is schema-qualified
    schema = tbl.split(".", 1)[0] if "." in tbl else None

    ddl = (
        f"CREATE TABLE IF NOT EXISTS {tbl} (\n    "
        + ",\n    ".join(base_cols + sharpe_cols)
        + "\n);"
    )
    with engine.begin() as c:
        if schema:
            c.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        c.execute(text(ddl))
        # backfill columns when table already exists
        c.execute(
            text(
                f"ALTER TABLE {tbl} "
                f"ADD COLUMN IF NOT EXISTS net_flows double precision"
            )
        )
        c.execute(
            text(
                f"ALTER TABLE {tbl} "
                f"ADD COLUMN IF NOT EXISTS drawdown double precision"
            )
        )


async def _ensure_table_async(repo: AsyncAccountRepository,
                              tbl: str,
                              windows: List[int]) -> None:
    base_cols = [
        "date               date PRIMARY KEY",
        "deposit            double precision",
        "realised_pl        double precision",
        "unrealised_pl      double precision",
        "realised_return    double precision",
        "unrealised_return  double precision",
        "daily_return       double precision",
        "cumulative_return  double precision",
        "drawdown           double precision",
        "net_flows          double precision",
        "updated_at         timestamptz DEFAULT now()",
    ]
    sharpe_cols = []
    for w in windows:
        sharpe_cols += [
            f"sharpe_{w}                 double precision",
            f"smart_sharpe_{w}           double precision",
            f"realised_sharpe_{w}        double precision",
            f"realised_smart_sharpe_{w}  double precision",
            f"unrealised_sharpe_{w}      double precision",
            f"unrealised_smart_sharpe_{w} double precision",
        ]

    schema = tbl.split(".", 1)[0] if "." in tbl else None
    ddl = (
        f"CREATE TABLE IF NOT EXISTS {tbl} (\n    "
        + ",\n    ".join(base_cols + sharpe_cols)
        + "\n);"
    )

    async with repo.pool.acquire() as conn:
        async with conn.transaction():
            if schema:
                await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            await conn.execute(ddl)
            await conn.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS net_flows double precision")
            await conn.execute(f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS drawdown double precision")

# ────────────────────────────────────────────────────────────────
# Main heavy builder
# ────────────────────────────────────────────────────────────────
def rebuild_equity_series(
    cfg: Dict,
    *,
    start: dt.date | None = None,
    windows: List[int] | None = None,
    rf_pct: float = 0.0,
    periods: int = 252,
) -> pd.DataFrame:
    """
    Recompute the entire equity curve by replaying the ledger history.
    """
    engine = create_engine(cfg["CONN_STRING_POSTGRESQL"])
    rest = REST(cfg["API_KEY"], cfg["SECRET_KEY"], cfg["ALPACA_BASE_URL"])
    t_eq = cfg.get("TABLE_ACCOUNT_EQUITY_FULL", "account_equity_full")
    t_flows = cfg.get("TABLE_ACCOUNT_CASH_FLOWS")
    if not t_flows:
        try:
            if "." in t_eq:
                sch, base = t_eq.split(".", 1)
                base2 = (
                    base.replace("equity_full_", "cash_flows_", 1)
                    if base.startswith("equity_full_")
                    else f"cash_flows_{base}"
                )
                t_flows = f"{sch}.{base2}"
            else:
                t_flows = t_eq.replace("equity_full_", "cash_flows_", 1)
        except Exception:
            t_flows = "cash_flows"

    windows = sorted(
        set(windows or cfg.get("EQUITY_WINDOWS", [10, 21, 63, 126, 252]))
    )
    _ensure_table(engine, t_eq, windows)
    today = dt.date.today()

    print("Fetching raw activities ledger...")
    flow_columns = _fetch_flow_columns(engine, t_flows)
    select_clause, order_col = _build_activity_select_columns(flow_columns)
    raw_activities_query = text(
        f"""
        SELECT
            {select_clause}
          FROM {t_flows}
         ORDER BY {order_col} ASC
        """
    )
    activities_df = pd.read_sql(raw_activities_query, engine)
    if activities_df.empty:
        return pd.DataFrame()
    activities_df["transaction_time"] = pd.to_datetime(
        activities_df["transaction_time"], utc=True, errors="coerce"
    )
    activities_df = activities_df.dropna(subset=["transaction_time"])
    if activities_df.empty:
        return pd.DataFrame()

    activity_start = activities_df["transaction_time"].dt.date.min()
    if pd.isna(activity_start):
        activity_start = today
    start_date = start or activity_start or today

    all_symbols = [
        str(symbol).upper()
        for symbol in activities_df["symbol"].dropna().unique()
        if str(symbol).strip()
    ]

    print(f"Fetching price history for {len(all_symbols)} symbols...")
    price_df = _fetch_daily_closes(
        rest,
        all_symbols,
        activity_start,
        today,
    )

    print("Replaying ledger state...")
    equity_df = _calculate_daily_ledger(activities_df, price_df)
    if equity_df.empty:
        return pd.DataFrame()

    if start_date:
        start_ts = pd.Timestamp(start_date)
        equity_df = equity_df.loc[equity_df.index >= start_ts]
    if equity_df.empty:
        return pd.DataFrame()

    equity_df["deposit"] = equity_df["deposit"].astype(float)
    equity_df["prev_deposit"] = equity_df["deposit"].shift(1)
    equity_df["daily_return"] = np.where(
        equity_df["prev_deposit"] > 0.0,
        (equity_df["deposit"] - equity_df["prev_deposit"])
        / equity_df["prev_deposit"],
        0.0,
    )
    equity_df["realised_return"] = equity_df["daily_return"]
    equity_df["unrealised_return"] = 0.0
    equity_df["cumulative_return"] = (
        (1.0 + equity_df["daily_return"]).cumprod() - 1.0
    )
    equity_df["peak"] = equity_df["deposit"].cummax()
    equity_df["drawdown"] = (
        (equity_df["deposit"] / equity_df["peak"]) - 1.0
    )
    equity_df["drawdown"] = equity_df["drawdown"].fillna(0.0)

    rf_daily = (rf_pct / 100.0) / periods if rf_pct else 0.0
    for w in windows:
        equity_df[f"realised_sharpe_{w}"] = _rolling_sharpe(
            equity_df["realised_return"],
            w,
            rf_daily,
            periods,
            False,
        )
        equity_df[f"realised_smart_sharpe_{w}"] = _rolling_sharpe(
            equity_df["realised_return"],
            w,
            rf_daily,
            periods,
            True,
        )
        equity_df[f"unrealised_sharpe_{w}"] = _rolling_sharpe(
            equity_df["unrealised_return"],
            w,
            rf_daily,
            periods,
            False,
        )
        equity_df[f"unrealised_smart_sharpe_{w}"] = _rolling_sharpe(
            equity_df["unrealised_return"],
            w,
            rf_daily,
            periods,
            True,
        )
        equity_df[f"sharpe_{w}"] = _rolling_sharpe(
            equity_df["daily_return"],
            w,
            rf_daily,
            periods,
            False,
        )
        equity_df[f"smart_sharpe_{w}"] = _rolling_sharpe(
            equity_df["daily_return"],
            w,
            rf_daily,
            periods,
            True,
        )

    equity_df.index.name = "date"
    drop_cols = ["prev_deposit", "peak", "cash_balance", "market_value"]
    db_df = equity_df.drop(columns=drop_cols, errors="ignore")
    db_df = db_df.reset_index()
    _snapshot_to_db(engine, t_eq, db_df, windows)
    return db_df


# ────────────────────────────────────────────────────────────────
# Light intraday patch (today’s row only)
# ────────────────────────────────────────────────────────────────
def update_today_row(cfg: Dict) -> None:
    """
    Fast intraday refresh, now keeping cumulative_return in sync.
    """
    engine   = create_engine(cfg["CONN_STRING_POSTGRESQL"])
    t_eq     = cfg.get("TABLE_ACCOUNT_EQUITY_FULL", "account_equity_full")
    t_open   = cfg["TABLE_ACCOUNT_POSITIONS"]
    t_closed = cfg["TABLE_ACCOUNT_CLOSED"]
    t_schedule = cfg.get("TABLE_MARKET_SCHEDULE")
    # Derive cash_flows table from equity name when not provided
    t_flows  = cfg.get("TABLE_ACCOUNT_CASH_FLOWS")
    if not t_flows:
        eq_full = t_eq
        try:
            if "." in eq_full:
                sch, base = eq_full.split(".", 1)
                base2 = (
                    base.replace("equity_full_", "cash_flows_", 1)
                    if base.startswith("equity_full_")
                    else f"cash_flows_{base}"
                )
                t_flows = f"{sch}.{base2}"
            else:
                t_flows = eq_full.replace("equity_full_", "cash_flows_", 1)
        except Exception:
            t_flows = "cash_flows"

    windows = cfg.get("EQUITY_WINDOWS", [10, 21, 63, 126, 252])
    _ensure_table(engine, t_eq, windows)

    # Use the America/New_York trading day to align with broker UI.
    now_utc = dt.datetime.now(tz=dt.timezone.utc)
    today_cal = now_utc.date()

    if t_schedule:
        with engine.begin() as c:
            today_row = c.execute(
                text(
                    f"""
                    SELECT session_date
                      FROM {t_schedule}
                     WHERE session_date = :d
                    """
                ),
                {"d": today_cal},
            ).fetchone()
        if not today_row:
            return
        today = today_row[0]
    else:
        today = trading_session_date(now_utc)

    with engine.begin() as c:
        prev = c.execute(
            text(
                f"""
                SELECT deposit, unrealised_pl, cumulative_return
                  FROM {t_eq}
                 WHERE date < :d
              ORDER BY date DESC
                 LIMIT 1
                """
            ),
            {"d": today},
        ).fetchone()
    if not prev:
        # nothing in the table yet – rebuild full
        rebuild_equity_series(cfg)
        return
    prev_deposit, prev_unreal, prev_cum_ret = prev

    # Guard against NULL/NaN/Inf in the previous day's cumulative return
    try:
        prev_cum_ret = float(prev_cum_ret)
        if not math.isfinite(prev_cum_ret):
            prev_cum_ret = 0.0
    except Exception:
        prev_cum_ret = 0.0

    realised_today = (
        pd.read_sql(
            text(
                f"""
        SELECT COALESCE(SUM(pnl_cash_fifo), 0) AS pl
                  FROM {t_closed}
                 WHERE exit_time::date = :d
                """
            ),
            engine,
            params={"d": today},
        )["pl"].astype(float).iat[0]
    )

    # Entry-based basis (avg_fill) so intraday and daily align and are
    # immune to WAC updates during the session.
    open_df = pd.read_sql(
        text(
            f"""
            SELECT qty, avg_fill AS basis, mkt_px
            FROM   {t_open}
            WHERE  qty <> 0
            """
        ),
        engine,
    )

    live_upl = 0.0
    if not open_df.empty:
        live_upl = (
            open_df["qty"] * (open_df["mkt_px"] - open_df["basis"])
        ).sum()

    # Net flows for today (UTC dates)
    try:
        raw_types = cfg.get(
            "CASH_FLOW_TYPES",
            (
                "CSD,CSW,JNLC,ACATC,ACATS,FEE,CFEE,DIV,DIVCGL,DIVCGS,"
                "DIVNRA,DIVROC,DIVTXEX,DIVWH,INT,INTPNL"
            ),
        )
        pats = [
            t.strip().upper()
            for t in str(raw_types).split(",")
            if t.strip()
        ]
        where_types = (
            " OR ".join([f"type ILIKE '{p}%'" for p in pats]) or "FALSE"
        )
        exclusion_clause = "AND type NOT IN ('FILL', 'PARTIAL_FILL')"
        prev_bday = (pd.Timestamp(today) - pd.offsets.BDay()).date()
        start_cal = min(prev_bday, today)
        flows_df = pd.read_sql(
            text(
                f"""
                SELECT ts::date AS date,
                       SUM(amount) AS net_flows
                  FROM {t_flows}
                 WHERE ts::date >= :start
                   AND ts::date <= :end
                   AND ({where_types})
                   {exclusion_clause}
                 GROUP BY 1
                 ORDER BY 1
                """
            ),
            engine,
            params={"start": start_cal, "end": today},
        )
        flows_series = flows_df.set_index("date")["net_flows"].astype(float)
        today_idx = pd.DatetimeIndex([pd.Timestamp(today)], name="date")
        aligned = _align_flows_to_business_days(flows_series, today_idx)
        flows_today = float(aligned.iloc[-1]) if not aligned.empty else 0.0
    except Exception:
        flows_today = 0.0

    # Compute today's deposit and returns with safe denominators
    deposit_today = float(prev_deposit) + float(realised_today) + (
        float(live_upl) - float(prev_unreal)
    ) + float(flows_today)
    base = float(prev_deposit) if float(prev_deposit or 0.0) != 0.0 else 1.0
    realised_ret = float(realised_today) / base
    unreal_ret = (float(live_upl) - float(prev_unreal)) / base
    daily_ret = realised_ret + unreal_ret

    # ---------- cumulative return update ---------------------------
    cum_ret_today = (
        (1.0 + float(prev_cum_ret)) * (1.0 + float(daily_ret)) - 1.0
    )

    # Peak-to-date deposit (strictly before today) for today's drawdown
    try:
        with engine.begin() as c:
            prev_peak = c.execute(
                text(
                    f"""
                    SELECT COALESCE(MAX(deposit),0)
                      FROM {t_eq}
                     WHERE date < :d
                    """
                ),
                {"d": today},
            ).scalar() or 0.0
    except Exception:
        prev_peak = float(prev_deposit or 0.0)
    peak_today = max(float(prev_peak), float(deposit_today))
    drawdown_today = (
        0.0
        if peak_today <= 0
        else (float(deposit_today) / float(peak_today)) - 1.0
    )

    # Build row and sanitise non-finite values to NULL
    def _safe(v: float) -> float | None:
        try:
            f = float(v)
            return f if math.isfinite(f) else None
        except Exception:
            return None

    row = dict(
        date               = today,
        deposit            = _safe(deposit_today),
        realised_pl        = _safe(realised_today),
        unrealised_pl      = _safe(live_upl),
        realised_return    = _safe(realised_ret),
        unrealised_return  = _safe(unreal_ret),
        daily_return       = _safe(daily_ret),
        cumulative_return  = _safe(cum_ret_today),
        drawdown           = _safe(drawdown_today),
        net_flows          = _safe(flows_today),
        updated_at         = dt.datetime.utcnow(),
    )

    cols = row.keys()
    sets = ", ".join(
        f"{c}=EXCLUDED.{c}" for c in cols if c != "date"
    )
    with engine.begin() as c:
        c.execute(
            text(
                f"""
                INSERT INTO {t_eq} ({','.join(cols)})
                VALUES ({','.join(':'+c for c in cols)})
                ON CONFLICT (date) DO UPDATE SET {sets}
                """
            ),
            row,
        )

# ────────────────────────────────────────────────────────────────
# tiny util
# ────────────────────────────────────────────────────────────────
def _fetch_daily_closes(rest: REST,
                        symbols: list[str],
                        start: dt.date,
                        end: dt.date) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame()
    bars = rest.get_bars(symbols,
                         TimeFrame.Day,
                         start=start.isoformat(),
                         end=(end + dt.timedelta(days=1)).isoformat()).df
    if bars.empty:
        raise RuntimeError("Alpaca returned no bar data.")
    if isinstance(bars.index, pd.MultiIndex):
        closes = bars["close"].unstack(level=0)
    elif "symbol" in bars.columns:
        closes = bars.reset_index().pivot(index="timestamp",
                                          columns="symbol",
                                          values="close")
    else:
        closes = bars["close"].rename(symbols[0]).to_frame()
    closes.index = closes.index.tz_localize(None).normalize()
    return closes.sort_index().asfreq("B").ffill().loc[start:end]


def _snapshot_to_db(engine, table: str,
                    df: pd.DataFrame,
                    windows: List[int]) -> None:
    _ensure_table(engine, table, windows)
    metadata = MetaData()
    # Split schema-qualified names for SQLAlchemy Table
    if "." in table:
        schema, name = table.split(".", 1)
    else:
        schema, name = None, table
    tbl = Table(name, metadata, schema=schema, autoload_with=engine)
    cols = [c for c in df.columns if c != "date"]
    records = df.to_dict(orient="records")
    stmt = insert(tbl).values(records)
    stmt = stmt.on_conflict_do_update(
        index_elements=["date"],
        set_={c: stmt.excluded[c] for c in cols})
    with engine.begin() as c:
        c.execute(stmt)


# ────────────────────────────────────────────────────────────────
# Async variants (asyncpg + AsyncAccountRepository)
# ────────────────────────────────────────────────────────────────


async def _ensure_table_async(repo: AsyncAccountRepository,
                              tbl: str,
                              windows: List[int]) -> None:
    """Async version of table ensure with drawdown/net_flows columns."""
    base_cols = [
        "date               date PRIMARY KEY",
        "deposit            double precision",
        "realised_pl        double precision",
        "unrealised_pl      double precision",
        "realised_return    double precision",
        "unrealised_return  double precision",
        "daily_return       double precision",
        "cumulative_return  double precision",
        "drawdown           double precision",
        "net_flows          double precision",
        "updated_at         timestamptz DEFAULT now()",
    ]
    sharpe_cols = []
    for w in windows:
        sharpe_cols += [
            f"sharpe_{w}                 double precision",
            f"smart_sharpe_{w}           double precision",
            f"realised_sharpe_{w}        double precision",
            f"realised_smart_sharpe_{w}  double precision",
            f"unrealised_sharpe_{w}      double precision",
            f"unrealised_smart_sharpe_{w} double precision",
        ]

    schema = tbl.split(".", 1)[0] if "." in tbl else None
    ddl = (
        f"CREATE TABLE IF NOT EXISTS {tbl} (\n    "
        + ",\n    ".join(base_cols + sharpe_cols)
        + "\n);"
    )

    async with repo.pool.acquire() as conn:
        async with conn.transaction():
            if schema:
                await conn.execute(
                    f"CREATE SCHEMA IF NOT EXISTS {schema}"
                )
            await conn.execute(ddl)
            await conn.execute(
                f"""
                ALTER TABLE {tbl}
                ADD COLUMN IF NOT EXISTS net_flows double precision
                """
            )
            await conn.execute(
                f"""
                ALTER TABLE {tbl}
                ADD COLUMN IF NOT EXISTS drawdown double precision
                """
            )


async def _snapshot_to_db_async(repo: AsyncAccountRepository,
                                table: str,
                                df: pd.DataFrame,
                                windows: List[int]) -> None:
    await _ensure_table_async(repo, table, windows)
    records = df.to_dict(orient="records")
    if not records:
        return

    cols = list(df.columns)
    placeholders = ", ".join(f"${i}" for i in range(1, len(cols) + 1))
    sets = ", ".join(f"{c}=EXCLUDED.{c}" for c in cols if c != "date")
    sql = (
        f"INSERT INTO {table} ({','.join(cols)}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT (date) DO UPDATE SET {sets}"
    )
    # Convert pandas Timestamp to python date/datetime for asyncpg
    params = []
    for rec in records:
        row = []
        for c in cols:
            val = rec[c]
            if isinstance(val, (pd.Timestamp, np.datetime64)):
                val = val.to_pydatetime()
            if pd.isna(val):
                val = None
            row.append(val)
        params.append(tuple(row))
        
    await repo.executemany(sql, params)

def _fetch_daily_closes(rest: REST,
                        symbols: list[str],
                        start: dt.date,
                        end: dt.date) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame()
    # Chunking to avoid URL length issues
    chunk_size = 50
    all_closes = []
    
    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i:i+chunk_size]
        try:
            bars = rest.get_bars(chunk, TimeFrame.Day,
                                 start=start.isoformat(),
                                 end=(end + dt.timedelta(days=1)).isoformat()).df
            if not bars.empty:
                if "symbol" in bars.columns:
                    closes = bars.pivot_table(index="timestamp", columns="symbol", values="close")
                else:
                    # Single symbol case
                    closes = bars[["close"]].rename(columns={"close": chunk[0]})
                all_closes.append(closes)
        except Exception:
            continue
            
    if not all_closes:
        return pd.DataFrame()
        
    final = pd.concat(all_closes, axis=1)
    final.index = final.index.tz_convert('America/New_York').normalize()
    return final


async def _ensure_repo(
    cfg: Dict,
    repo: Optional[AsyncAccountRepository],
) -> Tuple[AsyncAccountRepository, bool]:
    """Return repo and flag indicating ownership."""
    if repo is not None:
        return repo, False
    conn_str = cfg["CONN_STRING_POSTGRESQL"]
    new_repo = await AsyncAccountRepository.create(conn_str)
    return new_repo, True


async def _trading_index_from_schedule_async(
    repo: AsyncAccountRepository,
    schedule_table: Optional[str],
    start: dt.date,
    end: dt.date,
) -> pd.DatetimeIndex:
    """Async trading-date index sourced from the schedule table."""
    if not schedule_table:
        return pd.bdate_range(start, end, name="date")

    try:
        rows = await repo.fetch(
            f"""
            SELECT session_date
              FROM {schedule_table}
             WHERE session_date BETWEEN $1 AND $2
          ORDER  BY session_date
            """,
            start,
            end,
        )
    except Exception:
        return pd.bdate_range(start, end, name="date")

    if not rows:
        return pd.bdate_range(start, end, name="date")

    dates = [
        r.get("session_date")
        for r in rows
        if r.get("session_date") is not None
    ]
    if not dates:
        return pd.bdate_range(start, end, name="date")

    return pd.DatetimeIndex(pd.to_datetime(dates), name="date")



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
    Async recompute of equity using Ledger Replay (Cash + Assets).
    """
    # 1. Setup
    if repo is None:
        repo = await AsyncAccountRepository.create(cfg["CONN_STRING_POSTGRESQL"])
        own_repo = True
    else:
        own_repo = False

    try:
        rest = REST(cfg["API_KEY"], cfg["SECRET_KEY"], cfg["ALPACA_BASE_URL"])
        t_eq = cfg.get("TABLE_ACCOUNT_EQUITY_FULL", "account_equity_full")
        windows = sorted(set(windows or cfg.get("EQUITY_WINDOWS", [10, 21, 63, 126, 252])))

        # 2. Determine Raw Activity Table Name
        # We derive this from the equity table name (e.g., 'accounts.equity_full_live' -> 'live')
        slug = "default"
        if "equity_full_" in t_eq:
            slug = t_eq.split("equity_full_")[-1]
        
        # NOTE: This is the critical fix. We point to the Ingester's raw table.
        raw_table = f"account_activities.raw_activities_{slug}"
        print(f"Rebuilding equity from raw ledger: {raw_table}")

        # 3. Fetch Raw Activities
        sql = f"""
            SELECT
                transaction_time,
                activity_type,
                net_amount,
                symbol,
                qty,
                side
            FROM {raw_table}
            ORDER BY transaction_time ASC
        """
        try:
            rows = await repo.fetch(sql)
        except Exception as e:
            print(f"Failed to fetch raw activities: {e}")
            return pd.DataFrame()

        if not rows:
            print("No activities found.")
            return pd.DataFrame()

        activities_df = pd.DataFrame(rows)
        # Ensure UTC and numeric types
        activities_df['transaction_time'] = pd.to_datetime(activities_df['transaction_time'], utc=True)
        activities_df['net_amount'] = pd.to_numeric(activities_df['net_amount']).fillna(0.0)
        activities_df['qty'] = pd.to_numeric(activities_df['qty']).fillna(0.0)

        # 4. Fetch History Prices
        all_symbols = activities_df['symbol'].dropna().unique().tolist()
        all_symbols = [s for s in all_symbols if s] # filter empty
        
        print(f"Fetching price history for {len(all_symbols)} symbols...")
        start_date = activities_df['transaction_time'].min().date()
        end_date = dt.date.today()
        
        price_df = await asyncio.to_thread(
            _fetch_daily_closes, rest, all_symbols, start_date, end_date
        )

        # 5. Calculate Ledger
        print("Replaying ledger...")
        equity_df = await asyncio.to_thread(
            _calculate_daily_ledger, activities_df, price_df
        )

        if equity_df.empty:
            return equity_df

        # 6. Calc Metrics
        equity_df['deposit'] = equity_df['deposit'].astype(float)
        equity_df['prev_deposit'] = equity_df['deposit'].shift(1)
        equity_df['daily_return'] = np.where(
            equity_df['prev_deposit'] > 0,
            (equity_df['deposit'] - equity_df['prev_deposit']) / equity_df['prev_deposit'],
            0.0
        )
        equity_df['cumulative_return'] = (1 + equity_df['daily_return']).cumprod() - 1
        
        peak = equity_df['deposit'].cummax()
        equity_df['drawdown'] = (equity_df['deposit'] / peak) - 1.0
        equity_df['drawdown'] = equity_df['drawdown'].fillna(0.0)

        # 7. Save
        equity_df.index.name = 'date'
        await _snapshot_to_db_async(
            repo,
            t_eq,
            equity_df.reset_index(),
            windows
        )
        
        return equity_df.reset_index()

    finally:
        if own_repo:
            await repo.close()

# Stub for synchronous support if needed elsewhere (wraps async)
def rebuild_equity_series(cfg: Dict, **kwargs) -> pd.DataFrame:
    return asyncio.run(rebuild_equity_series_async(cfg, **kwargs))

# Stub for update_today (can be left as is or updated similarly)
async def update_today_row_async(cfg: Dict, repo: Optional[AsyncAccountRepository] = None) -> None:
    # For now, just trigger a full rebuild to keep it consistent
    await rebuild_equity_series_async(cfg, repo=repo)

def update_today_row(cfg: Dict) -> None:
    asyncio.run(update_today_row_async(cfg))



async def update_today_row_async(
    cfg: Dict,
    repo: Optional[AsyncAccountRepository] = None,
) -> None:
    """Async intraday refresh of today's equity row."""

    repo, own_repo = await _ensure_repo(cfg, repo)
    try:
        t_eq = cfg.get("TABLE_ACCOUNT_EQUITY_FULL", "account_equity_full")
        t_open = cfg["TABLE_ACCOUNT_POSITIONS"]
        t_closed = cfg["TABLE_ACCOUNT_CLOSED"]
        t_schedule = cfg.get("TABLE_MARKET_SCHEDULE")
        t_flows = cfg.get("TABLE_ACCOUNT_CASH_FLOWS")
        if not t_flows:
            eq_full = t_eq
            try:
                if "." in eq_full:
                    sch, base = eq_full.split(".", 1)
                    base2 = (
                        base.replace("equity_full_", "cash_flows_", 1)
                        if base.startswith("equity_full_")
                        else f"cash_flows_{base}"
                    )
                    t_flows = f"{sch}.{base2}"
                else:
                    t_flows = eq_full.replace("equity_full_", "cash_flows_", 1)
            except Exception:
                t_flows = "cash_flows"

        windows = cfg.get("EQUITY_WINDOWS", [10, 21, 63, 126, 252])
        await _ensure_table_async(repo, t_eq, windows)

        now_utc = dt.datetime.now(tz=dt.timezone.utc)
        today_cal = now_utc.date()
        if t_schedule:
            row = await repo.fetchrow(
                f"""
                SELECT session_date
                  FROM {t_schedule}
                 WHERE session_date = $1
                """,
                today_cal,
            )
            if not row:
                return
            today = row.get("session_date")
        else:
            today = trading_session_date(now_utc)

        prev = await repo.fetchrow(
            f"""
            SELECT deposit, unrealised_pl, cumulative_return
              FROM {t_eq}
             WHERE date < $1
          ORDER BY date DESC
             LIMIT 1
            """,
            today,
        )
        if not prev:
            await rebuild_equity_series_async(cfg, repo=repo)
            return

        prev_deposit = float(prev.get("deposit") or 0.0)
        prev_unreal = float(prev.get("unrealised_pl") or 0.0)
        try:
            prev_cum_ret = float(prev.get("cumulative_return") or 0.0)
            if not math.isfinite(prev_cum_ret):
                prev_cum_ret = 0.0
        except Exception:
            prev_cum_ret = 0.0

        raw_types = cfg.get(
            "CASH_FLOW_TYPES",
            (
                "CSD,CSW,JNLC,ACATC,ACATS,FEE,CFEE,DIV,DIVCGL,"
                "DIVCGS,DIVNRA,DIVROC,DIVTXEX,DIVWH,INT,INTPNL"
            ),
        )
        pats = [
            t.strip().upper()
            for t in str(raw_types).split(",")
            if t.strip()
        ]
        where_types = (
            " OR ".join([f"type ILIKE '{p}%'" for p in pats]) or "FALSE"
        )

        realised_today_row = await repo.fetchrow(
            f"""
            SELECT COALESCE(SUM(pnl_cash_fifo), 0) AS pl
              FROM {t_closed}
             WHERE exit_time::date = $1
            """,
            today,
        )
        realised_today = float(
            (realised_today_row or {}).get("pl") or 0.0
        )

        open_rows = await repo.fetch(
            f"""
            SELECT qty, avg_fill AS basis, mkt_px
              FROM {t_open}
             WHERE qty <> 0
            """
        )
        open_df = pd.DataFrame(open_rows)
        live_upl = 0.0
        if not open_df.empty:
            basis = open_df.get("avg_fill", 0.0)
            live_upl = float(
                (
                    open_df["qty"]
                    * (open_df["mkt_px"] - basis)
                ).sum()
            )

        try:
            prev_bday = (pd.Timestamp(today) - pd.offsets.BDay()).date()
            start_cal = min(prev_bday, today)
            flow_rows = await repo.fetch(
                f"""
                SELECT ts::date AS date,
                       SUM(amount) AS net_flows
                  FROM {t_flows}
                 WHERE ts::date >= $1
                   AND ts::date <= $2
                   AND ({where_types})
                   {exclusion_clause}
              GROUP BY 1
              ORDER BY 1
                """,
                start_cal,
                today,
            )
            flows_series = (
                pd.DataFrame(flow_rows)
                .set_index("date")["net_flows"]
                .astype(float)
                if flow_rows
                else pd.Series(dtype=float)
            )
            today_idx = pd.DatetimeIndex([pd.Timestamp(today)], name="date")
            aligned = _align_flows_to_business_days(
                flows_series,
                today_idx,
            )
            flows_today = float(aligned.iloc[-1]) if not aligned.empty else 0.0
        except Exception:
            flows_today = 0.0

        deposit_today = float(prev_deposit) + float(realised_today) + (
            float(live_upl) - float(prev_unreal)
        ) + float(flows_today)
        base = (
            float(prev_deposit)
            if float(prev_deposit or 0.0) != 0.0
            else 1.0
        )
        realised_ret = float(realised_today) / base
        unreal_ret = (float(live_upl) - float(prev_unreal)) / base
        daily_ret = realised_ret + unreal_ret
        cum_ret_today = (1.0 + float(prev_cum_ret)) * (
            1.0 + float(daily_ret)
        ) - 1.0

        try:
            peak_row = await repo.fetchrow(
                f"""
                SELECT COALESCE(MAX(deposit),0) AS peak
                  FROM {t_eq}
                 WHERE date < $1
                """,
                today,
            )
            prev_peak = float((peak_row or {}).get("peak") or 0.0)
        except Exception:
            prev_peak = float(prev_deposit or 0.0)
        peak_today = max(float(prev_peak), float(deposit_today))
        dd_today = (
            0.0 if peak_today <= 0
            else (float(deposit_today) / float(peak_today)) - 1.0
        )

        def _safe(v: float) -> float | None:
            try:
                fval = float(v)
                return fval if math.isfinite(fval) else None
            except Exception:
                return None

        row = dict(
            date=today,
            deposit=_safe(deposit_today),
            realised_pl=_safe(realised_today),
            unrealised_pl=_safe(live_upl),
            realised_return=_safe(realised_ret),
            unrealised_return=_safe(unreal_ret),
            daily_return=_safe(daily_ret),
            cumulative_return=_safe(cum_ret_today),
            drawdown=_safe(dd_today),
            net_flows=_safe(flows_today),
            updated_at=dt.datetime.utcnow(),
        )

        cols = list(row.keys())
        sets = ", ".join(
            f"{c}=EXCLUDED.{c}" for c in cols if c != "date"
        )
        placeholders = ", ".join(
            f"${i}" for i in range(1, len(cols) + 1)
        )
        await repo.execute(
            f"""
            INSERT INTO {t_eq} ({','.join(cols)})
            VALUES ({placeholders})
            ON CONFLICT (date) DO UPDATE SET {sets}
            """,
            *[row[c] for c in cols],
        )
    finally:
        if own_repo:
            await repo.close()
