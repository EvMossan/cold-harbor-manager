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
    Recompute the **entire** equity curve + all Sharpe variants and
    up-sert into Postgres.

    This is the slow path – call once per day (e.g. after market close).
    """
    # ---------- cfg essentials --------------------------------------
    engine  = create_engine(cfg["CONN_STRING_POSTGRESQL"])
    rest    = REST(cfg["API_KEY"], cfg["SECRET_KEY"], cfg["ALPACA_BASE_URL"])
    t_closed = cfg["TABLE_ACCOUNT_CLOSED"]
    t_open   = cfg["TABLE_ACCOUNT_POSITIONS"]
    t_eq     = cfg.get("TABLE_ACCOUNT_EQUITY_FULL", "account_equity_full")
    t_schedule = cfg.get("TABLE_MARKET_SCHEDULE")
    # Derive cash_flows table from equity name when not provided
    t_flows  = cfg.get("TABLE_ACCOUNT_CASH_FLOWS")
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

    windows = sorted(set(windows or cfg.get("EQUITY_WINDOWS",
                                            [10, 21, 63, 126, 252])))
    _ensure_table(engine, t_eq, windows)              # DDL (noop if exists)
    today   = dt.date.today()

    # ---------- helper sql pulls -----------------------------------
    def realised_pl_by_day(since: dt.date) -> pd.Series:
        sql = f"""
        SELECT exit_time::date AS date,
               SUM(pnl_cash)   AS realised_pl
        FROM   {t_closed}
        WHERE  exit_time::date >= :start
        GROUP  BY 1
        ORDER  BY 1"""
        ser = (
            pd.read_sql(text(sql), engine, params={"start": since})
            .set_index("date")["realised_pl"]
        )
        return ser

    def open_positions_snapshot() -> pd.DataFrame:
        sql = f"""
        SELECT symbol,
               qty,
               avg_fill,
               avg_px_symbol,
               filled_at,
               mkt_px,
               'open' AS status,
               filled_at AS entry_time,
               NULL      AS exit_time,
               CASE WHEN qty > 0 THEN 1 ELSE -1 END AS side
        FROM   {t_open}
        WHERE  qty <> 0"""
        return pd.read_sql(text(sql), engine)

    def closed_trades_since(since: dt.date) -> pd.DataFrame:
        sql = f"""
        SELECT symbol, qty, entry_price AS avg_fill,
               entry_time, exit_time,
               'closed'     AS status,
               CASE WHEN side='long' THEN 1 ELSE -1 END AS side
        FROM   {t_closed}
        WHERE  exit_time::date >= :start"""
        return pd.read_sql(text(sql), engine,
                           params={"start": since})

    # ---------- starting capital policy ---------------------------------
    # The equity curve no longer uses any explicit initial deposit
    # overrides. The starting value is derived solely from historical net
    # cash flows and P/L. Keep an explicit 0.0 here for clarity.
    init_cash = 0.0

    # ---------- gather trades --------------------------------------
    closed_df = closed_trades_since(start or dt.date.min)
    open_df   = open_positions_snapshot()

    frames = [df for df in (closed_df, open_df) if not df.empty]
    if not frames:
        trades_df = pd.DataFrame(columns=[
            "symbol", "qty", "avg_fill", "entry_time", "exit_time", "side"])
    elif len(frames) == 1:
        trades_df = frames[0].copy()
    else:
        common = set.intersection(*(set(f.columns) for f in frames))
        trades_df = pd.concat([f[list(common)].dropna(axis=1, how="all")
                               for f in frames],
                               ignore_index=True)

    raw_types = cfg.get(
        "CASH_FLOW_TYPES",
        (
            "CSD,CSW,JNLC,ACATC,ACATS,FEE,CFEE,DIV,DIVCGL,DIVCGS,"
            "DIVNRA,DIVROC,DIVTXEX,DIVWH,INT,INTPNL"
        ),
    )
    earliest = earliest_activity_date(
        engine,
        {"closed": t_closed, "cash_flows": t_flows, "open": t_open},
        cash_flow_types=raw_types,
        fallback_years=5,
    )
    realised_s = realised_pl_by_day(start or earliest)
    start_date = start or earliest

    idx = _trading_index_from_schedule(
        engine,
        t_schedule,
        start_date,
        today,
    )

    # ---------- cash flows (external) by day ------------------------
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
        flows_df = pd.read_sql(
            text(
                f"""
                SELECT ts::date AS date,
                       SUM(amount) AS net_flows
                  FROM {t_flows}
                 WHERE {where_types}
                 GROUP BY 1
                 ORDER BY 1
                """
            ),
            engine,
        )
        flows_series = flows_df.set_index("date")["net_flows"].astype(float)
        flows_s = _align_flows_to_business_days(flows_series, idx)
    except Exception:
        flows_s = pd.Series(0.0, index=idx, dtype=float)

    # ---------- unrealised P/L up to yesterday ----------------------
    def unrealised_pl_series(all_trades: pd.DataFrame,
                             start_: dt.date,
                             end_: dt.date) -> pd.Series:
        if all_trades.empty:
            return pd.Series(0.0,
                             index=pd.bdate_range(start_, end_),
                             name="unrealised_pl")
        prices = _fetch_daily_closes(rest,
                                     all_trades["symbol"].unique().tolist(),
                                     start_, end_)
        total = pd.Series(0.0, index=prices.index)
        for _, t in all_trades.iterrows():
            signed_qty = abs(t.qty) * (1 if t.side > 0 else -1)
            entry_px   = t.avg_fill
            open_from  = t.entry_time.date()
            open_to    = (t.exit_time.date() - dt.timedelta(days=1)
                          if pd.notna(t.exit_time) else end_)
            pnl = signed_qty * (prices[t.symbol] - entry_px)
            pnl.loc[: open_from - dt.timedelta(days=1)] = 0.0
            pnl.loc[open_to + dt.timedelta(days=1):]    = 0.0
            total += pnl
        return total.reindex(pd.bdate_range(start_, end_),
                             fill_value=0.0).rename("unrealised_pl")

    unreal_total = unrealised_pl_series(trades_df,
                                        start_date,
                                        today - dt.timedelta(days=1))\
                   .reindex(idx, fill_value=0.0)

    realised_full = realised_s.reindex(idx, fill_value=0.0)
    realised_cum  = realised_full.cumsum()

    live_upl = 0.0
    if not open_df.empty:
        try:
            basis = (
                open_df["avg_px_symbol"]
                .where(
                    open_df["avg_px_symbol"].notna(),
                    open_df["avg_fill"],
                )
            )
        except Exception:
            basis = open_df.get("avg_fill", 0.0)
        live_upl = (open_df["qty"] * (open_df["mkt_px"] - basis)).sum()

    df = pd.DataFrame(index=idx)
    df["realised_pl"]   = realised_full
    df["unrealised_pl"] = unreal_total

    # Use live unrealised P/L only while the current session is active
    last_session = idx[-1].date()
    now_utc = dt.datetime.now(tz=dt.timezone.utc)
    now_ny = now_utc.astimezone(ZoneInfo("America/New_York"))
    session_today = trading_session_date(now_utc)
    in_session = (
        session_today == last_session
        and now_ny.date() == session_today
        and 4 <= now_ny.hour < 20
    )
    if in_session:
        df.at[idx[-1], "unrealised_pl"] = live_upl

    # flows cumulative aligned to idx
    flows_idxed = flows_s.reindex(idx, fill_value=0.0)
    flows_cum = flows_idxed.cumsum()
    df["net_flows"] = flows_idxed
    # Deposit is built strictly from flows and P/L (no overrides).
    df["deposit"] = realised_cum + df["unrealised_pl"] + flows_cum

    # Denominator for returns: previous day's deposit. For the first
    # available day fall back to today's deposit (or 1.0 to avoid 0/0).
    cap_prev = df["deposit"].shift(1)
    cap_prev = cap_prev.fillna(df["deposit"]).replace(0.0, 1.0)

    df["realised_return"]   = df["realised_pl"] / cap_prev
    delta_unreal            = df["unrealised_pl"].diff().fillna(0.0)
    df["unrealised_return"] = delta_unreal / cap_prev
    df["daily_return"]      = df["realised_return"] + df["unrealised_return"]
    df["cumulative_return"] = (1.0 + df["daily_return"]).cumprod() - 1.0

    # Persistent drawdown (% as fraction, ≤ 0): deposit/peak - 1
    peak = df["deposit"].cummax().replace(0.0, np.nan)
    df["drawdown"] = (df["deposit"] / peak) - 1.0
    df["drawdown"] = df["drawdown"].fillna(0.0)

    rf_daily = (rf_pct / 100.0) / periods if rf_pct else 0.0
    for w in windows:
        df[f"realised_sharpe_{w}"]         = _rolling_sharpe(
            df["realised_return"],   w, rf_daily, periods, False)
        df[f"realised_smart_sharpe_{w}"]   = _rolling_sharpe(
            df["realised_return"],   w, rf_daily, periods, True)
        df[f"unrealised_sharpe_{w}"]       = _rolling_sharpe(
            df["unrealised_return"], w, rf_daily, periods, False)
        df[f"unrealised_smart_sharpe_{w}"] = _rolling_sharpe(
            df["unrealised_return"], w, rf_daily, periods, True)
        df[f"sharpe_{w}"]                  = _rolling_sharpe(
            df["daily_return"],      w, rf_daily, periods, False)
        df[f"smart_sharpe_{w}"]            = _rolling_sharpe(
            df["daily_return"],      w, rf_daily, periods, True)

    # ---------- bulk up-sert ----------------------------------------
    _snapshot_to_db(engine, t_eq, df.reset_index(names="date"), windows)
    return df.reset_index(names="date")


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
                SELECT COALESCE(SUM(pnl_cash), 0) AS pl
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
    """Async bulk upsert for equity series."""
    await _ensure_table_async(repo, table, windows)

    records = df.to_dict(orient="records")
    if not records:
        return

    cols = list(df.columns)
    placeholders = ", ".join(
        f"${i}" for i in range(1, len(cols) + 1)
    )
    sets = ", ".join(
        f"{c}=EXCLUDED.{c}" for c in cols if c != "date"
    )
    sql = (
        f"INSERT INTO {table} ({','.join(cols)}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT (date) DO UPDATE SET {sets}"
    )
    params = [
        tuple(rec[c] for c in cols)
        for rec in records
    ]
    await repo.executemany(sql, params)


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
    Async recompute of the full equity curve using asyncpg.
    """
    repo, own_repo = await _ensure_repo(cfg, repo)
    try:
        rest = REST(cfg["API_KEY"], cfg["SECRET_KEY"],
                    cfg["ALPACA_BASE_URL"])
        t_closed = cfg["TABLE_ACCOUNT_CLOSED"]
        t_open = cfg["TABLE_ACCOUNT_POSITIONS"]
        t_eq = cfg.get("TABLE_ACCOUNT_EQUITY_FULL",
                       "account_equity_full")
        t_schedule = cfg.get("TABLE_MARKET_SCHEDULE")
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

        windows = sorted(set(windows or cfg.get(
            "EQUITY_WINDOWS",
            [10, 21, 63, 126, 252],
        )))
        await _ensure_table_async(repo, t_eq, windows)
        today = dt.date.today()

        async def realised_pl_by_day(since: dt.date) -> pd.Series:
            sql = f"""
            SELECT exit_time::date AS date,
                   SUM(pnl_cash)   AS realised_pl
              FROM {t_closed}
             WHERE exit_time::date >= $1
          GROUP BY 1
          ORDER  BY 1
            """
            rows = await repo.fetch(sql, since)
            if not rows:
                return pd.Series(dtype=float)
            ser = (
                pd.DataFrame(rows)
                .set_index("date")["realised_pl"]
                .astype(float)
            )
            return ser

        async def open_positions_snapshot() -> pd.DataFrame:
            sql = f"""
            SELECT symbol,
                   qty,
                   avg_fill,
                   avg_px_symbol,
                   filled_at,
                   mkt_px,
                   'open' AS status,
                   filled_at AS entry_time,
                   NULL      AS exit_time,
                   CASE WHEN qty > 0 THEN 1 ELSE -1 END AS side
              FROM {t_open}
             WHERE qty <> 0
            """
            rows = await repo.fetch(sql)
            return pd.DataFrame(rows)

        async def closed_trades_since(
            since: dt.date,
        ) -> pd.DataFrame:
            sql = f"""
            SELECT symbol,
                   qty,
                   entry_price AS avg_fill,
                   entry_time,
                   exit_time,
                   'closed'     AS status,
                   CASE WHEN side='long' THEN 1 ELSE -1 END AS side
              FROM {t_closed}
             WHERE exit_time::date >= $1
            """
            rows = await repo.fetch(sql, since)
            return pd.DataFrame(rows)

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

        async def flows_series(idx: pd.DatetimeIndex) -> pd.Series:
            try:
                rows = await repo.fetch(
                    f"""
                    SELECT ts::date AS date,
                           SUM(amount) AS net_flows
                      FROM {t_flows}
                     WHERE {where_types}
                  GROUP BY 1
                  ORDER BY 1
                    """
                )
                if not rows:
                    return pd.Series(0.0, index=idx, dtype=float)
                flows_df = pd.DataFrame(rows)
                flows_s = (
                    flows_df.set_index("date")["net_flows"].astype(float)
                )
                return _align_flows_to_business_days(flows_s, idx)
            except Exception:
                return pd.Series(0.0, index=idx, dtype=float)

        closed_df = await closed_trades_since(start or dt.date.min)
        open_df = await open_positions_snapshot()

        frames = [df for df in (closed_df, open_df) if not df.empty]
        if not frames:
            trades_df = pd.DataFrame(
                columns=[
                    "symbol",
                    "qty",
                    "avg_fill",
                    "entry_time",
                    "exit_time",
                    "side",
                ]
            )
        elif len(frames) == 1:
            trades_df = frames[0].copy()
        else:
            common = set.intersection(*(set(f.columns) for f in frames))
            trades_df = pd.concat(
                [
                    f[list(common)].dropna(axis=1, how="all")
                    for f in frames
                ],
                ignore_index=True,
            )

        earliest = await earliest_activity_date_async(
            repo,
            {"closed": t_closed, "cash_flows": t_flows, "open": t_open},
            cash_flow_types=raw_types,
            fallback_years=5,
        )
        realised_s = await realised_pl_by_day(start or earliest)
        start_date = start or earliest

        idx = await _trading_index_from_schedule_async(
            repo,
            t_schedule,
            start_date,
            today,
        )
        flows_s = await flows_series(idx)

        def unrealised_pl_series(
            all_trades: pd.DataFrame,
            start_: dt.date,
            end_: dt.date,
        ) -> pd.Series:
            if all_trades.empty:
                return pd.Series(
                    0.0,
                    index=pd.bdate_range(start_, end_),
                    name="unrealised_pl",
                )
            prices = _fetch_daily_closes(
                rest,
                all_trades["symbol"].unique().tolist(),
                start_,
                end_,
            )
            total = pd.Series(0.0, index=prices.index)
            for _, t in all_trades.iterrows():
                signed_qty = abs(t.qty) * (1 if t.side > 0 else -1)
                entry_px = t.avg_fill
                open_from = t.entry_time.date()
                open_to = (
                    t.exit_time.date() - dt.timedelta(days=1)
                    if pd.notna(t.exit_time)
                    else end_
                )
                pnl = signed_qty * (prices[t.symbol] - entry_px)
                pnl.loc[: open_from - dt.timedelta(days=1)] = 0.0
                pnl.loc[open_to + dt.timedelta(days=1):] = 0.0
                total += pnl
            return total.reindex(
                pd.bdate_range(start_, end_),
                fill_value=0.0,
            ).rename("unrealised_pl")

        unreal_total = unrealised_pl_series(
            trades_df,
            start_date,
            today - dt.timedelta(days=1),
        ).reindex(idx, fill_value=0.0)

        realised_full = realised_s.reindex(idx, fill_value=0.0)
        realised_cum = realised_full.cumsum()

        live_upl = 0.0
        if not open_df.empty:
            try:
                basis = (
                    open_df["avg_px_symbol"]
                    .where(
                        open_df["avg_px_symbol"].notna(),
                        open_df["avg_fill"],
                    )
                )
            except Exception:
                basis = open_df.get("avg_fill", 0.0)
            live_upl = (
                open_df["qty"] * (open_df["mkt_px"] - basis)
            ).sum()

        df = pd.DataFrame(index=idx)
        df["realised_pl"] = realised_full
        df["unrealised_pl"] = unreal_total

        last_session = idx[-1].date()
        now_utc = dt.datetime.now(tz=dt.timezone.utc)
        now_ny = now_utc.astimezone(ZoneInfo("America/New_York"))
        session_today = trading_session_date(now_utc)
        in_session = (
            session_today == last_session
            and now_ny.date() == session_today
            and 4 <= now_ny.hour < 20
        )
        if in_session:
            df.at[idx[-1], "unrealised_pl"] = live_upl

        flows_idxed = flows_s.reindex(idx, fill_value=0.0)
        flows_cum = flows_idxed.cumsum()
        df["net_flows"] = flows_idxed
        df["deposit"] = (
            realised_cum + df["unrealised_pl"] + flows_cum
        )

        cap_prev = df["deposit"].shift(1)
        cap_prev = cap_prev.fillna(df["deposit"]).replace(0.0, 1.0)

        df["realised_return"] = df["realised_pl"] / cap_prev
        delta_unreal = df["unrealised_pl"].diff().fillna(0.0)
        df["unrealised_return"] = delta_unreal / cap_prev
        df["daily_return"] = (
            df["realised_return"] + df["unrealised_return"]
        )
        df["cumulative_return"] = (
            (1.0 + df["daily_return"]).cumprod() - 1.0
        )

        peak = df["deposit"].cummax().replace(0.0, np.nan)
        df["drawdown"] = (df["deposit"] / peak) - 1.0
        df["drawdown"] = df["drawdown"].fillna(0.0)

        rf_daily = (rf_pct / 100.0) / periods if rf_pct else 0.0
        for w in windows:
            df[f"realised_sharpe_{w}"] = _rolling_sharpe(
                df["realised_return"],
                w,
                rf_daily,
                periods,
                False,
            )
            df[f"realised_smart_sharpe_{w}"] = _rolling_sharpe(
                df["realised_return"],
                w,
                rf_daily,
                periods,
                True,
            )
            df[f"unrealised_sharpe_{w}"] = _rolling_sharpe(
                df["unrealised_return"],
                w,
                rf_daily,
                periods,
                False,
            )
            df[f"unrealised_smart_sharpe_{w}"] = _rolling_sharpe(
                df["unrealised_return"],
                w,
                rf_daily,
                periods,
                True,
            )
            df[f"sharpe_{w}"] = _rolling_sharpe(
                df["daily_return"],
                w,
                rf_daily,
                periods,
                False,
            )
            df[f"smart_sharpe_{w}"] = _rolling_sharpe(
                df["daily_return"],
                w,
                rf_daily,
                periods,
                True,
            )

        await _snapshot_to_db_async(
            repo,
            t_eq,
            df.reset_index(names="date"),
            windows,
        )
        return df.reset_index(names="date")
    finally:
        if own_repo:
            await repo.close()


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
            SELECT COALESCE(SUM(pnl_cash), 0) AS pl
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
            live_upl = float(
                (open_df["qty"] * (open_df["mkt_px"]
                                   - open_df["basis"])).sum()
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
