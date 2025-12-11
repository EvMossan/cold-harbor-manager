"""
coldharbour_manager.order_manager
==========================

Break-Even stop-loss manager for Alpaca **bracket** positions
-------------------------------------------------------------
When a strategy opens a bracket order it receives two exit legs:

* **take-profit** : fixed limit above / below entry  
* **stop-loss**   : initial “disaster” stop one gap away

After the trade starts working we would like to *eliminate tail-risk* by
dragging the stop up to **break-even (BE)** — i.e. exactly the entry price —
*but only after* price had already travelled far enough in our favour.

The manager does that continuously, outside of the strategy itself, so that
every strategy can “fire and forget” its brackets.

━━━━━━━
Trading logic
━━━━━━━━━━━━

1. **Trigger level**  
   • At signal time the breakout engine pre-computes a 30-minute target  
     `takeprofit_{R}_1_price`.  
   • A position is **armed** for BE when the *regular-hours* high/low first
     touches that trigger.

2. **Stop move conditions** (per symbol)  
   ┃ long   ┃ short ┃ explanation  
   ┣━━━━━━━━┻━━━━━━━┻━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  
   ┃ `max_high ≥ trigger` **and** `last_trade ≥ entry`  
   ┃ `min_low  ≤ trigger` **and** `last_trade ≤ entry`  → price *has reached*  
   ┃                                                     trigger **and** is
   ┃                                                     still on the “good”
   ┃                                                     side of entry.

3. **Safety rails**  
   * The stop is **never lowered**.  
   * If price has already crossed back through entry the move is skipped.  
   * Setting `FORCE_BE=1` bypasses the entry-range guard for dry-runs.

━━━━━━━
Data flow per one-minute cycle
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

live positions → symbol list
┣━ fetch matching orders (parents + legs)
┃   ┣━ slice open stop-loss legs
┃   ┗━ slice take-profit legs
┣━ for each symbol build meta:
┃     parent_id · fill_ts · entry_px · BE trigger
┣━ market-data fan-out (ThreadPool):
┃     • _extrema_for_symbol()  → max_high/min_low
┃     • last trade snapshot
┣━ decide → queue PATCH jobs
┗━ PATCH fan-out (ThreadPool) → audit to Postgres

Market-data optimiser
---------------------
`_extrema_for_symbol()` **automatically selects the coarsest resolution** that
still produces a correct high/low:

* **1-min** : fill-day after the timestamp *and* today up to “now”.  
* **1-day** : last *N* (default 30) regular sessions in between.  
* **1-week**: anything older is summarised by weekly candles.

That keeps the bar payload tiny even for weeks-old positions.

Concurrency
-----------
*Two* thread pools are used:

| Pool | What for | Default workers | Notes |
|------|----------|-----------------|-------|
| MD-fetch | 1-min/day/week bars **and** latest trades | `MAX_WORKERS` (cfg, env) | All HTTP requests are I/O-bound → cheap to over-allocate. |
| PATCH   | `PATCH /v2/orders/{id}` stop updates        | 16                      | Avoids rate-limit bursts. |

All expensive DB and network calls are done inside the workers; the main thread
only assembles / filters data frames.

Database touch-points
---------------------
* **TimescaleDB** – optional, not yet used inside the BE manager.  
* **Postgres**    – table `cfg["TABLE_BE_EVENTS"]` receives an audit row for
  *every* decision (`moved`, `skip_…`, `error_…`).  
  The table is auto-created on first run.

Configuration keys
------------------
| Key | Type | Default | Purpose |
|-----|------|---------|---------|
| `API_KEY`, `SECRET_KEY`, `ALPACA_BASE_URL` | str | — | Alpaca creds & endpoint |
| `CONN_STRING_POSTGRESQL`, `CONN_STRING_TIMESCALE` | str | — | DB connections |
| `MIN_STOP_GAP` | float | 0.01 | Exchange rule: stop must differ from entry |
| `BE_TRIGGER_R` | int   | 2    | Which TP-column to read in *breakouts* table |
| `TICK_DEFAULT` | float | 0.01 | Fallback tick size |
| `MAX_WORKERS`  | int   | 30   | Thread-pool fan-out |
| `TABLE_BE_EVENTS`, `TABLE_BREAKOUTS` | str | … | Schema/table names |

Author
------
Evgenii Mosikhin
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone, timedelta, time as dtime
from zoneinfo import ZoneInfo
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Coroutine, Dict

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from alpaca_trade_api.rest import REST, TimeFrame, TimeFrameUnit
import psycopg2

from coldharbour_manager.services.account_manager.core_logic import (
    account_analytics,
)
from coldharbour_manager.services.account_manager.loader import (
    fetch_history_meta_dates,
    load_orders_from_db,
)
from coldharbour_manager.infrastructure.db import AsyncAccountRepository

fetch_orders = account_analytics.fetch_orders
build_lot_portfolio = account_analytics.build_lot_portfolio

logging.basicConfig(
    level=logging.INFO,                       # raise to DEBUG for verboser output
    format="%(asctime)s │ %(levelname)5s │ %(message)s",
)

def prev_30m_bar(ts: datetime) -> datetime:
    """
    Return the *previous* 30-minute bar anchor in UTC.

    Example
    -------
    18:30:35.746 → 18:00:00
    18:05:12.000 → 17:30:00
    """
    ts = ts.astimezone(timezone.utc)                   # normalise to UTC
    floored = ts.replace(second=0, microsecond=0,
                        minute=(ts.minute // 30) * 30)
    return floored - timedelta(minutes=30)

def round_price(price: float) -> float:
    """
    Conform *price* to the minimum tick required by Alpaca:

    • if price < 1 USD  → tick = 0.0001   (4 dp)
    • else              → tick = 0.01     (2 dp)

    Returns the rounded price as float (already at the correct precision).
    """
    tick = 0.0001 if price < 1 else 0.01
    # round() twice to avoid issues like 0.199999 -> 0.20
    return round(round(price / tick) * tick, 4 if tick < 0.01 else 2)

# ─────────────────────────────────────────────────────────────────────────────
# OrderManager – portion up to leg refresh
# ─────────────────────────────────────────────────────────────────────────────
class BreakevenOrderManager:
    """
    Manager that, each time it is instantiated and run,
      • downloads **all** historical orders since 2025-07-01
      • materialises stop-loss / TP views in memory
      • (later) will act on live positions & bars
    """

    # ------------------------------------------------------------------
    # 0. Construction & housekeeping
    # ------------------------------------------------------------------
    def __init__(self, cfg: Dict[str, Any]):
        """
        Parameters
        ----------
        cfg
            Must provide at least:
              API_KEY, SECRET_KEY, ALPACA_BASE_URL,
              CONN_STRING_POSTGRESQL
        """
        self.cfg = cfg
        self.api = REST(cfg["API_KEY"], cfg["SECRET_KEY"], cfg["ALPACA_BASE_URL"])

        # Databases
        self.pg = psycopg2.connect(cfg["CONN_STRING_POSTGRESQL"])  # logs
        self.pg_ts = psycopg2.connect(cfg["CONN_STRING_TIMESCALE"])  # bars
        self.pg.autocommit = True
        self.pg_ts.autocommit = True

        # data cache refreshed every run
        self.orders_df: pd.DataFrame = pd.DataFrame()

        self.log = logging.getLogger("BE_Manager")
        self.log.info("⇢ OrderManager initialised")

        self.slug = cfg.get("ACCOUNT_SLUG", "default")
        self.dry_run = cfg.get("DRY_RUN", False)
        if self.dry_run:
            self.log.warning("⚠️ DRY RUN MODE. No orders will be sent.")

        self.repo: AsyncAccountRepository | None = None

    def close(self) -> None | Coroutine[Any, Any, None]:
        """Close database handles and optionally await repo close.

        If an event loop is running the coroutine is returned so callers can
        await it.
        """
        self.pg.close()
        self.pg_ts.close()
        if not self.repo:
            return None
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(self.repo.close())
        else:
            return self.repo.close()
        return None

    async def _fetch_portfolio_state(self) -> pd.DataFrame:
        """
        Load orders (DB → fallback API) and build the lot portfolio.
        Optimized to use stored history start date.

        Returns the DataFrame with every active lot.
        """
        if not self.repo:
            self.repo = await AsyncAccountRepository.create(
                self.cfg["CONN_STRING_POSTGRESQL"]
            )

        try:
            orders = await load_orders_from_db(
                self.repo, self.slug
            )
        except Exception as exc:
            self.log.warning("DB load failed: %s", exc)
            orders = pd.DataFrame()

        if orders.empty:
            self.log.info("Loading orders from API (Fallback)...")
            start_date = None

            # --- START DATE RESOLUTION (Double Fallback) ---
            try:
                dates = await fetch_history_meta_dates(self.repo, self.slug)
                if dates and dates[0]:
                    start_date = dates[0]
                    self.log.info(
                        "Optimized fetch: using DB meta start date %s",
                        start_date,
                    )
                else:
                    raise ValueError("DB meta returned empty/None dates")
            except Exception as db_exc:
                self.log.warning(
                    f"DB meta fetch failed: {db_exc}. Trying API detection..."
                )
                try:
                    api_dates = await asyncio.to_thread(
                        account_analytics.get_history_start_dates, self.api
                    )
                    if api_dates and api_dates[0]:
                        start_date = api_dates[0]
                        self.log.info(
                            "Optimized fetch: using API-detected start date %s",
                            start_date,
                        )
                except Exception as api_exc:
                    self.log.warning(
                        "API date detection failed: %s. Defaulting to 365 days "
                        "lookback.",
                        api_exc,
                    )

            # --- FETCH ORDERS ---
            try:
                orders = await asyncio.to_thread(
                    fetch_orders,
                    self.api,
                    start_date=start_date,
                )
            except Exception as exc:
                self.log.warning("API load failed: %s", exc)
                orders = pd.DataFrame()

        if orders.empty:
            return pd.DataFrame()

        self.orders_df = orders

        try:
            portfolio = await asyncio.to_thread(
                build_lot_portfolio, orders, api=self.api
            )
        except Exception as exc:
            self.log.warning("Portfolio build failed: %s", exc)
            return pd.DataFrame()

        return portfolio

    def fetch_be_trigger_price(self, symbol: str, created_at: datetime) -> float | None:
        """
        Look up *takeprofit_{BE_TRIGGER_R}_1_price* in `TABLE_BREAKOUTS`
        for the (symbol, 30-min-timestamp) that corresponds to this order.

        Returns the price as float, or **None** if no matching breakout row.
        """
        ts_floor = prev_30m_bar(created_at)
        self.log.debug(">> DEBUG: created_at=%s  prev30m=%s", created_at, ts_floor)
        tp_col   = f"takeprofit_{self.cfg['BE_TRIGGER_R']}_1_price"

        sql = (
            f"SELECT {tp_col} FROM {self.cfg['TABLE_BREAKOUTS']} "
            f"WHERE symbol = %s AND timestamp = %s LIMIT 1"
        )

        with self.pg.cursor() as cur:
            cur.execute(sql, (symbol, ts_floor))
            row = cur.fetchone()

        if row:
            price = float(row[0])
            self.log.debug("Trigger price %s @ %.2f", symbol, price)
            return price

        self.log.warning("No breakout row for %s @ %s", symbol, ts_floor)
        return None
    
    def _last_trade_for_symbol(self, sym: str) -> tuple[str, float | None]:
        """
        Worker run in a thread: return (symbol, last_trade_price or None).
        Falls back to snapshot close if real-time helpers are missing.
        """
        try:
            if hasattr(self.api, "get_last_trade"):          # older SDK
                price = self.api.get_last_trade(sym).price
            elif hasattr(self.api, "get_latest_trade"):      # newer SDK
                price = self.api.get_latest_trade(sym).price
            else:                                            # snapshot fallback
                price = self.api.get_snapshot(sym).last_trade.price
            return sym, float(price)
        except Exception as exc:
            self.log.warning("last-trade fetch failed for %s: %s", sym, exc)
            return sym, None

    # ------------------------------------------------------------------
    def _fetch_last_trade_prices_parallel(
        self,
        symbols: list[str],
        *,
        max_workers: int = 32,
    ) -> dict[str, float]:
        """
        Fan-out wrapper around `_last_trade_for_symbol`.  Returns the same
        {symbol: price} mapping the single-threaded version produced.
        """
        prices: dict[str, float] = {}
        max_workers = max(1, min(max_workers, len(symbols)))

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(self._last_trade_for_symbol, sym): sym for sym in symbols}
            for fut in as_completed(futures):
                sym, px = fut.result()
                if px is not None:
                    prices[sym] = px

        return prices
    
    # ───────────────────────────────────────────────────────────────────
    # 2a. Parallel extrema helpers   (Step-1)
    # ───────────────────────────────────────────────────────────────────
    def _extrema_for_symbol(self, sym: str, since_ts: datetime) -> tuple[str, float, float]:
        """
        High/low from `since_ts` → now, with automatic resolution:

        • 1-min bars:   fill-day (after entry) and today
        • 1-day bars:   latest 30 sessions in between
        • 1-week bars:  anything older than that
        """
        try:
            ny   = ZoneInfo("America/New_York")
            now  = datetime.utcnow().replace(tzinfo=timezone.utc)
            today = now.astimezone(ny).date()
            fill  = since_ts.astimezone(ny)

            DAILY_WINDOW = 30

            highs, lows = [], []

            # ── 1) fill-day minute slice (only if filled after 09:30) ──────────
            if fill.date() < today and fill.time() > dtime(9, 30):
                end_fill = datetime.combine(fill.date(), dtime(16, 0), tzinfo=ny)
                bars = self.api.get_bars(
                    sym, TimeFrame(1, TimeFrameUnit.Minute),
                    start=since_ts.isoformat(),
                    end=end_fill.astimezone(timezone.utc).isoformat(),
                    adjustment="all", feed="sip"
                ).df
                if not bars.empty:
                    bars.index = bars.index.tz_convert(ny)
                    intraday = bars.loc[
                        (bars.index.time >= dtime(9, 30)) &
                        (bars.index.time <= dtime(16, 0))
                    ]
                    if not intraday.empty:
                        highs.append(intraday["high"].max())
                        lows.append(intraday["low"].min())

            # determine cutoff dates -------------------------------------------
            first_day_after_fill = (fill.date() if fill.time() == dtime(9, 30)
                                    else fill.date() + timedelta(days=1))
            last_daily_day  = today - timedelta(days=1)
            first_weekly_day = first_day_after_fill
            if (last_daily_day - first_day_after_fill).days + 1 > DAILY_WINDOW:
                first_daily_day  = last_daily_day - timedelta(days=DAILY_WINDOW - 1)
                first_weekly_day = first_day_after_fill
            else:
                first_daily_day  = first_day_after_fill
                first_weekly_day = None  # no weekly layer needed

            # ── 2) older history → weekly bars (if any) ───────────────────────
            if first_weekly_day and first_weekly_day < first_daily_day:
                weekly = self.api.get_bars(
                    sym, TimeFrame(1, TimeFrameUnit.Week),
                    start=first_weekly_day.isoformat(),
                    end=(first_daily_day - timedelta(days=1)).isoformat(),
                    adjustment="all", feed="sip"
                ).df
                if not weekly.empty:
                    highs.append(weekly["high"].max())
                    lows.append(weekly["low"].min())

            # ── 3) recent sessions → daily bars (≤ DAILY_WINDOW) ──────────────
            if first_daily_day <= last_daily_day:
                daily = self.api.get_bars(
                    sym, TimeFrame(1, TimeFrameUnit.Day),
                    start=first_daily_day.isoformat(),
                    end=last_daily_day.isoformat(),
                    adjustment="all", feed="sip"
                ).df
                if not daily.empty:
                    highs.append(daily["high"].max())
                    lows.append(daily["low"].min())

            # ── 4) today 09:30 → now (minute bars) ────────────────────────────
            start_today = datetime.combine(today, dtime(9, 30), tzinfo=ny)
            if now > start_today.astimezone(timezone.utc):
                today_bars = self.api.get_bars(
                    sym, TimeFrame(1, TimeFrameUnit.Minute),
                    start=start_today.astimezone(timezone.utc).isoformat(),
                    end=now.isoformat(),
                    adjustment="all", feed="sip"
                ).df
                if not today_bars.empty:
                    today_bars.index = today_bars.index.tz_convert(ny)
                    rth = today_bars.loc[today_bars.index.time <= dtime(16, 0)]
                    if not rth.empty:
                        highs.append(rth["high"].max())
                        lows.append(rth["low"].min())

            # ── return (or safe fallback) ─────────────────────────────────────
            if not highs:
                return sym, 0.0, 1e9

            return sym, float(max(highs)), float(min(lows))

        except Exception as exc:
            self.log.warning("extrema fetch failed for %s: %s", sym, exc)
            return sym, 0.0, 1e9

    # ------------------------------------------------------------------
    def _fetch_price_extrema_parallel(
        self,
        symbols: list[str],
        since: dict[str, datetime],
        *,
        max_workers: int = 32,
    ) -> pd.DataFrame:
        """
        Thread-pool wrapper around `_extrema_for_symbol`.  Returns the same
        DataFrame shape as the old single-threaded fetcher.
        """
        rows: list[tuple[str, float, float]] = []

        with ThreadPoolExecutor(
            max_workers=min(max_workers, len(symbols))) as pool:
            futures = {
                pool.submit(self._extrema_for_symbol, sym, since[sym]): sym
                for sym in symbols
            }
            for fut in as_completed(futures):
                rows.append(fut.result())

        return pd.DataFrame(rows, columns=["symbol", "max_high", "min_low"])

    # ------------------------------------------------------------------
    def tick_size(self, symbol: str) -> float:
        """
        Tick increment.  For US equities 0.01 is sufficient; cfg override wins.
        """
        return float(self.cfg.get("TICK_DEFAULT", 0.01))

    # ───────────────────────────────────────────────────────────────────
    # 3. Stop-loss replacement helpers
    # ───────────────────────────────────────────────────────────────────
    # ------------------------------------------------------------------
    def replace_stop_loss(self, child_id: str, new_px_raw: float, symbol: str) -> bool:
        """
        Patch the active stop-loss child leg to new_px, honouring the leg’s
        true order_type:

        • stop_limit  → send stop_price *and* limit_price
        • stop        → send stop_price only
        """
        new_px = round_price(new_px_raw)

        if self.dry_run:
            self.log.info(
                "[DRY RUN] Would PATCH %s %s → %.4f",
                symbol,
                child_id,
                new_px,
            )
            return True

        # find the leg row so we know which style it is
        leg = self.orders_df[self.orders_df["id"] == child_id]
        if leg.empty:
            self.log.error("replace_stop_loss: child_id %s not found in cache", child_id)
            return False

        # 'order_type' present for parents, 'type' for some children – check both
        kind = (leg["order_type"].iloc[0] or leg["type"].iloc[0]).lower()

        params = dict(stop_price=new_px)
        if kind == "stop_limit":          # only then add limit_price
            params["limit_price"] = new_px

        self.log.debug("PATCH %s → %.4f (%s)", child_id, new_px, kind)

        try:
            self.api.replace_order(order_id=child_id, **params)
            return True
        except Exception as exc:
            self.log.error("PATCH FAIL id=%s err=%s", child_id, exc)
            return False
        
    def _tightest_legal_stop(self, entry_px: float, is_long: bool) -> float:
        """
        Closest stop Alpaca will accept.

            • LONG  → stop ≤ entry − gap
            • SHORT → stop ≥ entry + gap
        """
        bp = Decimal(str(entry_px))                    # entry as Decimal

        # cfg value may be str, int, or float → normalise then bound-check
        gap_cfg = Decimal(str(self.cfg.get("MIN_STOP_GAP", 0.01)))
        min_gap = Decimal("0.01") if entry_px >= 1 else Decimal("0.0001")
        gap = max(gap_cfg, min_gap)                    # still a Decimal

        raw = bp - gap if is_long else bp + gap        # all-Decimal arithmetic

        # snap to tick grid
        decimals = 4 if entry_px < 1 else 2
        tick = Decimal(f"1e-{decimals}")
        legal = (raw / tick).to_integral_value(ROUND_HALF_UP) * tick

        return float(legal) 
        
    def _patch_stop_worker(
        self,
        child_id : str,
        new_px   : float,
        sym      : str,
        parent_id: str,
        old_stop : float,
        mkt_note : str,
    ) -> tuple[str, bool]:
        """
        Try to replace the stop-loss on the *child* leg and return:

            (parent_id,  True/False)

        so the caller can mark exactly which parent moved.
        """
        moved = self.replace_stop_loss(child_id, new_px, sym)

        # ---------- structured audit message ------------------------
        self.log_event(
            "STOP_MOVED" if moved else "ERROR_REPLACE",
            sym,
            parent_id,
            old = old_stop,
            new = new_px,
            note = f"child_id={child_id} {mkt_note}",
        )

        # ---------- human console line ------------------------------
        tag = "✅" if moved else "✗ "
        self.log.info("%s %s stop ⇒ %.4f", tag, sym, new_px)

        return parent_id, moved

    # ------------------------------------------------------------------
    def _apply_replacements_parallel(
        self,
        jobs: list[dict],
        *,
        max_workers: int = 16,
    ) -> set[str]:
        """
        Fire all PATCH requests concurrently and return **the set of
        parent_ids whose stops were really moved**.
        """
        if not jobs:
            return set()

        moved_parent_ids: set[str] = set()

        with ThreadPoolExecutor(max_workers=min(max_workers, len(jobs))) as pool:
            fut2pid = {
                pool.submit(self._patch_stop_worker, **job): job["parent_id"]
                for job in jobs
            }
            for fut in as_completed(fut2pid):
                pid, ok = fut.result()
                if ok:
                    moved_parent_ids.add(pid)

        return moved_parent_ids
    
    # ───────────────────────────────────────────────────────────────────
    # 4. Audit log
    # ───────────────────────────────────────────────────────────────────
    def _ensure_be_table(self) -> None:
        """
        CREATE TABLE IF NOT EXISTS …  (run once per OrderManager instance).
        """
        if getattr(self, "_be_table_ready", False):
            return  # already created in this process

        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.cfg["TABLE_BE_EVENTS"]} (
            ts           timestamptz DEFAULT now(),
            symbol       text,
            position_id  text,
            action       text,
            old_stop     real,
            new_stop     real,
            note         text
        );
        """
        with self.pg.cursor() as cur:
            cur.execute(ddl)
        self.pg.commit()
        self._be_table_ready = True

    # ------------------------------------------------------------------
    # 4. Audit log
    # ------------------------------------------------------------------
    def log_event(
            self,
            action      : str,
            symbol      : str,
            position_id : str,
            *,
            old : float | None = None,
            new : float | None = None,
            note: str = "",
    ) -> None:
        """
        Emit a DEBUG/INFO line only – no database insert.
        (We now persist snapshots elsewhere.)
        """
        if self.dry_run:
            dry_parts = [action, symbol, position_id]
            if old is not None and new is not None:
                dry_parts.append(f"old={old:.2f}")
                dry_parts.append(f"new={new:.2f}")
            if note:
                dry_parts.append(note)
            self.log.info("[DRY RUN EVENT] %s", " ".join(dry_parts))
            return

        # ── human-readable line ──────────────────────────────────────────
        parts = [action, symbol]
        if old is not None and new is not None:
            parts.append(f"old={old:.2f}  new={new:.2f}")
        if note:
            parts.append(note)

        msg = "  ".join(parts)
        (self.log.info if action == "STOP_MOVED" else self.log.debug)(msg)

    def _build_meta(self, portfolio_df: pd.DataFrame) -> dict[tuple[str, str], dict]:
        """
        Adapt the portfolio view into the classic (symbol,parent) meta map.
        """
        meta: dict[tuple[str, str], dict] = {}
        if portfolio_df.empty:
            return meta

        active = portfolio_df[
            (portfolio_df["Stop_Loss_ID"].notna())
            & (portfolio_df["Remaining Qty"] > 0)
        ]
        unsafe = portfolio_df[
            portfolio_df["Stop_Loss_ID"].isna()
            & (portfolio_df["Remaining Qty"] > 0)
        ]

        for _, row in unsafe.iterrows():
            self.log.warning(
                "⚠️ NO STOP LOSS: %s (Parent: %s)",
                row["Symbol"],
                row["Parent ID"],
            )

        order_index = (
            self.orders_df.set_index("id") if "id" in self.orders_df else None
        )

        for _, row in active.iterrows():
            sym = row["Symbol"]
            parent_val = row["Parent ID"]
            if pd.isna(parent_val):
                continue
            pid = str(parent_val)

            fill_dt = pd.to_datetime(row["Buy Date"], utc=True, errors="coerce")
            if pd.isna(fill_dt):
                self.log.warning(
                    "⚠️ INVALID BUY DATE: %s (Parent: %s)", sym, pid
                )
                continue

            fill_ts = fill_dt.to_pydatetime()
            if fill_ts.tzinfo is None:
                fill_ts = fill_ts.replace(tzinfo=timezone.utc)

            be_tp = self.fetch_be_trigger_price(sym, fill_ts)
            if be_tp is None:
                self.log_event("NO_TRIGGER_ROW", sym, pid)
                continue

            entry_px = float(row["Buy Price"])
            remaining_qty = float(row["Remaining Qty"])

            order_row = None
            if order_index is not None and pid in order_index.index:
                order_row = order_index.loc[pid]
                if isinstance(order_row, pd.DataFrame):
                    order_row = order_row.iloc[0]

            raw_side = ""
            if order_row is not None:
                raw_side = str(order_row.get("side", "") or "").lower()

            if raw_side == "buy":
                side = "long"
            elif raw_side == "sell":
                side = "short"
            elif raw_side in {"long", "short"}:
                side = raw_side
            else:
                side = "long"

            current_sl = row["Stop_Loss_Price"]
            current_sl = float(current_sl) if pd.notna(current_sl) else 0.0
            sl_child_id = str(row["Stop_Loss_ID"])

            meta[(sym, pid)] = {
                "parent_id": pid,
                "fill_ts": fill_ts,
                "be_tp": be_tp,
                "entry_px": entry_px,
                "current_sl": current_sl,
                "sl_child_id": sl_child_id,
                "side": side,
                "remaining_qty": remaining_qty,
            }

        return meta
    
    # ───────────────────────────────────────────────────────────────────
    #  Snapshot-table helper: one TEXT column with the full line
    # ───────────────────────────────────────────────────────────────────
    def _ensure_be_table(self) -> None:
        """
        Ensure TABLE_BE_EVENTS (= log_stop_manager) exists **with the new
        snapshot-layout**. All earlier audit columns are dropped here.
        """
        if getattr(self, "_be_table_ready", False):
            return                                  # already checked in this process

        tbl = self.cfg["TABLE_BE_EVENTS"]           # "log_stop_manager"

        ddl = f"""
        CREATE TABLE IF NOT EXISTS {tbl} (
            timestamp          timestamptz DEFAULT now(),
            parent_id   text,               -- bracket order id
            symbol      text,
            filled_at   timestamptz,
            qty         integer,
            fill_avg   real,
            sl_px       real,
            be_tp       real,
            mkt_px      real,
            moved_flag  text               -- 'OK' | 'Already' | '—'
        );
        """
        with self.pg.cursor() as cur:
            cur.execute(ddl)
        self.pg.commit()
        self._be_table_ready = True

    def _log_position_snapshot(
        self,
        positions        : pd.DataFrame,
        meta             : dict[tuple[str, str], dict],
        last_px          : dict[str, float],
        moved_parent_ids : set[str],
        already_parent_ids: set[str],
    ) -> None:
        """
        One INFO line **and** one DB row per *(symbol, parent_id)*.

        Flags:
            moved_flag = 'OK'       → stop updated in this cycle
                       = 'Already'  → was at BE before the cycle
                       = '—'        → nothing done
        """
        self._ensure_be_table()
        tbl = self.cfg["TABLE_BE_EVENTS"]

        rows = []
        pos_by_sym = positions.set_index("symbol")
        snapshot_records: list[dict[str, Any]] = []

        for (sym, pid), m in meta.items():

            # ---- parent-specific details -----------------------------
            qty = int(m.get("remaining_qty", 0))
            sl_px = m["current_sl"]
            mkt_px   = last_px.get(sym)

            if   pid in moved_parent_ids:   flag = "OK"
            elif pid in already_parent_ids: flag = "Already"
            else:                           flag = "—"

            # ---- DB payload ---------------------------------------------
            rows.append((
                pid,
                sym,
                m['fill_ts'],
                qty,
                m['entry_px'],
                sl_px,
                m['be_tp'],
                mkt_px,
                flag,
            ))
            snapshot_records.append(
                {
                    "Parent": pid[:8],
                    "Symbol": sym,
                    "Filled at": m["fill_ts"].isoformat(
                        timespec="seconds"
                    ),
                    "Qty": qty,
                    "Avg": f"{m['entry_px']:.4f}",
                    "SL": f"{sl_px:.4f}" if sl_px is not None else "n/a",
                    "TP": f"{m['be_tp']:.4f}",
                    "Mkt": (
                        f"{mkt_px:.4f}" if mkt_px is not None else "n/a"
                    ),
                    "Flag": flag,
                }
            )

        if rows:
            with self.pg.cursor() as cur:
                cur.executemany(
                    f"""INSERT INTO {tbl}
                            (parent_id, symbol, filled_at, qty,
                             fill_avg, sl_px, be_tp, mkt_px, moved_flag)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);""",
                    rows,
                )
            self.pg.commit()

        if snapshot_records:
            snapshot_frame = pd.DataFrame(snapshot_records)
            snapshot_frame.index.name = "No."
            self.log.info("\n%s", snapshot_frame.to_string())

    def _prune_old_snapshots(self, *, days: int = 30) -> None:
        """
        Keep only <days> worth of rows in TABLE_BE_EVENTS / log_stop_manager.
        Runs fast because we have an index on ts.
        """
        tbl = self.cfg["TABLE_BE_EVENTS"]
        with self.pg.cursor() as cur:
            cur.execute(
                f"""
                DELETE FROM {tbl}
                WHERE timestamp < now() - interval %s
                """,
                (f"{days} days",)
            )
            deleted = cur.rowcount
        self.pg.commit()
        if deleted:
            self.log.debug("Pruned %d snapshot rows older than %d days", deleted, days)

    def _make_job_or_log(
        self,
        sym: str,
        pos_side: str,
        meta: dict,
        mkt: float | None,
        extrema: dict,
    ) -> tuple[dict | None, str]:
        """
        Decide what to do with <sym> and emit log_events along the way.

        Returns
        -------
        (job-dict | None, status)   where status ∈ {"todo","already","noop"}
        """
        pid      = meta["parent_id"]
        entry_px = meta["entry_px"]
        is_long = pos_side.lower() == "long"
        old_stop = meta["current_sl"]
        child = meta["sl_child_id"]

        legal_be = self._tightest_legal_stop(entry_px, is_long)
        if abs(old_stop - legal_be) < 1e-9:
            self.log_event("ALREADY_AT_BE", sym, pid)
            return None, "already"

        # ── market-price guard ────────────────────────────────────────────
        if mkt is None:
            self.log_event("PRICE_UNKNOWN", sym, pid)
            return None, "noop"

        crossed = (is_long and mkt < entry_px) or (not is_long and mkt > entry_px)
        if crossed:
            self.log_event("PRICE_CROSSED_ENTRY", sym, pid,
                        note=f"mkt={mkt:.2f} entry={entry_px:.2f}")
            return None, "noop"

        # ── armed?  (hit BE trigger) ──────────────────────────────────────
        hi, lo = extrema["max_high"], extrema["min_low"]
        armed  = hi >= meta["be_tp"] if is_long else lo <= meta["be_tp"]
        if not armed:
            return None, "noop"

        # build PATCH job
        job = {
            "child_id":  child,
            "new_px":    legal_be,
            "sym":       sym,
            "parent_id": pid,
            "old_stop":  old_stop,
            "mkt_note":  f"mkt={mkt:.2f}",
        }
        return job, "todo"

    def _collect_patch_jobs(
        self,
        positions : pd.DataFrame,
        meta      : dict[tuple[str, str], dict],
        extrema   : dict[str, dict],
        last_px   : dict[str, float],
    ) -> tuple[list[dict], set[str]]:
        """
        Iterate over **every (symbol, parent_id)** pair and decide:

            • jobs               → list[dict]  (PATCH payloads)
            • already_parent_ids → set[str]

        NOTE
        ----
        *extrema* and *last_px* are keyed by **symbol only** – they were fetched
        once per ticker (Step 6).  That is fine here: every parent of the same
        symbol shares the identical high/low snapshot and last trade.
        """
        jobs: list[dict] = []
        already_pids: set[str] = set()

        pos_by_sym = positions.set_index("symbol")

        for (sym, pid), m in meta.items():
            if sym not in pos_by_sym.index:           # position disappeared mid-cycle
                continue

            job, status = self._make_job_or_log(
                sym      = sym,
                pos_side = pos_by_sym.at[sym, "side"],
                meta     = m,
                mkt      = last_px.get(sym),
                extrema  = extrema[sym],              # ← per-symbol lookup
            )

            if status == "already":
                already_pids.add(pid)
            elif status == "todo" and job:
                jobs.append(job)

        return jobs, already_pids

    # ───────────────────────────────────────────────────────────────
    #  orchestrator
    # ───────────────────────────────────────────────────────────────
    async def run_async(self) -> None:
        """
        Execute one break-even cycle (async entry point).
        """
        self.log.info("— Break-Even manager cycle start —")

        portfolio = await self._fetch_portfolio_state()
        if portfolio.empty:
            self.log.info("No active positions found.")
            return

        meta = self._build_meta(portfolio)
        if not meta:
            self.log.info("No valid brackets to manage.")
            return

        parents_total = len(meta)
        since_by_sym: dict[str, datetime] = {}
        side_by_sym: dict[str, str] = {}
        for (sym, _), m in meta.items():
            since_by_sym.setdefault(sym, m["fill_ts"])
            since_by_sym[sym] = min(since_by_sym[sym], m["fill_ts"])
            side_by_sym.setdefault(sym, m["side"])

        positions = pd.DataFrame(
            {
                "symbol": list(side_by_sym),
                "side": list(side_by_sym.values()),
            }
        )

        extrema = (
            self._fetch_price_extrema_parallel(
                symbols=list(since_by_sym),
                since=since_by_sym,
                max_workers=self.cfg.get("MAX_WORKERS"),
            )
            .set_index("symbol")
            .to_dict("index")
        )

        last_px = self._fetch_last_trade_prices_parallel(
            symbols=list(since_by_sym),
            max_workers=self.cfg.get("MAX_WORKERS"),
        )

        jobs, already_pids = self._collect_patch_jobs(
            positions, meta, extrema, last_px
        )
        moved_pids = self._apply_replacements_parallel(
            jobs, max_workers=self.cfg.get("MAX_WORKERS", 10)
        )

        self._log_position_snapshot(
            positions, meta, last_px, moved_pids, already_pids
        )

        self.log.info(
            "Cycle finished – %d parents moved, %d already @BE, "
            "%d parents reviewed across %d positions",
            len(moved_pids),
            len(already_pids),
            parents_total,
            len(positions),
        )

        self._prune_old_snapshots(days=30)
        self.log.info("— cycle complete —")


    def run(self) -> None | Coroutine[Any, Any, None]:
        """Blocking wrapper for ``run_async``.

        When an existing loop is running this returns the coroutine for
        awaiting.
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(self.run_async())
            return None
        return self.run_async()
