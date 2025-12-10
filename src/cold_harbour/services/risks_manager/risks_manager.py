"""
cold_harbour.order_manager
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

import sys, time
from datetime import datetime, timezone, timedelta, time as dtime
from zoneinfo import ZoneInfo
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict

import pandas as pd
import requests
from alpaca_trade_api.rest import REST, TimeFrame, TimeFrameUnit
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2
import logging

logging.basicConfig(
    level=logging.INFO,                       # raise to DEBUG for verboser output
    format="%(asctime)s │ %(levelname)5s │ %(message)s",
)

# ─────────────────────────────────────────────────────────────────────────────
# Helper 1: download orders for a symbol (parents + legs)
# ─────────────────────────────────────────────────────────────────────────────
def fetch_orders_for_symbols(
    api_key: str,
    secret_key: str,
    base_url: str,
    symbols: list[str] | str,
    *,
    after: str | None = None,
    until: str | None = None,
    limit: int = 500,
    show_progress: bool = False,
) -> pd.DataFrame:
    """
    Fetch every order (parents + legs) for the given `symbols`.

    Notes
    -----
    - Uses the provided `base_url` to target either LIVE or PAPER:
        e.g. "https://api.alpaca.markets" (live)
             "https://paper-api.alpaca.markets" (paper)
    - Flattens child legs and attaches `parent_id` so stop/TP rows can be
      processed alongside their parents.

    Parameters
    ----------
    api_key : str
        Alpaca API key.
    secret_key : str
        Alpaca API secret.
    base_url : str
        Trading API base URL (live or paper).
    symbols : list[str] | str
        One ticker or a list (e.g., ["AAPL","MSFT"]). Passed as a comma-
        separated string per the API spec.
    after, until : str | None
        Optional ISO-8601 bounds (inclusive/exclusive as per Alpaca API).
    limit : int
        Max rows per page (Alpaca cap = 500).
    show_progress : bool
        If True, prints a crude progress counter.

    Returns
    -------
    pd.DataFrame
        Orders (parents and legs) with basic numeric/datetime coercion applied.

    Raises
    ------
    requests.HTTPError
        If the HTTP request fails.
    RuntimeError
        If the API returns 200 but no rows were found.
    """
    # ---- 0) Prepare ----------------------------------------------------
    if isinstance(symbols, (list, tuple, set)):
        symbols = ",".join(symbols)

    base = (base_url or "https://api.alpaca.markets").rstrip("/")
    url = f"{base}/v2/orders"

    headers = {
        "accept": "application/json",
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": secret_key,
    }
    params: dict[str, Any] = {
        "status": "all",
        "nested": True,
        "limit": limit,
        "direction": "asc",
        "symbols": symbols,
    }
    if after:
        params["after"] = after
    if until:
        params["until"] = until

    # ---- 1) Paginate ---------------------------------------------------
    all_rows: list[dict] = []
    page_token: str | None = None
    page = 0

    while True:
        if page_token:
            params["page_token"] = page_token
        elif "page_token" in params:
            del params["page_token"]

        rsp = requests.get(url, headers=headers, params=params, timeout=30)
        rsp.raise_for_status()
        chunk: list[dict] = rsp.json()

        if show_progress:
            page += 1
            sys.stdout.write(
                f"\rpage {page:<3}  rows {len(chunk):<4}  total {len(all_rows)+len(chunk):<6}"
            )
            sys.stdout.flush()

        if not chunk:
            break

        # Unfold legs and attach parent_id to children.
        for order in chunk:
            all_rows.append(order)
            for leg in order.get("legs") or []:
                leg["parent_id"] = order.get("id")
                all_rows.append(leg)

        page_token = rsp.headers.get("cb-after")
        if not page_token:
            break

    if show_progress:
        print()

    # ---- 2) DataFrame + clean-up --------------------------------------
    df = pd.DataFrame(all_rows)
    if df.empty:
        raise RuntimeError(
            "fetch_orders_for_symbols() returned no rows – check credentials, "
            "base_url, or symbol list."
        )

    # Coerce numerics if present
    num_cols = ["filled_qty", "filled_avg_price", "limit_price", "stop_price", "qty"]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Coerce datetimes if present
    dt_cols = ["created_at", "submitted_at", "filled_at", "updated_at", "expires_at"]
    for col in dt_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    # Ensure 'side' exists as string column
    if "side" in df.columns:
        df["side"] = df["side"].fillna("").astype(str)
    else:
        df["side"] = ""

    return df.reset_index(drop=True)


def extract_stop_loss_legs(df: pd.DataFrame) -> pd.DataFrame:
    """Return rows representing stop-loss children of bracket orders."""
    order_types = {"stop", "stop_limit"}
    mask_parent = df["parent_id"].notna()
    mask_kind = (
        df["order_type"].str.lower().isin(order_types)
        | df["type"].str.lower().isin(order_types)
    )
    return df[mask_parent & mask_kind].copy()


def extract_take_profit_legs(df: pd.DataFrame) -> pd.DataFrame:
    """Return rows representing take-profit (limit) children of bracket orders."""
    mask_parent = df["parent_id"].notna()
    mask_kind = (df["order_type"].str.lower() == "limit") | (df["type"].str.lower() == "limit")
    return df[mask_parent & mask_kind].copy()


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
        self.pg       = psycopg2.connect(cfg["CONN_STRING_POSTGRESQL"])  # logs
        self.pg_ts    = psycopg2.connect(cfg["CONN_STRING_TIMESCALE"])   # bars
        self.pg.autocommit    = True          # ← ensures each INSERT is durable
        self.pg_ts.autocommit = True

        # data caches refreshed every run
        self.orders_df: pd.DataFrame = pd.DataFrame()
        self.stop_loss_df: pd.DataFrame = pd.DataFrame()
        self.take_profit_df: pd.DataFrame = pd.DataFrame()

        self.log = logging.getLogger("BE_Manager")   # per-instance handle
        self.log.info("⇢ OrderManager initialised")

    def close(self) -> None:
        """Close both database connections."""
        self.pg.close()
        self.pg_ts.close()

    # ------------------------------------------------------------------
    # 1. Orders universe  (FULL refresh – always stateless)
    # ------------------------------------------------------------------
    def refresh_leg_views(self) -> None:
        self.stop_loss_df   = extract_stop_loss_legs(self.orders_df)
        self.take_profit_df = extract_take_profit_legs(self.orders_df)
        self.log.info(
            "• stop-loss legs: %d  • take-profit legs: %d",
            len(self.stop_loss_df), len(self.take_profit_df),
        )

    def find_parents_for_symbol(self, symbol: str) -> set[str]:
        """
        Return IDs of still-open **filled** bracket parents for <symbol>.
        """
        # ➊ open stop-loss leg
        df = self.stop_loss_df
        live_sl = df[
            (df["symbol"] == symbol)
        & df["status"].str.lower().isin({"new", "held"})
        ]

        if live_sl.empty:
            return set()

        # ➋ the parent row must tell us it got a fill
        filled_parents = {
            pid for pid in live_sl["parent_id"]
            if (self.orders_df.loc[self.orders_df["id"] == pid, "filled_qty"]
                    .astype(float)               # NaN → float('nan')
                    .fillna(0)                   # NaN → 0
                    .iloc[0] > 0)                # keep only qty > 0
        }
        return filled_parents
       
    # ───────────────────────────────────────────────────────────────────
    # 2. Market-data helpers
    # ───────────────────────────────────────────────────────────────────    

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
    def current_stop_info(self, parent_id: str) -> tuple[float, str] | tuple[None, None]:
        """
        Locate the active stop-loss child for *parent_id*.

        Returns
        -------
        (stop_price, child_id)  – if found and still open
        (None, None)            – if not found / already cancelled
        """
        df = self.stop_loss_df
        live = df[
            (df["parent_id"] == parent_id)
            & ~df["status"].str.lower().isin(
                ["canceled", "filled", "expired", "replaced"]
            )
        ]
        if live.empty:
            return None, None

        # pick the most recently updated leg
        row = live.sort_values("updated_at").iloc[-1]
        return float(row["stop_price"]), str(row["id"])

    # ------------------------------------------------------------------
    def replace_stop_loss(self, child_id: str, new_px_raw: float, symbol: str) -> bool:
        """
        Patch the active stop-loss child leg to new_px, honouring the leg’s
        true order_type:

        • stop_limit  → send stop_price *and* limit_price
        • stop        → send stop_price only
        """
        new_px = round_price(new_px_raw)

        # find the leg row so we know which style it is
        leg = self.stop_loss_df[self.stop_loss_df["id"] == child_id]
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
        # ── human-readable line ──────────────────────────────────────────
        parts = [action, symbol]
        if old is not None and new is not None:
            parts.append(f"old={old:.2f}  new={new:.2f}")
        if note:
            parts.append(note)

        msg = "  ".join(parts)
        (self.log.info if action == "STOP_MOVED" else self.log.debug)(msg)

    # ------------------------------------------------------------------ helpers (private) ──
    def _get_live_positions(self) -> pd.DataFrame:
        """Return live positions as a DataFrame; abort early if none."""
        pos = pd.DataFrame([p._raw for p in self.api.list_positions()])
        self.log.info("Live positions: %d", len(pos))
        return pos

    def _load_order_universe(self, symbols: list[str]) -> None:
        """Refresh self.orders_df and the stop-loss / TP views."""
        self.orders_df = fetch_orders_for_symbols(
            api_key  = self.cfg["API_KEY"],
            secret_key = self.cfg["SECRET_KEY"],
            base_url=self.cfg["ALPACA_BASE_URL"],
            symbols = symbols,
            after   = "2025-07-01T00:00:00Z",
            until   = datetime.utcnow().isoformat(timespec="seconds") + "Z",
            limit   = 500,
            show_progress = False,
        )
        self.log.info("• %d relevant order rows fetched", len(self.orders_df))
        self.refresh_leg_views()

    def _build_meta(self, positions: pd.DataFrame) -> dict[tuple[str, str], dict]:
        """
        For **every** live *(symbol, parent_id)* pair collect:

            • parent_id        • entry_px
            • fill_ts (UTC)    • BE-trigger price

        Returns
        -------
        meta[(sym, pid)] = {...}
        """
        meta: dict[tuple[str, str], dict] = {}

        for _, pos in positions.iterrows():
            sym   = pos["symbol"]
            pids  = self.find_parents_for_symbol(sym)      # ← now a *set*
            if not pids:
                self.log_event("NO_PARENT_ORDER", sym, "n/a")
                continue

            for pid in pids:
                p_row = self.orders_df[self.orders_df["id"] == pid]
                if p_row.empty:
                    self.log_event("PARENT_NOT_IN_DF", sym, pid)
                    continue

                p_row = p_row.iloc[0]

                # --- guard: parent must be FILLED (qty>0) and have a real timestamp ----------
                qty  = p_row["filled_qty"]
                ts   = p_row["filled_at"]

                filled_ok = (not pd.isna(qty)) and (float(qty) > 0)
                time_ok   = not pd.isna(ts)

                if not (filled_ok and time_ok):
                    self.log_event("SKIP_EMPTY_PARENT", sym, pid)
                    continue

                be_tp = self.fetch_be_trigger_price(sym, p_row["created_at"])
                if be_tp is None:
                    self.log_event("NO_TRIGGER_ROW", sym, pid)
                    continue

                meta[(sym, pid)] = dict(
                    parent_id = pid,
                    fill_ts   = p_row["filled_at"] or p_row["submitted_at"],
                    be_tp     = be_tp,
                    entry_px  = float(p_row["filled_avg_price"]),
                )

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

        for (sym, pid), m in meta.items():

            # ---- parent-specific details -----------------------------
            p_row = self.orders_df[self.orders_df["id"] == pid]
            qty   = int(p_row.iloc[0]["filled_qty"]) if not p_row.empty else 0

            sl_px, _ = self.current_stop_info(pid)
            mkt_px   = last_px.get(sym)

            if   pid in moved_parent_ids:   flag = "OK"
            elif pid in already_parent_ids: flag = "Already"
            else:                           flag = "—"

            # ---- console ------------------------------------------------
            self.log.info(
                "%s | %-5s | %s | Qty %-5d | Avg %.4f | SL %6s | TP %6.4f | "
                "Mkt %6s | %s",
                pid[:8],
                sym,
                m['fill_ts'].isoformat(timespec="seconds"),
                qty,
                m['entry_px'],
                f"{sl_px:.4f}" if sl_px is not None else "n/a ",
                m['be_tp'],
                f"{mkt_px:.4f}" if mkt_px is not None else "n/a ",
                flag,
            )

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
        is_long  = pos_side.lower() == "long"

        old_stop, child = self.current_stop_info(pid)
        if old_stop is None:
            self.log_event("NO_STOP_CHILD", sym, pid)
            return None, "noop"

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
    def run(self) -> None:
        """
        One full break-even cycle.

        Now fully parent-aware:
            • extrema are fetched *once per SYMBOL* (earliest fill_ts wins)
            • summary counters reflect #parents, not #symbols
        """
        self.log.info("— Break-Even manager cycle start —")

        # 0) live positions -------------------------------------------------
        positions = self._get_live_positions()
        if positions.empty:
            return

        # 1) orders universe  ----------------------------------------------
        self._load_order_universe(positions["symbol"].tolist())

        # 2) meta  (one entry per (symbol,parent_id)) ----------------------
        meta = self._build_meta(positions)
        if not meta:
            
            self.log.info("No valid brackets with open stops – aborting cycle")
            return
        
        parents_total = len(meta)
        # ---------- ✱ NEW ✱  earliest fill per SYMBOL ---------------------
        since_by_sym: dict[str, datetime] = {}
        for (sym, _pid), m in meta.items():
            since_by_sym.setdefault(sym, m["fill_ts"])
            since_by_sym[sym] = min(since_by_sym[sym], m["fill_ts"])

        # 3) price extrema (symbol → {max_high,min_low}) -------------------
        extrema = (
            self._fetch_price_extrema_parallel(
                symbols=list(since_by_sym),
                since=since_by_sym,
                max_workers=self.cfg.get("MAX_WORKERS"),
            )
            .set_index("symbol")
            .to_dict("index")
        )

        # 4) last-trade prices (symbol → px) -------------------------------
        last_px = self._fetch_last_trade_prices_parallel(
            symbols=list(since_by_sym),
            max_workers=self.cfg.get("MAX_WORKERS"),
        )

        # 5) decide & queue PATCHes ----------------------------------------
        jobs, already_pids = self._collect_patch_jobs(
            positions, meta, extrema, last_px
        )
        moved_pids = self._apply_replacements_parallel(
            jobs, max_workers=self.cfg.get("MAX_WORKERS", 10)
        )

        # 6) snapshot & logging --------------------------------------------
        self._log_position_snapshot(
            positions, meta, last_px, moved_pids, already_pids
        )

        self.log.info(
            "Cycle finished – %d parents moved, %d already @BE, "
            "%d parents reviewed across %d positions",
            len(moved_pids), len(already_pids), parents_total, len(positions),
        )

        # 7) housekeeping ---------------------------------------------------
        self._prune_old_snapshots(days=30)
        self.log.info("— cycle complete —")