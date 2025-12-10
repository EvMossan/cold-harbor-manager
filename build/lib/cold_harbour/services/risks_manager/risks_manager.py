"""
cold_harbour.services.risks_manager.risk_manager
================================================

Refactored RiskManager that oversees open positions and manages
Break-Even (BE) stop-loss adjustments.

Architecture:
-------------
1.  **Data Source**: Loads historical orders from the local database
    (`load_orders_from_db`). If the DB is unavailable or empty, it falls
    back to the Alpaca API (`fetch_orders`).
2.  **State Construction**: Uses `build_lot_portfolio` to reconstruct
    the current state of open positions, including links to active
    Stop-Loss and Take-Profit legs.
3.  **Decision Engine**: Preserves the original logic for:
    * Fetching Breakout Triggers (`takeprofit_X_price`).
    * Calculating Price Extrema (High/Low) since entry.
    * Validating Stop moves (Break-Even checks).
4.  **Execution**: Patches active Stop-Loss orders via Alpaca API.

Thread Safety:
--------------
Uses `asyncio` for the main loop and `ThreadPoolExecutor` (via
`asyncio.to_thread`) for blocking I/O operations (Alpaca API calls,
synchronous DB queries for legacy tables).
"""

from __future__ import annotations

import asyncio
import logging
import sys
from datetime import datetime, timedelta, timezone, time as dtime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg2
from alpaca_trade_api.rest import REST, TimeFrame, TimeFrameUnit

# Internal imports for analytics and data loading
from cold_harbour.services.account_manager.core_logic.account_analytics import (
    fetch_orders,
    build_lot_portfolio,
)
from cold_harbour.services.account_manager.loader import load_orders_from_db
from cold_harbour.infrastructure.db import AsyncAccountRepository

# Configure module-level logger
log = logging.getLogger("RiskManager")


# ─────────────────────────────────────────────────────────────────────────────
# Static Helpers (Preserved from original logic)
# ─────────────────────────────────────────────────────────────────────────────

def prev_30m_bar(ts: datetime) -> datetime:
    """
    Return the *previous* 30-minute bar anchor in UTC.

    Example:
        18:30:35.746 -> 18:00:00
        18:05:12.000 -> 17:30:00
    """
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    else:
        ts = ts.astimezone(timezone.utc)
    
    floored = ts.replace(
        second=0, 
        microsecond=0, 
        minute=(ts.minute // 30) * 30
    )
    return floored - timedelta(minutes=30)


def round_price(price: float) -> float:
    """
    Conform *price* to the minimum tick required by Alpaca:
    * if price < 1 USD  -> tick = 0.0001 (4 dp)
    * else              -> tick = 0.01   (2 dp)
    """
    tick = 0.0001 if price < 1 else 0.01
    return round(round(price / tick) * tick, 4 if tick < 0.01 else 2)


# ─────────────────────────────────────────────────────────────────────────────
# RiskManager Class
# ─────────────────────────────────────────────────────────────────────────────

class RiskManager:
    """
    Autonomous service that monitors open positions and moves Stop-Loss
    orders to Break-Even when specific market conditions are met.
    """

    def __init__(self, cfg: Dict[str, Any]):
        """
        Initialize the RiskManager with configuration settings.

        Args:
            cfg: Dictionary containing API credentials, DB strings, and
                 risk parameters.
        """
        self.cfg = cfg
        
        # Alpaca REST API client for execution and market data
        self.api = REST(
            cfg["API_KEY"], 
            cfg["SECRET_KEY"], 
            cfg["ALPACA_BASE_URL"]
        )

        # 1. Async Repository for loading Orders (High performance)
        self.db_conn_string = cfg["CONN_STRING_POSTGRESQL"]
        self.repo: Optional[AsyncAccountRepository] = None

        # 2. Synchronous Connection for Legacy Logic (Breakouts/Audit)
        # Preserved from old implementation to maintain identical logic.
        self.pg = psycopg2.connect(cfg["CONN_STRING_POSTGRESQL"])
        self.pg.autocommit = True

        # Configuration Parameters
        self.slug = cfg.get("ACCOUNT_SLUG", "default")
        # Default to loading ~27 years to ensure "all history" is covered
        self.history_days = 10000 
        self._be_table_ready = False

        log.info("RiskManager initialized.")

    async def init(self) -> None:
        """Initialize the asynchronous database connection pool."""
        if not self.repo:
            self.repo = await AsyncAccountRepository.create(
                self.db_conn_string
            )
            log.info("Async DB connection established.")
            
            # Ensure the audit table exists (legacy logic requirement)
            await asyncio.to_thread(self._ensure_be_table)

    def close(self) -> None:
        """Close database connections gracefully."""
        if self.repo:
            asyncio.create_task(self.repo.close())
        if self.pg:
            self.pg.close()

    async def run(self) -> None:
        """
        Main execution loop.
        Periodically rebuilds the portfolio and attempts to optimize stops.
        """
        await self.init()
        log.info("Starting RiskManager main loop...")

        while True:
            try:
                # 1. Build the current portfolio state
                portfolio_df = await self._build_current_portfolio()

                if not portfolio_df.empty:
                    # 2. Process active positions
                    await self._process_positions(portfolio_df)
                else:
                    log.debug("Portfolio is empty. No positions to manage.")

            except asyncio.CancelledError:
                log.info("RiskManager stopped by cancellation.")
                self.close()
                break
            except Exception as e:
                log.exception("Unexpected error in RiskManager loop: %s", e)

            # Wait before the next cycle (e.g., 60 seconds)
            await asyncio.sleep(60)

    # ─────────────────────────────────────────────────────────────────────────
    # Data Loading & Portfolio Construction
    # ─────────────────────────────────────────────────────────────────────────

    async def _build_current_portfolio(self) -> pd.DataFrame:
        """
        Load orders from DB (with API fallback) and construct the
        Lot-Based Portfolio.

        Returns:
            pd.DataFrame: A DataFrame containing open positions with
            columns like 'Parent ID', 'Stop_Loss_ID', 'Buy Price', etc.
        """
        orders_df = pd.DataFrame()

        # Step 1: Try loading from the local database
        try:
            orders_df = await load_orders_from_db(
                self.repo, 
                self.slug, 
                days_back=self.history_days
            )
        except Exception as e:
            log.warning(
                "DB Load Failed: %s. Attempting fallback to API.", e
            )

        # Step 2: Fallback to Alpaca API if DB failed or returned nothing
        if orders_df.empty:
            log.info("Fetching orders from Alpaca API (Fallback)...")
            try:
                # Running synchronous fetch_orders in a thread
                orders_df = await asyncio.to_thread(
                    fetch_orders,
                    self.api,
                    days_back=self.history_days
                )
            except Exception as e:
                log.error("API Fallback Failed: %s", e)
                return pd.DataFrame()

        if orders_df.empty:
            return pd.DataFrame()

        # Step 3: Build the Portfolio using the shared core logic
        # We pass self.api so it can fetch current market prices
        portfolio = await asyncio.to_thread(
            build_lot_portfolio,
            orders_df,
            api=self.api
        )

        return portfolio

    # ─────────────────────────────────────────────────────────────────────────
    # Position Processing & Logic
    # ─────────────────────────────────────────────────────────────────────────

    async def _process_positions(self, portfolio: pd.DataFrame) -> None:
        """
        Iterate through all open positions in the portfolio.
        Checks for missing Stop-Losses and attempts to move stops to BE.
        """
        for _, row in portfolio.iterrows():
            symbol = row.get("Symbol")
            parent_id = row.get("Parent ID")
            sl_id = row.get("Stop_Loss_ID")

            # CRITICAL CHECK: Alert if a position has no Stop-Loss
            if pd.isna(sl_id) or not sl_id:
                log.warning(
                    "⚠️ POSITION WITHOUT STOP LOSS DETECTED: "
                    "Symbol=%s, ParentID=%s", 
                    symbol, parent_id
                )
                continue

            # If we have a stop, proceed with the BE logic
            await self._check_and_move_stop(row)

    async def _check_and_move_stop(self, row: pd.Series) -> None:
        """
        Evaluate market conditions and move the stop if criteria are met.
        This orchestrates the legacy logic helpers.
        """
        symbol = row["Symbol"]
        parent_id = row["Parent ID"]
        
        # Extract data from the portfolio row
        # Note: 'Buy Date' might be a string or timestamp depending on source
        fill_ts = pd.to_datetime(row["Buy Date"], utc=True)
        entry_px = float(row["Buy Price"])
        current_sl = float(row["Stop_Loss_Price"])
        sl_id = str(row["Stop_Loss_ID"])
        
        # 1. Fetch Break-Even Trigger Price from DB (Synchronous)
        be_trigger = await asyncio.to_thread(
            self.fetch_be_trigger_price, 
            symbol, 
            fill_ts
        )
        
        if be_trigger is None:
            # No trigger defined in breakouts table, cannot proceed
            return

        # 2. Fetch Market Data (Extrema and Last Trade)
        # Using the preserved logic for finding high/low since entry
        _, max_high, min_low = await asyncio.to_thread(
            self._extrema_for_symbol, 
            symbol, 
            fill_ts
        )
        
        _, last_trade = await asyncio.to_thread(
            self._last_trade_for_symbol, 
            symbol
        )

        if last_trade is None:
            return

        # 3. Determine if we should move the stop
        # (Reusing the exact decision logic from the original manager)
        should_move, new_stop = self._calculate_stop_decision(
            symbol=symbol,
            entry_px=entry_px,
            current_sl=current_sl,
            be_trigger=be_trigger,
            max_high=max_high,
            min_low=min_low,
            last_trade=last_trade
        )

        if should_move:
            # 4. Execute the Order Replacement
            success = await asyncio.to_thread(
                self.replace_stop_loss,
                child_id=sl_id,
                new_px_raw=new_stop,
                symbol=symbol,
                row_data=row # Passing row to determine order type if needed
            )
            
            if success:
                # Log the event to the audit table
                await asyncio.to_thread(
                    self.log_event,
                    "STOP_MOVED",
                    symbol,
                    parent_id,
                    old=current_sl,
                    new=new_stop,
                    note=f"Trigger hit. Mkt={last_trade}"
                )

    def _calculate_stop_decision(
        self,
        symbol: str,
        entry_px: float,
        current_sl: float,
        be_trigger: float,
        max_high: float,
        min_low: float,
        last_trade: float
    ) -> Tuple[bool, float]:
        """
        Pure logic to decide if the stop should be moved to Break-Even.
        Preserves the original rules.
        """
        # Determine side based on stop location relative to entry
        # (Portfolio usually has 'side', but we can infer safely)
        is_long = current_sl < entry_px

        # Calculate the target Break-Even price
        legal_be = self._tightest_legal_stop(entry_px, is_long)

        # Guard: Already at or better than BE?
        if abs(current_sl - legal_be) < 1e-9:
            return False, 0.0
        
        # Guard: Prevent moving stop backwards (loosening risk)
        if is_long and current_sl > legal_be:
            return False, 0.0
        if not is_long and current_sl < legal_be:
            return False, 0.0

        # Guard: Price crossed back through entry?
        crossed = (is_long and last_trade < entry_px) or \
                  (not is_long and last_trade > entry_px)
        if crossed:
            return False, 0.0

        # Trigger Condition: Has price reached the target?
        armed = (max_high >= be_trigger) if is_long else (min_low <= be_trigger)
        
        if armed:
            return True, legal_be
        
        return False, 0.0

    # ─────────────────────────────────────────────────────────────────────────
    # Legacy Logic Helpers (Preserved Verbatim/Adapted)
    # ─────────────────────────────────────────────────────────────────────────

    def fetch_be_trigger_price(
        self, symbol: str, created_at: datetime
    ) -> float | None:
        """
        Look up the trigger price in the legacy `TABLE_BREAKOUTS`.
        """
        ts_floor = prev_30m_bar(created_at)
        tp_col = f"takeprofit_{self.cfg.get('BE_TRIGGER_R', 2)}_1_price"
        table = self.cfg.get("TABLE_BREAKOUTS", "breakouts")

        sql = (
            f"SELECT {tp_col} FROM {table} "
            f"WHERE symbol = %s AND timestamp = %s LIMIT 1"
        )

        try:
            with self.pg.cursor() as cur:
                cur.execute(sql, (symbol, ts_floor))
                row = cur.fetchone()
            if row:
                return float(row[0])
        except Exception as e:
            log.error("Error fetching BE trigger for %s: %s", symbol, e)
        
        return None

    def _extrema_for_symbol(
        self, sym: str, since_ts: datetime
    ) -> Tuple[str, float, float]:
        """
        Calculate High/Low since `since_ts` using optimized bar resolution.
        """
        try:
            ny = ZoneInfo("America/New_York")
            now = datetime.now(timezone.utc)
            today = now.astimezone(ny).date()
            fill = since_ts.astimezone(ny)

            DAILY_WINDOW = 30
            highs, lows = [], []

            # 1. Fill-day minute slice
            if fill.date() < today and fill.time() > dtime(9, 30):
                end_fill = datetime.combine(
                    fill.date(), dtime(16, 0), tzinfo=ny
                )
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

            # 2. Historical bars logic
            first_day_after_fill = (
                fill.date() if fill.time() == dtime(9, 30)
                else fill.date() + timedelta(days=1)
            )
            last_daily_day = today - timedelta(days=1)
            
            first_daily_day = first_day_after_fill
            first_weekly_day = None

            if (last_daily_day - first_day_after_fill).days + 1 > DAILY_WINDOW:
                first_daily_day = last_daily_day - timedelta(days=DAILY_WINDOW - 1)
                first_weekly_day = first_day_after_fill

            # Weekly bars
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

            # Daily bars
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

            # 3. Today's intraday bars
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

            if not highs:
                return sym, 0.0, 1e9

            return sym, float(max(highs)), float(min(lows))

        except Exception as exc:
            log.warning("Extrema fetch failed for %s: %s", sym, exc)
            return sym, 0.0, 1e9

    def _last_trade_for_symbol(self, sym: str) -> Tuple[str, Optional[float]]:
        """Fetch the latest trade price for a symbol."""
        try:
            if hasattr(self.api, "get_latest_trade"):
                price = self.api.get_latest_trade(sym).price
            elif hasattr(self.api, "get_last_trade"):
                price = self.api.get_last_trade(sym).price
            else:
                price = self.api.get_snapshot(sym).last_trade.price
            return sym, float(price)
        except Exception as exc:
            log.warning("Last-trade fetch failed for %s: %s", sym, exc)
            return sym, None

    def _tightest_legal_stop(self, entry_px: float, is_long: bool) -> float:
        """
        Calculate the closest legal stop price to the entry.
        Honors the MIN_STOP_GAP configuration.
        """
        bp = Decimal(str(entry_px))
        gap_cfg = Decimal(str(self.cfg.get("MIN_STOP_GAP", 0.01)))
        min_gap = Decimal("0.01") if entry_px >= 1 else Decimal("0.0001")
        gap = max(gap_cfg, min_gap)

        raw = bp - gap if is_long else bp + gap

        # Snap to tick grid
        decimals = 4 if entry_px < 1 else 2
        tick = Decimal(f"1e-{decimals}")
        legal = (raw / tick).to_integral_value(ROUND_HALF_UP) * tick

        return float(legal)

    def replace_stop_loss(
        self, 
        child_id: str, 
        new_px_raw: float, 
        symbol: str, 
        row_data: Optional[pd.Series] = None
    ) -> bool:
        """
        Send a replacement order to Alpaca.
        Determines if it needs 'limit_price' based on original order type.
        """
        new_px = round_price(new_px_raw)
        
        # We need to know if the original stop was 'stop' or 'stop_limit'.
        # The portfolio row doesn't strictly carry 'order_type' of the leg,
        # but we can try to infer or simply default to 'stop' if unknown.
        # Ideally, we should fetch the order details if we are unsure, 
        # but for performance, we might assume standard 'stop'.
        # However, to be safe and robust (as per "don't change logic"):
        # We will quickly fetch the specific order to check its type.
        
        try:
            order = self.api.get_order(child_id)
            kind = str(getattr(order, "type", "stop")).lower()
        except Exception as e:
            log.error("Failed to fetch order type for patching: %s", e)
            return False

        params = dict(stop_price=new_px)
        if kind == "stop_limit":
            params["limit_price"] = new_px

        log.info("PATCHING %s %s -> %.4f (%s)", symbol, child_id, new_px, kind)

        try:
            self.api.replace_order(order_id=child_id, **params)
            return True
        except Exception as exc:
            log.error("PATCH FAIL id=%s err=%s", child_id, exc)
            return False

    # ─────────────────────────────────────────────────────────────────────────
    # Logging / Audit
    # ─────────────────────────────────────────────────────────────────────────

    def _ensure_be_table(self) -> None:
        """Create the audit table if it does not exist."""
        if self._be_table_ready:
            return

        tbl = self.cfg.get("TABLE_BE_EVENTS", "log_stop_manager")
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {tbl} (
            ts           timestamptz DEFAULT now(),
            symbol       text,
            position_id  text,
            action       text,
            old_stop     real,
            new_stop     real,
            note         text
        );
        """
        try:
            with self.pg.cursor() as cur:
                cur.execute(ddl)
            self._be_table_ready = True
        except Exception as e:
            log.error("Failed to ensure audit table: %s", e)

    def log_event(
        self,
        action: str,
        symbol: str,
        position_id: str,
        *,
        old: float | None = None,
        new: float | None = None,
        note: str = "",
    ) -> None:
        """Log significant events to the DB and Console."""
        # Console output
        msg = f"{action} {symbol} {position_id} old={old} new={new} {note}"
        log.info(msg)

        # Database insert
        tbl = self.cfg.get("TABLE_BE_EVENTS", "log_stop_manager")
        try:
            with self.pg.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {tbl} 
                    (symbol, position_id, action, old_stop, new_stop, note)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (symbol, position_id, action, old, new, note)
                )
        except Exception as e:
            log.error("Failed to write to audit log: %s", e)