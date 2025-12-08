"""
src/cold_harbour/debug_audit.py

Deep diagnostic tool to reconcile:
1. Broker API (The Truth)
2. Daily Ledger Logic (Reconstructed Cash + Close Px)
3. Intraday Logic (Yesterday Close + Delta)

Run with: python -m cold_harbour.debug_audit
"""

import asyncio
import os
import sys
import logging
from datetime import datetime, timedelta, timezone
import pandas as pd
from tabulate import tabulate  # pip install tabulate if missing, or use pandas printing

from cold_harbour.manager_run import _cfg_for
from cold_harbour.core.destinations import DESTINATIONS
from cold_harbour.services.account_manager.runtime import AccountManager
from cold_harbour.core.equity import _calculate_daily_ledger_utc, _fetch_daily_closes_utc
from cold_harbour.services.account_manager.loader import load_activities_from_db

# Configure simplified logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("Audit")

async def audit_account(dest_cfg: dict):
    slug = dest_cfg["ACCOUNT_SLUG"]
    log.info(f"\n{'='*60}\nAUDITING ACCOUNT: {slug}\n{'='*60}")

    mgr = AccountManager(dest_cfg)
    await mgr.init()

    # 1. FETCH BROKER TRUTH
    # ---------------------------------------------------------
    log.info("1. Fetching Broker Snapshot (API)...")
    acct = mgr.rest.get_account()
    positions = mgr.rest.list_positions()
    
    broker_equity = float(acct.equity)
    broker_cash = float(acct.cash) # Buying power is different, we want settled + unsettled cash
    broker_mv = float(acct.long_market_value) - abs(float(acct.short_market_value))
    
    # Calculate Broker-implied cash manually to verify api.cash isn't misleading
    # Equity = Cash + MV  =>  Cash = Equity - MV
    broker_implied_cash = broker_equity - broker_mv

    log.info(f"   Broker Equity: ${broker_equity:,.2f}")
    log.info(f"   Broker Cash:   ${broker_cash:,.2f} (Implied: ${broker_implied_cash:,.2f})")
    log.info(f"   Broker MV:     ${broker_mv:,.2f}")

    # 2. RUN DAILY LEDGER REPLAY (Local DB)
    # ---------------------------------------------------------
    log.info("2. Replaying Daily Ledger (DB)...")
    # Fetch all activities
    activities_df = await load_activities_from_db(mgr.repo, slug)
    
    # Identify unique symbols involved
    if not activities_df.empty:
        syms = activities_df['symbol'].dropna().unique().tolist()
        syms = [s for s in syms if s]
    else:
        syms = []

    # Fetch daily prices for "Today" (or latest available) to simulate daily calc
    now = datetime.now(timezone.utc).date()
    start_date = (now - timedelta(days=5)) # Lookback a bit
    
    price_df = await asyncio.to_thread(
        _fetch_daily_closes_utc, mgr.rest, syms, start_date, now
    )
    
    # We cheat and inject current API prices into the "price_df" for TODAY 
    # to see if the Ledger logic matches Broker when using Broker Prices.
    today_prices = {p.symbol: float(p.current_price) for p in positions}
    # Create a row for today in price_df if missing or update it
    # This aligns the "Market Value" calculation of Ledger with Realtime
    
    # Run Ledger Replay
    # We pass a set of dates including today
    valid_dates = set(price_df.index.tolist())
    valid_dates.add(now)
    
    # Inject current prices for 'now' into price_df for accurate MV comparison
    for sym, px in today_prices.items():
        price_df.loc[now, sym] = px
        
    equity_df = await asyncio.to_thread(
        _calculate_daily_ledger_utc, 
        activities_df, 
        price_df, 
        valid_dates
    )
    
    if not equity_df.empty:
        daily_row = equity_df.iloc[-1]
        daily_cash = daily_row['cash_balance']
        daily_mv = daily_row['market_value']
        daily_equity = daily_row['deposit']
    else:
        daily_cash = 0.0
        daily_mv = 0.0
        daily_equity = 0.0

    # 3. DIAGNOSE INTRADAY GAPS
    # ---------------------------------------------------------
    log.info("3. Checking Intraday Anchors...")
    
    # Get Yesterday's Equity from DB (Anchor for Intraday)
    # This assumes yesterday was the last row in tbl_equity before today
    yest_row = await mgr._db_fetchrow(
        f"SELECT * FROM {mgr.tbl_equity} WHERE date < $1 ORDER BY date DESC LIMIT 1",
        now
    )
    yest_equity = float(yest_row['deposit']) if yest_row else 0.0
    yest_date = yest_row['date'] if yest_row else "N/A"
    
    log.info(f"   Intraday Anchor (Yesterday {yest_date}): ${yest_equity:,.2f}")

    # 4. COMPARE & REPORT
    # ---------------------------------------------------------
    
    # CASH COMPARISON
    cash_diff = daily_cash - broker_implied_cash
    mv_diff = daily_mv - broker_mv
    
    print("\n" + "="*80)
    print(f"AUDIT RESULTS FOR {slug}")
    print("="*80)
    
    df_compare = pd.DataFrame([
        ["Cash Balance", broker_implied_cash, daily_cash, cash_diff, "Missing Fee/Div/Journal?"],
        ["Market Value", broker_mv, daily_mv, mv_diff, "Price Sync Issue"],
        ["Total Equity", broker_equity, daily_equity, daily_equity - broker_equity, "Sum of above"]
    ], columns=["Metric", "Broker (Truth)", "Daily Replay (Calc)", "Diff", "Suspect"])
    
    print(tabulate(df_compare, headers="keys", floatfmt=",.2f", tablefmt="simple"))
    
    print("\n--- Intraday Logic Check ---")
    print(f"Yesterday Closed Equity (DB): {yest_equity:,.2f}")
    print(f"Broker Equity Now:            {broker_equity:,.2f}")
    print(f"Implied Today PnL (Broker):   {(broker_equity - yest_equity):,.2f}")
    
    # Check for 'Ghost' Fees (Fees in broker but not in DB)
    # This is often why Cash differs
    print("\n--- Recent Flows Analysis (Last 5 Days) ---")
    recent_flows = activities_df[
        activities_df['transaction_time'] >= pd.Timestamp(now - timedelta(days=5), tz="UTC")
    ]
    # Filter for non-trade flows
    non_trade = recent_flows[
        ~recent_flows['activity_type'].isin(['FILL', 'PARTIAL_FILL'])
    ].copy()
    
    if not non_trade.empty:
        print(non_trade[['transaction_time', 'activity_type', 'net_amount']].to_string())
    else:
        print("No non-trade flows (fees/divs) found in local DB for last 5 days.")

    await mgr.close()

async def main():
    # Pick the first destination (usually live) or iterate
    target_dest = DESTINATIONS[0] 
    cfg = _cfg_for(target_dest)
    await audit_account(cfg)

if __name__ == "__main__":
    asyncio.run(main())