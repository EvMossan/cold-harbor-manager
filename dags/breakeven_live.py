"""
07.ALPACA_BreakevenStopManager.py
=================================

DAG purpose
-----------
Run the **BreakevenOrderManager** once per minute during U.S. regular-hours
trading so every open *bracket* position in the Alpaca account has its stop-loss
leg lifted to break-even (R = 0) as soon as price has travelled
``BE_TRIGGER_R × R`` in the trade’s favour **and** remains at or beyond that
trigger.

Manager vs. DAG
---------------
* The *trading logic, data-fetching and stop replacement* live in  
  **cold_harbour.stop_order_manager.BreakevenOrderManager**.  
  → See that module’s doc-string for the full algorithm, safety rules, and
  configuration knobs.

* This file does only three things  
  1. **Build the cfg dict** (API keys, DB strings, tuning parameters)  
  2. **Invoke `BreakevenOrderManager.run()`** exactly once  
  3. **Schedule** the call every minute, 09 : 30 – 16 : 00 ET, Mon-Fri

Airflow specifics
-----------------
* **Schedule** ``*/1 9-16 * * 1-5`` (once a minute, RTH only)  
* **Operator** ``run_break_even_cycle`` (PythonOperator)  
* **Retries** 5 attempts, 15-second back-off  
* **Catch-up** disabled – each run acts only on *current* positions.

Required environment variables
------------------------------
* ``ALPACA_API_KEY_JOHNYSSAN``  
* ``ALPACA_SECRET_KEY_JOHNYSSAN``  
* ``POSTGRESQL_LIVE_CONN_STRING``  
* ``TIMESCALE_LIVE_CONN_STRING``  
* ``FORCE_BE`` (optional – bypass entry-range guard for dry-run)

Author
------
Evgenii Mosikhin
"""

import asyncio
import os
from datetime import datetime, timedelta
# from zoneinfo import ZoneInfo
# from typing import Any, Dict, List
# from decimal import Decimal, ROUND_HALF_UP
# import pandas as pd
from alpaca_trade_api.rest import REST, TimeFrame, TimeFrameUnit
# from concurrent.futures import ThreadPoolExecutor, as_completed

from airflow import DAG
from airflow.operators.python import PythonOperator

from coldharbour_manager.services.risks_manager import BreakevenOrderManager


# ───────────────────────────────────────────────────────────
# PythonOperator callable
# ───────────────────────────────────────────────────────────
async def _run_break_even_cycle_async() -> None:
    """Wrapper that builds the cfg dict, runs one cycle, then closes."""
    # ── raise log level for this task only ────────────────────────────
    import logging
    logging.getLogger().setLevel(logging.INFO)  # root
    logging.getLogger('airflow.task').setLevel(logging.INFO)
    # If you use your own named logger inside BreakevenOrderManager:
    logging.getLogger('BE_Manager').setLevel(logging.INFO)

    config = {
        # Alpaca
        "API_KEY":         os.getenv("ALPACA_API_KEY_LIVE"),
        "SECRET_KEY":      os.getenv("ALPACA_SECRET_KEY_LIVE"),
        "ALPACA_BASE_URL": "https://api.alpaca.markets",

        # Connections
        "CONN_STRING_POSTGRESQL": os.getenv(          # ← plain Postgres (orders, logs)
            "POSTGRESQL_LIVE_LOCAL_CONN_STRING",
            "postgresql://user:pass@localhost:5432/live"
        ),
        "CONN_STRING_TIMESCALE": os.getenv(           # ← Timescale (prices, quotes)
            "TIMESCALE_LIVE_LOCAL_CONN_STRING",
            "postgresql://user:pass@localhost:5432/timescale"
        ),

        # Table names
        "TABLE_BARS_1MIN": "alpaca_bars_1min",
        "TABLE_BE_EVENTS": "log_stop_manager_live",
        'TABLE_BREAKOUTS': 'alpaca_breakouts',
        "ACCOUNT_SLUG": "live",

        # BE-manager parameters
        "BE_TRIGGER_R": 2,
        "TICK_DEFAULT": 0.01,
        "MIN_STOP_GAP":    0.01,
        "PRICE_DECIMALS":  Decimal("0.01"),

        "FORCE_BE": False,

        "MAX_WORKERS": 10,

        "DRY_RUN": False
        
    }

    # ── 2. Run a single cycle -------------------------------------------
    om = BreakevenOrderManager(config)
    try:
        await om.run()
    finally:
        await om.close()


def run_break_even_cycle() -> None:
    """Sync wrapper to allow PythonOperator to execute the async task."""
    asyncio.run(_run_break_even_cycle_async())


# ───────────────────────────────────────────────────────────
# DAG definition
# ───────────────────────────────────────────────────────────
default_args = {
    "owner": "JJ",
    "depends_on_past": False,
    "retries": 5,
    "retry_delay": timedelta(seconds=3),
    "email_on_failure": False,
    "email_on_retry": False,
}

# NY trading hours cron: *every minute* 09:30-16:00 Mo-Fr
schedule_cron = "*/1 9-16 * * 1-5"

with DAG(
    dag_id="Order_Manager_StopLoss_to_Breakeven_LIVE",
    description="Break-Even stop-loss adjustment cycle",
    default_args=default_args,
    start_date=datetime(2025, 7, 14),
    schedule_interval=schedule_cron,
    catchup=False,
    tags=["StopLoss Manager", "Alpaca", "Breakeven"],
) as dag:

    run_be_cycle = PythonOperator(
        task_id="run_break_even_cycle",
        python_callable=run_break_even_cycle,
    )

    # Only one task, but keep the explicit reference for extensibility
    run_be_cycle
