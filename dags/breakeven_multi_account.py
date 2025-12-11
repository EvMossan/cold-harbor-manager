"""
Dynamic_Breakeven_Manager.py
============================

Dynamic generation of DAGs for each account listed in
coldharbour_manager.core.destinations.DESTINATIONS.

Flow:
1. Import the DESTINATIONS list.
2. Create a DAG named "Risk_Manager_{slug}" per account.
3. Supply account-specific keys (API, Secret, URL) and table
   settings.
"""

import asyncio
import os
import logging
from datetime import datetime, timedelta
from decimal import Decimal

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Import shared configs and helpers
from coldharbour_manager.core.destinations import DESTINATIONS, slug_for
from coldharbour_manager.services.risks_manager import BreakevenOrderManager

# Default settings for every DAG
default_args = {
    "owner": "JJ",
    "depends_on_past": False,
    "retries": 5,
    "retry_delay": timedelta(seconds=3),
    "email_on_failure": False,
    "email_on_retry": False,
}

# Schedule: every minute of the trading session (estimate)
SCHEDULE_CRON = "*/1 9-16 * * 1-5"

def create_dag(destination: dict):
    """
    Factory function that creates a DAG for the given destination.
    """
    
    # 1. Generate a unique ID (slug)
    # slug_for makes names safe: "Cold Harbour v1.0" -> "cold_harbour_v1_0"
    account_slug = slug_for(destination)
    dag_id = f"Risk_Manager_{account_slug}"
    # Get the account name for logging/UI
    account_name = destination.get("name", account_slug)

    async def _run_async_task():
        """Asynchronous task logic."""
        # Configure the logger for this DAG
        logger = logging.getLogger(f"BE_Manager_{account_slug}")
        logger.setLevel(logging.INFO)
        logger.info(f"Starting Risk Manager cycle for: {account_name} ({account_slug})")

        # 2. Build the configuration for this account.
        # Data is already loaded into destination via os.getenv.
        
        # Determine table names.
        # Breakouts and Bars are shared; logs are slug-specific.
        log_table = f"log_breakeven_{account_slug}"
        
        config = {
            # --- Alpaca Credentials ---
            # destinations.py already applied os.getenv so values are ready
            "API_KEY":         destination.get("key_id"),
            "SECRET_KEY":      destination.get("secret_key"),
            "ALPACA_BASE_URL": destination.get("base_url", "https://paper-api.alpaca.markets"),

            # --- Connections ---
            "CONN_STRING_POSTGRESQL": os.getenv(
                "POSTGRESQL_LIVE_LOCAL_CONN_STRING",
                "postgresql://user:pass@localhost:5432/live"
            ),
            "CONN_STRING_TIMESCALE": os.getenv(
                "TIMESCALE_LIVE_LOCAL_CONN_STRING",
                "postgresql://user:pass@localhost:5432/timescale"
            ),

            # --- Tables ---
            "TABLE_BARS_1MIN": "alpaca_bars_1min",      # Shared
            "TABLE_BREAKOUTS": "alpaca_breakouts",      # Shared
            "TABLE_BE_EVENTS": log_table,               # Slug-specific (log_stop_manager_live, etc.)
            
            "ACCOUNT_SLUG": account_slug,

            # --- Tuning ---
            "BE_TRIGGER_R": 2,
            "TICK_DEFAULT": 0.01,
            "MIN_STOP_GAP": 0.01,
            "PRICE_DECIMALS": Decimal("0.01"),
            
            # Risk Factor or Dry Run can be overridden via destination config.
            "DRY_RUN": destination.get("dry_run", False),
            "MAX_WORKERS": 10,
        }

        # Validate keys in case env vars failed to load.
        if not config["API_KEY"] or not config["SECRET_KEY"]:
            logger.error(f"Missing credentials for {account_name}. Skipping.")
            return

        # 3. Launch the manager.
        om = BreakevenOrderManager(config)
        try:
            await om.run()
        finally:
            await om.close()

    def task_wrapper():
        """Synchronous wrapper for the Airflow PythonOperator."""
        asyncio.run(_run_async_task())

    # 4. Create the DAG object
    dag = DAG(
        dag_id=dag_id,
        description=f"Break-Even manager for {account_name}",
        default_args=default_args,
        start_date=datetime(2025, 7, 14),
        schedule=SCHEDULE_CRON,
        catchup=False,
        tags=["Risk Manager", "Multi-Account", account_slug],
    )

    with dag:
        PythonOperator(
            task_id=f"run_be_cycle_{account_slug}",
            python_callable=task_wrapper,
        )

    return dag

# ---------------------------------------------------------------------
# MAIN REGISTRATION LOOP (Dynamic DAG Generation)
# ---------------------------------------------------------------------

# Iterate through the list from destinations.py
for dest in DESTINATIONS:
    # Skip if trading is explicitly disabled (optional)
    if not dest.get("allow_trading", True):
        continue

    # Create the DAG
    generated_dag = create_dag(dest)
    
    # IMPORTANT: Register the DAG in the global namespace.
    # Airflow only discovers DAG objects in globals().
    globals()[generated_dag.dag_id] = generated_dag
