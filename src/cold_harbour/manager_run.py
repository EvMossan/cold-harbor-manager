"""services/account_manager/run_multi.py
Run multiple AccountManager instances in one process.

All accounts are discovered from ``cold_harbour.destinations.DESTINATIONS``
and isolated via per-account tables and NOTIFY channels derived from the
destination name (slug).

Signal handling is centralized here to perform a clean shutdown for all
managers.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from typing import Dict, Any, List

import nest_asyncio

from cold_harbour.services.account_manager import AccountManager
from cold_harbour.core.destinations import (
    DESTINATIONS,
    account_table_names,
    notify_channels_for,
)


def _common_cfg() -> Dict[str, Any]:
    """Return keys shared by all accounts.

    The AccountManager will also derive ``CONN_STRING_POSTGRESQL`` from
    the SQLAlchemy URL internally; we pass both to keep helpers happy.
    """
    return {
        # Prefer local direct connections (bypass Cloudflare) for manager
        "POSTGRESQL_LIVE_SQLALCHEMY": os.getenv(
            "POSTGRESQL_LIVE_LOCAL_CONN_STRING"
        )
        or os.getenv("POSTGRESQL_LIVE_SQLALCHEMY"),
        "CONN_STRING_POSTGRESQL": os.getenv(
            "POSTGRESQL_LIVE_LOCAL_CONN_STRING"
        )
        or os.getenv("POSTGRESQL_LIVE_CONN_STRING"),
        "TIMESCALE_LIVE_CONN_STRING": os.getenv(
            "TIMESCALE_LIVE_LOCAL_CONN_STRING"
        )
        or os.getenv("TIMESCALE_LIVE_CONN_STRING"),
        "TIMESCALE_LIVE_SQLALCHEMY": os.getenv(
            "TIMESCALE_LIVE_LOCAL_CONN_STRING"
        )
        or os.getenv("TIMESCALE_LIVE_SQLALCHEMY"),
        "ZMQ_SIP_STREAM_ENDPOINT": os.getenv("ZMQ_SIP_STREAM_ENDPOINT"),
        "FLUSH_INTERVAL_S": float(os.getenv("FLUSH_INTERVAL_S", 60)),
        "UI_PUSH_PCT_THRESHOLD": float(
            os.getenv("UI_PUSH_PCT_THRESHOLD", 0.25)
        ),
        "HEARTBEAT_SEC": int(
            os.getenv("HEARTBEAT_SEC", os.getenv("EQUITY_UPDATE_INTERVAL_S", 60))
        ),
        # Optional schema for account tables
        "ACCOUNT_SCHEMA": os.getenv("ACCOUNT_SCHEMA", "accounts"),
    }


def _cfg_for(dest: Dict[str, Any]) -> Dict[str, Any]:
    """Build AccountManager cfg for a single destination.

    Tables and channels are derived from the destination slug; no extra
    keys are required in the destination mapping.
    """
    base = _common_cfg()
    schema = base.get("ACCOUNT_SCHEMA", "accounts")

    tables = account_table_names(dest, schema=schema)
    chans = notify_channels_for(dest)

    cfg = {
        **base,
        "API_KEY": dest.get("key_id"),
        "SECRET_KEY": dest.get("secret_key"),
        "ALPACA_BASE_URL": dest.get(
            "base_url",
            os.getenv(
                "ALPACA_BASE_URL_PAPER", "https://paper-api.alpaca.markets"
            ),
        ),
        "TABLE_ACCOUNT_POSITIONS": tables["open"].split(".", 1)[-1],
        "TABLE_ACCOUNT_CLOSED": tables["closed"].split(".", 1)[-1],
        "TABLE_ACCOUNT_EQUITY_FULL": tables["equity"].split(".", 1)[-1],
        "TABLE_MARKET_SCHEDULE": tables["schedule"].split(".", 1)[-1],
        "POS_CHANNEL": chans["pos"],
        "CLOSED_CHANNEL": chans["closed"],
        "ENABLE_TRADE_STREAM": dest.get("trade_stream_enabled", False),
    }

    # Pass initial deposit from Destinations.
    # This keeps DAG and AccountManager in sync and ensures that when
    # the equity table is recreated, the first row uses the configured
    # starting capital.
    init_deposit = dest.get("initial_deposit")
    if init_deposit is not None:
        cfg["EQUITY_INIT_CASH"] = init_deposit

    # Ensure schema-qualification – AccountManager will re-qualify if needed
    # but we keep full names here for clarity across helpers.
    if schema:
        for k in (
            "TABLE_ACCOUNT_POSITIONS",
            "TABLE_ACCOUNT_CLOSED",
            "TABLE_ACCOUNT_EQUITY_FULL",
            "TABLE_MARKET_SCHEDULE",
        ):
            v = cfg[k]
            if v and "." not in v:
                cfg[k] = f"{schema}.{v}"
    return cfg


def _configure_logging() -> None:
    fmt = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"
    logging.basicConfig(
        level=logging.DEBUG,
        format=fmt,
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )
    logging.captureWarnings(True)
    for name, lvl in {
        "alpaca_trade_api": logging.INFO,
        "alpaca_trade_api.stream": logging.INFO,
        "alpaca.trading.stream": logging.INFO,
        "urllib3": logging.WARNING,
        "websockets": logging.INFO,
        "websockets.protocol": logging.INFO,
        "websockets.handshake": logging.INFO,
        "websockets.server": logging.INFO,
        "websockets.client": logging.INFO,
        "websockets.connection": logging.INFO,
    }.items():
        logging.getLogger(name).setLevel(lvl)


async def _run_all(managers: List[AccountManager]) -> None:
    """Run all managers concurrently with a shared stop event."""
    stop_evt = asyncio.Event()

    async def _gather() -> None:
        await asyncio.gather(
            *[
                m.run(stop_event=stop_evt, install_signal_handlers=False)
                for m in managers
            ]
        )

    loop = asyncio.get_running_loop()

    def _stop(*_args: Any) -> None:  # type: ignore[name-defined]
        if not stop_evt.is_set():
            logging.getLogger("AccountMgr").info(
                "[multi] Shutdown signal received; stopping all managers …"
            )
            stop_evt.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            pass

    await _gather()


def main() -> None:
    nest_asyncio.apply()
    _configure_logging()
    logging.getLogger("AccountMgr").setLevel(logging.DEBUG)

    managers = [AccountManager(_cfg_for(d)) for d in DESTINATIONS]
    asyncio.run(_run_all(managers))


if __name__ == "__main__":
    main()
