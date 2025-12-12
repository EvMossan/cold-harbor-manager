"""
Entrypoint for the Cold Harbour Data Ingester service.

Invoke with ``python -m coldharbour_manager.ingester_run``.
"""

from __future__ import annotations


import asyncio
import logging
import os
import sys

# Apply nest_asyncio when running inside notebooks; safe always.
try:
    import nest_asyncio
    nest_asyncio.apply()
except ImportError:
    pass

from coldharbour_manager.services.activity_ingester.runtime import IngesterService


def main() -> None:
    log_level_name = os.getenv("LOG_LEVEL", "DEBUG").upper()
    level = getattr(logging, log_level_name, logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )

    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("alpaca_trade_api").setLevel(logging.INFO)
    logging.getLogger("alpaca_trade_api.stream").setLevel(logging.INFO)
    logging.getLogger("alpaca.trading.stream").setLevel(logging.INFO)
    logging.getLogger("websockets").setLevel(logging.INFO)
    logging.getLogger("websockets.protocol").setLevel(logging.INFO)
    logging.getLogger("websockets.client").setLevel(logging.INFO)
    logging.getLogger("websockets.server").setLevel(logging.INFO)
    logging.getLogger("websockets.connection").setLevel(logging.INFO)

    service = IngesterService()

    try:
        asyncio.run(service.run())
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        logging.getLogger("IngesterMain").exception("Fatal error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
