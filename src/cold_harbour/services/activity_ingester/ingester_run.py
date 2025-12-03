"""
Entrypoint for the Cold Harbour Data Ingester Service.
Run with: python -m cold_harbour.ingester_run
"""

import asyncio
import logging
import os
import sys

# Apply nest_asyncio if needed (e.g. mainly for notebooks, 
# but harmless here if already installed in env)
try:
    import nest_asyncio
    nest_asyncio.apply()
except ImportError:
    pass

from cold_harbour.services.activity_ingester.runtime import IngesterService

def main() -> None:
    # Configure logging
    log_level = os.getenv("LOG_LEVEL", "DEBUG").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout
    )
    
    # Mute noisy libraries if necessary
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("alpaca_trade_api").setLevel(logging.INFO)

    service = IngesterService()

    try:
        asyncio.run(service.run())
    except KeyboardInterrupt:
        # Should be handled by signal handlers in runtime, but just in case
        pass
    except Exception as e:
        logging.getLogger("Main").exception(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()