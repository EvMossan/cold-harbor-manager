from __future__ import annotations

import asyncio
import os
from typing import Any, Dict

from coldharbour_manager import create_app
from coldharbour_manager.services.account_manager import AccountManager


def run_web() -> None:
    """Run the Flask web app in a simple local development mode."""

    app = create_app()
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "5000")),
        debug=False,
        threaded=True,
    )


def run_manager(cfg: Dict[str, Any]) -> AccountManager:
    """Return an AccountManager initialized with the provided config."""

    manager = AccountManager(cfg)

    async def _start() -> None:
        await manager.init()
        await manager.run()  # type: ignore[attr-defined]

    # Schedule the async loop fire-and-forget for quick local runs
    asyncio.get_event_loop().create_task(_start())
    return manager


__all__ = ["run_web", "run_manager"]
