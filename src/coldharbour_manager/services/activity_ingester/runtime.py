"""
Runtime orchestrator for the Data Ingester Service.
Manages lifecycle of workers for multiple accounts defined in DESTINATIONS.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from typing import Any, Dict, List, Optional

from coldharbour_manager.core.destinations import DESTINATIONS, slug_for
from coldharbour_manager.infrastructure.db import AsyncAccountRepository
from coldharbour_manager.services.activity_ingester import storage, workers
from coldharbour_manager.services.activity_ingester.config import IngesterConfig

log = logging.getLogger("IngesterRuntime")


class IngesterService:
    """
    Main service class that orchestrates data ingestion for multiple accounts.
    """

    def __init__(self) -> None:
        self.repo: Optional[AsyncAccountRepository] = None
        self._tasks: List[asyncio.Task] = []
        self._stop_event = asyncio.Event()
        
        # Resolve DB connection string from environment
        # Prefer local connection before falling back to tunnel values.
        self._db_dsn = (
            os.getenv("POSTGRESQL_LIVE_LOCAL_CONN_STRING")
            or os.getenv("POSTGRESQL_LIVE_CONN_STRING")
            or os.getenv("POSTGRESQL_LIVE_SQLALCHEMY")
        )
        if not self._db_dsn:
            raise ValueError(
                "Missing DB connection string (POSTGRESQL_LIVE_CONN_STRING)"
            )

    async def init(self) -> None:
        """Initialize database connection pool."""
        log.info("Initializing Ingester Service...")
        self.repo = await AsyncAccountRepository.create(self._db_dsn)
        log.info("Database connection established.")

    async def _start_account_workers(self, dest: Dict[str, Any]) -> None:
        """
        Bootstrap and launch workers for a single destination/account.
        """
        assert self.repo is not None
        
        name = dest.get("name", "unknown")
        slug = slug_for(dest)
        
        api_key = dest.get("key_id")
        secret_key = dest.get("secret_key")
        base_url = dest.get("base_url")
        
        if not (api_key and secret_key and base_url):
            log.warning(f"Skipping account '{name}': missing credentials.")
            return

        log.info(f"[{slug}] Bootstrapping account storage...")
        
        # 1. Ensure DB Schema/Tables exist for this account
        await storage.ensure_schema_and_tables(self.repo, slug)

        # 2. Check if Backfill is needed (run as background task)
        # We spawn this separately so it doesn't block stream startup
        backfill_task = asyncio.create_task(
            workers.run_backfill_task(
                self.repo, api_key, secret_key, base_url, slug
            ),
            name=f"backfill-{slug}"
        )
        self._tasks.append(backfill_task)

        # 3. Start Stream Consumer (Infinite Loop)
        stream_task = self._spawn_guarded(
            f"stream-{slug}",
            lambda: workers.run_stream_consumer(
                self.repo, api_key, secret_key, base_url, slug
            )
        )
        self._tasks.append(stream_task)

        # 4. Start Healing Worker (Infinite Loop)
        healing_task = self._spawn_guarded(
            f"healing-{slug}",
            lambda: workers.run_healing_worker(
                self.repo, api_key, secret_key, base_url, slug
            )
        )
        self._tasks.append(healing_task)
        
        log.info(f"[{slug}] All workers launched.")

    def _spawn_guarded(
        self,
        name: str,
        factory: Any
    ) -> asyncio.Task:
        """
        Run a task in a restart loop to handle crashes gracefully.
        """
        async def _runner() -> None:
            while not self._stop_event.is_set():
                try:
                    await factory()
                except asyncio.CancelledError:
                    break
                except Exception as exc:
                    log.exception(f"Task {name} crashed: {exc}. Restarting in 5s...")
                    await asyncio.sleep(5)
                
                if self._stop_event.is_set():
                    break

        return asyncio.create_task(_runner(), name=name)

    async def run(self) -> None:
        """
        Main entry point. Starts everything and waits for stop signal.
        """
        await self.init()

        # Register signal handlers for graceful shutdown
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._stop_event.set)
            except NotImplementedError:
                # Signal handlers might not work in all environments (e.g. Windows)
                pass

        log.info(f"Found {len(DESTINATIONS)} destinations configuration.")

        # Launch workers for all accounts
        for dest in DESTINATIONS:
            await self._start_account_workers(dest)

        log.info("Ingester Service running. Press Ctrl+C to stop.")
        
        # Wait until stop signal
        await self._stop_event.wait()
        await self.shutdown()

    async def shutdown(self) -> None:
        """Clean up resources."""
        log.info("Shutting down Ingester Service...")
        
        # Cancel all running tasks
        for t in self._tasks:
            t.cancel()
        
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            
        # Close DB pool
        if self.repo:
            await self.repo.close()
            
        log.info("Shutdown complete.")
