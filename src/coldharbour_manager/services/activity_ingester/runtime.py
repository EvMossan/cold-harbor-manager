"""
Runtime orchestrator for the Data Ingester service covering all
configured destination accounts.
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
    """Drive ingestion across the configured destinations."""

    def __init__(self) -> None:
        self.repo: Optional[AsyncAccountRepository] = None
        self._tasks: List[asyncio.Task] = []
        self._stop_event = asyncio.Event()
        
        # Prefer the local Postgres DSN before falling back to tunnels.
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
        """Initialize the database connection pool."""
        log.info("Initializing Ingester Service...")
        self.repo = await AsyncAccountRepository.create(self._db_dsn)
        log.info("Database connection established.")

    async def _start_account_workers(self, dest: Dict[str, Any]) -> None:
        """Launch workers for a destination account."""
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
        
        # Ensure schema and tables exist before spinning up workers.
        await storage.ensure_schema_and_tables(self.repo, slug)

        # Kick off the metadata check so raw_history_meta exists.
        meta_task = asyncio.create_task(
            workers.run_startup_meta_check(
                self.repo, api_key, secret_key, base_url, slug
            ),
            name=f"meta-{slug}"
        )
        self._tasks.append(meta_task)

        # Run backfill without blocking stream startup.
        backfill_task = asyncio.create_task(
            workers.run_backfill_task(
                self.repo, api_key, secret_key, base_url, slug
            ),
            name=f"backfill-{slug}"
        )
        self._tasks.append(backfill_task)

        # Run the stream consumer loop with automatic restart logic.
        stream_task = self._spawn_guarded(
            f"stream-{slug}",
            lambda: workers.run_stream_consumer(
                self.repo, api_key, secret_key, base_url, slug
            )
        )
        self._tasks.append(stream_task)

        # Run the healing loop with the same restart guard.
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
        """Run a task loop that restarts failed workers."""
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
        """Start all workers and await a termination signal."""
        await self.init()

        # Register signal handlers for graceful shutdown.
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._stop_event.set)
            except NotImplementedError:
                # Some environments lack support for these handlers.
                pass

        log.info(f"Found {len(DESTINATIONS)} destinations configuration.")

        # Launch workers for each configured destination.
        for dest in DESTINATIONS:
            await self._start_account_workers(dest)

        log.info("Ingester Service running. Press Ctrl+C to stop.")
        
        # Wait until stop signal.
        await self._stop_event.wait()
        await self.shutdown()

    async def shutdown(self) -> None:
        """Cancel running work and close resources."""
        log.info("Shutting down Ingester Service...")
        
        # Cancel all running tasks.
        for t in self._tasks:
            t.cancel()
        
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            
        # Close the DB pool.
        if self.repo:
            await self.repo.close()
            
        log.info("Shutdown complete.")
