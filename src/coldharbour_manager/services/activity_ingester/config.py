"""Configuration constants for the Data Ingester service."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class IngesterConfig:
    """Static configuration for the ingestion service."""

    # Database Schema
    DB_SCHEMA: str = "account_activities"

    # Worker Intervals
    # How often to run the "healing" REST poll to catch missed events (seconds)
    HEALING_INTERVAL_SEC: int = 60

    # How far back to look during a healing poll (seconds)
    HEALING_LOOKBACK_SEC: int = 3600 * 200  # 10 minutes

    # Backfill Settings
    # How far back to download history on the very first run (days)
    BACKFILL_DAYS: int = 365 * 1

    # Batching
    # Max items to insert in one SQL transaction
    DB_BATCH_SIZE: int = 1000

    # Alpaca API Constraints
    # Max page size for REST requests
    API_PAGE_SIZE: int = 100