"""Helpers that expose Airflow host configuration."""

from __future__ import annotations

import os


def get_airflow_hostname() -> str:
    """Return the hostname to use for log-server URLs."""

    return os.getenv("AIRFLOW_LOG_SERVER_HOST", "localhost")
