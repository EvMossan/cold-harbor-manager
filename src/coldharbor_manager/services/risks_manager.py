"""Expose :class:`RiskManager` without colliding with legacy packages."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Optional


def _load_risks_module() -> ModuleType:
    """
    Load the bundled risks_manager without colliding with other installs.

    The module is loaded under a private name to avoid picking up any
    existing ``cold_harbour`` on sys.path.
    """
    module_path = (
        Path(__file__)
        .resolve()
        .parents[2]
        / "cold_harbour"
        / "services"
        / "risks_manager"
        / "risks_manager.py"
    )
    spec = importlib.util.spec_from_file_location(
        "_cold_harbour_web_risks_manager", module_path
    )
    module: Optional[ModuleType] = (
        importlib.util.module_from_spec(spec) if spec else None
    )
    if module is None or spec is None or spec.loader is None:
        raise ImportError("Failed to load bundled risks_manager module")
    spec.loader.exec_module(module)
    return module


_risks_mod = _load_risks_module()
RiskManager = _risks_mod.RiskManager

__all__ = ["RiskManager"]
