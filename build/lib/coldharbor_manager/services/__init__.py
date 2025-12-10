"""Expose service modules through the coldharbor_manager namespace."""
from pathlib import Path

_BASE_DIR = Path(__file__).resolve().parent
_ALIAS_SERVICES = _BASE_DIR.parent / "cold_harbour" / "services"

__path__ = [str(_BASE_DIR)]
if _ALIAS_SERVICES.exists():
    __path__.append(str(_ALIAS_SERVICES))
