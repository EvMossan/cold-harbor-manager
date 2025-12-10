"""Expose the existing cold_harbour modules via coldharbor_manager."""
from pathlib import Path

_THIS_DIR = Path(__file__).resolve().parent
_ALIAS_DIR = _THIS_DIR.parent / "cold_harbour"
if _ALIAS_DIR.exists():
    __path__.append(str(_ALIAS_DIR))
