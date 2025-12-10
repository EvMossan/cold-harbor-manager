"""Alias package that re-exports coldharbour_manager."""

from coldharbour_manager import ACCOUNT_BLUEPRINTS, accounts_bp, create_app

__all__ = ["create_app", "accounts_bp", "ACCOUNT_BLUEPRINTS"]
