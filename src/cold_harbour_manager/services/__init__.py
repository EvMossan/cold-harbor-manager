"""Alias to expose services via cold_harbour_manager namespace."""

from coldharbour_manager.services.risks_manager.risks_manager import (
    BreakevenOrderManager,
)

__all__ = ["BreakevenOrderManager"]
