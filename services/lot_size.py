"""Shared helpers for working with instrument lot sizes."""

from __future__ import annotations

from typing import Any


def normalize_lot_size(raw_value: Any) -> int | None:
    """Return a positive integer lot size or ``None`` if invalid."""

    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        return None
    if value <= 0:
        return None
    return int(value)


__all__ = ["normalize_lot_size"]
