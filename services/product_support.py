from __future__ import annotations

"""Shared helpers for product-type normalization and MTF eligibility caching."""

from cache import cache_get, cache_set

# Cache for MTF eligibility per broker:symbol with TTL
MTF_CACHE_TTL = 3600  # one hour


def map_product_type(product_type: str | None, broker: str) -> str:
    """Normalize product type from payload to broker-specific value."""
    broker_normalized = (broker or "").lower()
    pt = str(product_type or "").strip().lower()

    if pt in {"cnc", "long term", "longterm", "delivery"}:
        base = "CNC"
    elif pt in {"mis", "intraday"}:
        base = "MIS"
    elif pt in {"mtf", "mtf_or_cnc", "mtf_or_longterm", "mtf or longterm"}:
        base = "MTF"
    else:
        base = None

    if base is None:
        return "INTRADAY" if broker_normalized in {"dhan", "fyers"} else "MIS"
    if base == "MIS":
        return "INTRADAY" if broker_normalized in {"dhan", "fyers"} else "MIS"
    if base == "MTF":
        return "MTF"
    if base == "CNC":
        return "CNC"
    return base


def is_mtf_supported(symbol: str, broker: str):
    """Return cached MTF support for a symbol/broker if present."""
    key = f"mtf_support:{broker}:{symbol}".lower()
    return cache_get(key)


def _cache_mtf_support(symbol: str, broker: str, supported: bool) -> None:
    """Update the MTF support cache for ``symbol``/``broker``."""
    key = f"mtf_support:{broker}:{symbol}".lower()
    cache_set(key, bool(supported), ttl=MTF_CACHE_TTL)


__all__ = [
    "MTF_CACHE_TTL",
    "map_product_type",
    "is_mtf_supported",
    "_cache_mtf_support",
]
