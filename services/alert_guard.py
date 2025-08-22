from __future__ import annotations

"""Utilities for filtering duplicate alerts and enforcing basic risk checks."""

import hashlib
import os
from typing import Any, Dict

import redis
from marshmallow import ValidationError


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_client = redis.Redis.from_url(REDIS_URL)

# Time-to-live for deduplication keys in seconds
DEDUP_TTL = 60

# Simple in-memory representation of user risk settings.  Tests can
# monkeypatch this dictionary to supply per-user limits and allowed symbols as
# well as broker configuration.
USER_SETTINGS: Dict[int, Dict[str, Any]] = {}


def _dedup_key(event: Dict[str, Any]) -> str:
    """Return a cache key used for duplicate detection."""

    if event.get("alert_id"):
        return f"alert:{event['alert_id']}"
    payload = (
        f"{event['user_id']}|{event.get('strategy_id')}|"
        f"{event['symbol']}|{event['action']}|{event['qty']}"
    )
    return "alert:" + hashlib.sha1(payload.encode()).hexdigest()


def check_duplicate_and_risk(event: Dict[str, Any]) -> bool:
    """Validate *event* against duplicate delivery and risk settings.

    Raises ``ValidationError`` if the event should not be processed.
    Returns ``True`` when the event passes all checks.
    """

    key = _dedup_key(event)
    # SET with NX ensures duplicates are rejected while setting a TTL for
    # automatic expiry of the deduplication key.
    if not redis_client.set(key, "1", nx=True, ex=DEDUP_TTL):
        raise ValidationError("duplicate alert")

    settings = USER_SETTINGS.get(event["user_id"], {})

    max_qty = settings.get("max_qty")
    if max_qty is not None and event["qty"] > max_qty:
        raise ValidationError("quantity exceeds max allowed")

    allowed = settings.get("allowed_symbols")
    if allowed and event["symbol"] not in allowed:
        raise ValidationError("symbol not allowed")

    return True


__all__ = ["check_duplicate_and_risk", "USER_SETTINGS", "redis_client"]
