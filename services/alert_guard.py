from __future__ import annotations

"""Utilities for filtering duplicate alerts and enforcing basic risk checks.

This module also provides helpers for retrieving and updating per-user
settings from a shared Redis datastore.  Settings are cached locally with a
time-to-live in order to avoid excessive Redis lookups when multiple events
for the same user are processed in quick succession.
"""

import hashlib
import json
import os
import time
from typing import Any, Dict, Tuple


import redis
from marshmallow import ValidationError


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_client = redis.Redis.from_url(REDIS_URL)

# Time-to-live for deduplication keys in seconds
DEDUP_TTL = 60

# Prefix for storing user settings in Redis
_SETTINGS_KEY = "user_settings:{user_id}"
# Time-to-live for the in-process cache of user settings (seconds)
SETTINGS_CACHE_TTL = 60
# Local cache mapping user_id -> (settings, expiry_timestamp)
_USER_SETTINGS_CACHE: Dict[int, Tuple[Dict[str, Any], float]] = {}
# In-memory fallback for deduplication when Redis is unavailable.
_LOCAL_DEDUP: Dict[str, float] = {}

def get_user_settings(user_id: int) -> Dict[str, Any]:
    """Return settings for *user_id*.

    Settings are fetched from Redis and cached locally for
    :data:`SETTINGS_CACHE_TTL` seconds.
    """

    now = time.time()
    cached = _USER_SETTINGS_CACHE.get(user_id)
    if cached and cached[1] > now:
        return cached[0]

    try:
        raw = redis_client.get(_SETTINGS_KEY.format(user_id=user_id))
    except redis.exceptions.RedisError:
        raw = None
    if raw is None:
        settings: Dict[str, Any] = {}
    else:
        if isinstance(raw, bytes):
            raw = raw.decode()
        try:
            settings = json.loads(raw)
        except json.JSONDecodeError:
            settings = {}
    _USER_SETTINGS_CACHE[user_id] = (settings, now + SETTINGS_CACHE_TTL)
    return settings


def update_user_settings(user_id: int, settings: Dict[str, Any], *, ttl: int | None = None) -> None:
    """Persist *settings* for *user_id* and refresh the local cache."""

    try:
        redis_client.set(
            _SETTINGS_KEY.format(user_id=user_id), json.dumps(settings), ex=ttl
        )
    except redis.exceptions.RedisError:
        pass
    _USER_SETTINGS_CACHE[user_id] = (settings, time.time() + SETTINGS_CACHE_TTL)


def _dedup_key(event: Dict[str, Any]) -> str:
    """Return a cache key used for duplicate detection."""

    if event.get("alert_id"):
        return f"alert:{event['alert_id']}"
    payload = (
        f"{event['user_id']}|{event.get('strategy_id')}|"
        f"{event['symbol']}|{event['action']}|{event['qty']}"
    )
    return "alert:" + hashlib.sha1(payload.encode()).hexdigest()


def check_risk_limits(event: Dict[str, Any]) -> bool:
    """Validate *event* against user-configured risk limits."""

    settings = get_user_settings(event["user_id"])

    max_qty = settings.get("max_qty")
    if max_qty is not None and event["qty"] > max_qty:
        raise ValidationError("quantity exceeds max allowed")

    allowed = settings.get("allowed_symbols")
    if allowed and event["symbol"] not in allowed:
        raise ValidationError("symbol not allowed")

    return True


def check_duplicate_and_risk(event: Dict[str, Any]) -> bool:
    """Validate *event* against duplicate delivery and risk settings.

    Raises ``ValidationError`` if the event should not be processed.
    Returns ``True`` when the event passes all checks.
    """

    key = _dedup_key(event)
    # SET with NX ensures duplicates are rejected while setting a TTL for
    # automatic expiry of the deduplication key.
    try:
        if not redis_client.set(key, "1", nx=True, ex=DEDUP_TTL):
            raise ValidationError("duplicate alert")
    except redis.exceptions.RedisError:
        # Fallback to an in-memory set with simple TTL pruning
        now = time.time()
        expired = [k for k, exp in _LOCAL_DEDUP.items() if exp < now]
        for k in expired:
            _LOCAL_DEDUP.pop(k, None)
        if key in _LOCAL_DEDUP:
            raise ValidationError("duplicate alert")
        _LOCAL_DEDUP[key] = now + DEDUP_TTL

    check_risk_limits(event)
    return True


__all__ = [
    "check_duplicate_and_risk",
    "check_risk_limits",
    "get_user_settings",
    "update_user_settings",
    "redis_client",
]
