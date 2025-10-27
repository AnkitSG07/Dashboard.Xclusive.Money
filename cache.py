import os
import json
import time
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional
from uuid import UUID

import redis

_redis_client = None
_local_store: dict[str, tuple[Any, Optional[float]]] = {}

def _json_default(value: Any) -> Any:
    """Serialize non-JSON primitives for :func:`json.dumps`."""

    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, UUID):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")

def _get_client():
    global _redis_client
    if _redis_client is None:
        url = os.environ.get("REDIS_URL")
        if url:
            try:
                _redis_client = redis.from_url(url, decode_responses=True)
                _redis_client.ping()
            except Exception:
                _redis_client = False  # type: ignore
        else:
            _redis_client = False  # type: ignore
    return _redis_client if _redis_client not in (None, False) else None


def cache_set(key: str, value: Any, ttl: int | None = None) -> None:
    client = _get_client()
    serialized = json.dumps(value, default=_json_default)
    if client:
        client.set(name=key, value=serialized, ex=ttl)
    else:
        expiry = time.time() + ttl if ttl else None
        _local_store[key] = (serialized, expiry)


def cache_get(key: str) -> Optional[Any]:
    client = _get_client()
    if client:
        data = client.get(key)
        if data is None:
            return None
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            return data
    entry = _local_store.get(key)
    if not entry:
        return None
    value, expiry = entry
    if expiry and time.time() > expiry:
        del _local_store[key]
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def cache_delete(key: str) -> None:
    client = _get_client()
    if client:
        client.delete(key)
    else:
        _local_store.pop(key, None)


def cache_clear(prefix: str = "") -> None:
    client = _get_client()
    if client:
        if prefix:
            for k in client.scan_iter(f"{prefix}*"):
                client.delete(k)
        else:
            client.flushdb()
    else:
        if prefix:
            for k in list(_local_store.keys()):
                if k.startswith(prefix):
                    del _local_store[k]
        else:
            _local_store.clear()
