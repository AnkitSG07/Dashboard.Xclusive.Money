from __future__ import annotations

import os
from typing import Any, Dict

def get_redis_url() -> str:
    """Return the Redis URL for Celery.

    The function checks the ``CELERY_BROKER_URL`` and ``REDIS_URL``
    environment variables and returns the first one found.  If neither
    variable is defined, a :class:`RuntimeError` is raised so the caller is
    forced to explicitly configure the connection.
    """

    url = os.environ.get("CELERY_BROKER_URL") or os.environ.get("REDIS_URL")
    if not url:
        raise RuntimeError("CELERY_BROKER_URL or REDIS_URL must be set")
    return url
    

def _decode_event(raw: Dict[Any, Any]) -> Dict[str, Any]:
    """Decode Redis bytes into a plain ``dict``.

    The Redis client returns byte strings for stream values. This helper
    normalises the payload by decoding UTF-8 strings and converting numeric
    fields to ``int`` where possible.
    """
    event: Dict[str, Any] = {}
    for k, v in raw.items():
        key = k.decode() if isinstance(k, bytes) else k
        if isinstance(v, bytes):
            try:
                v = int(v)
            except ValueError:
                v = v.decode()
        event[key] = v
    return event
