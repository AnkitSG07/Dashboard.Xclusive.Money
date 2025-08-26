from __future__ import annotations

from typing import Any, Dict


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
