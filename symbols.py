"""Helpers for serving symbol metadata to API consumers."""

from __future__ import annotations

import logging
import threading
import time
from types import MappingProxyType
from typing import Dict, Iterable, List, Mapping, Optional, Tuple

from brokers.symbol_map import SYMBOLS_KEY, get_symbol_map, refresh_symbol_map

log = logging.getLogger(__name__)

_CACHE_LOCK = threading.Lock()
_SYMBOL_SNAPSHOT: Optional[Tuple[Mapping[str, str], ...]] = None
_LAST_GOOD_SNAPSHOT: Optional[Tuple[Mapping[str, str], ...]] = None
_LAST_REFRESH_STARTED_AT: Optional[float] = None
_LAST_REFRESH_COMPLETED_AT: Optional[float] = None
_LAST_REFRESH_DURATION: Optional[float] = None
_LAST_REFRESH_SUCCESS: Optional[bool] = None
_REFRESH_THREAD_LOCK = threading.Lock()
_ASYNC_REFRESH_IN_FLIGHT = False


def _freeze(entries: Iterable[Mapping[str, str]]) -> Tuple[Mapping[str, str], ...]:
    """Return an immutable snapshot of ``entries`` suitable for caching."""

    return tuple(MappingProxyType(dict(entry)) for entry in entries)


def _thaw(snapshot: Tuple[Mapping[str, str], ...]) -> List[Dict[str, str]]:
    """Return a mutable list of plain dictionaries for JSON serialization."""

    return [dict(entry) for entry in snapshot]


def _build_symbol_entries() -> List[Dict[str, str]]:
    """Build the symbol list from the broker symbol map."""

    symbols: List[Dict[str, str]] = []
    symbol_map = get_symbol_map()
    broker_fields: Tuple[Tuple[str, Tuple[str, ...], str], ...] = (
        ("dhan", ("dhan",), "security_id"),
        ("aliceblue", ("aliceblue",), "symbol_id"),
        ("angelone", ("angelone", "angel"), "token"),
        ("iifl", ("iifl",), "token"),
        ("kotak", ("kotakneo",), "token"),
        ("upstox", ("upstox",), "token"),
    )
    
    for sym, data in symbol_map.items():
        entry: Dict[str, str] = {"symbol": sym}

        for exchange, mapping in data.items():
            if exchange == SYMBOLS_KEY:
                continue
            if not isinstance(mapping, Mapping):
                continue

            for output_key, broker_keys, field in broker_fields:
                if output_key in entry:
                    continue
                for broker_key in broker_keys:
                    broker_entry = mapping.get(broker_key)
                    if not isinstance(broker_entry, Mapping):
                        continue
                    broker_id = broker_entry.get(field)
                    if broker_id:
                        entry[output_key] = broker_id
                        break

        if len(entry) > 1:
            symbols.append(entry)
    return symbols


def refresh_symbol_snapshot(*, force: bool = False) -> bool:
    """Refresh the cached symbol snapshot, preserving the last good copy."""

    global _SYMBOL_SNAPSHOT, _LAST_GOOD_SNAPSHOT
    global _LAST_REFRESH_STARTED_AT, _LAST_REFRESH_COMPLETED_AT
    global _LAST_REFRESH_DURATION, _LAST_REFRESH_SUCCESS
    start_wall = time.time()
    start_perf = time.perf_counter()

    with _CACHE_LOCK:
        _LAST_REFRESH_STARTED_AT = start_wall

    try:
        refresh_symbol_map(force=force)
        new_snapshot = _freeze(_build_symbol_entries())
    except Exception:
        duration = time.perf_counter() - start_perf
        completed = time.time()
        with _CACHE_LOCK:
            _LAST_REFRESH_COMPLETED_AT = completed
            _LAST_REFRESH_DURATION = duration
            _LAST_REFRESH_SUCCESS = False
        log.exception("Symbol cache refresh failed after %.2fs", duration)
        return False

    duration = time.perf_counter() - start_perf
    completed = time.time()
    with _CACHE_LOCK:
        _SYMBOL_SNAPSHOT = new_snapshot
        _LAST_GOOD_SNAPSHOT = new_snapshot
        _LAST_REFRESH_COMPLETED_AT = completed
        _LAST_REFRESH_DURATION = duration
        _LAST_REFRESH_SUCCESS = True

    log.info(
        "Symbol cache refresh completed in %.2fs (%d symbols)",
        duration,
        len(new_snapshot),
    )
    return True


def refresh_symbols_background(*, force: bool = False) -> Optional[threading.Thread]:
    """Kick off an asynchronous refresh if one is not already running."""

    def _worker() -> None:
        try:
            refresh_symbol_snapshot(force=force)
        finally:
            global _ASYNC_REFRESH_IN_FLIGHT
            with _REFRESH_THREAD_LOCK:
                _ASYNC_REFRESH_IN_FLIGHT = False

    global _ASYNC_REFRESH_IN_FLIGHT
    with _REFRESH_THREAD_LOCK:
        if _ASYNC_REFRESH_IN_FLIGHT:
            return None
        _ASYNC_REFRESH_IN_FLIGHT = True

    thread = threading.Thread(
        target=_worker,
        name="symbol-cache-refresh",
        daemon=True,
    )
    thread.start()
    return thread


def get_symbols() -> Optional[List[Dict[str, str]]]:
    """Return the cached list of symbols or ``None`` if unavailable."""

    with _CACHE_LOCK:
        snapshot = _SYMBOL_SNAPSHOT
        if snapshot is None:
            snapshot = _LAST_GOOD_SNAPSHOT
        if snapshot is None:
            return None
        return _thaw(snapshot)


def get_symbol_cache_stats() -> Dict[str, Optional[float | bool | int]]:
    """Expose cache metadata for logging/monitoring purposes."""

    with _CACHE_LOCK:
        current = _SYMBOL_SNAPSHOT
        fallback = _LAST_GOOD_SNAPSHOT
        using_fallback = current is None and fallback is not None
        snapshot = current or fallback or ()
        return {
            "using_fallback": using_fallback,
            "symbol_count": len(snapshot),
            "last_refresh_started_at": _LAST_REFRESH_STARTED_AT,
            "last_refresh_completed_at": _LAST_REFRESH_COMPLETED_AT,
            "last_refresh_duration": _LAST_REFRESH_DURATION,
            "last_refresh_success": _LAST_REFRESH_SUCCESS,
        }
