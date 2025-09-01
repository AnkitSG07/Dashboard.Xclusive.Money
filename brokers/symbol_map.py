"""Dynamic symbol map for Indian equities.

This module builds a mapping of every equity symbol available on the
Indian stock exchanges (NSE and BSE) across a number of supported
brokers.  The data is generated from publicly available instrument dumps
so that the map automatically stays up to date without having to maintain
it manually.

Only the common exchange token is used for most brokers as they rely on
the standard exchange instrumentation.  Dhan uses its own security id,
which is retrieved from their master scrip file and joined with the
exchange token information.
"""

from __future__ import annotations

import csv
import os
import time
from functools import lru_cache
from io import StringIO
from pathlib import Path
from typing import Dict, Tuple
import logging
import threading
import time
import requests

log = logging.getLogger(__name__)



# URLs that expose complete instrument dumps for the respective brokers.
ZERODHA_URL = "https://api.kite.trade/instruments"
DHAN_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

# Store downloaded instrument dumps locally so subsequent runs do not hammer
# the upstream APIs which are rate limited and occasionally return HTTP 429.
# The cache location and expiry can be tweaked via environment variables.
CACHE_DIR = Path(
    os.getenv("SYMBOL_MAP_CACHE_DIR", Path(__file__).parent / "_cache")
)
CACHE_DIR.mkdir(exist_ok=True)
CACHE_MAX_AGE = int(os.getenv("SYMBOL_MAP_CACHE_MAX_AGE", "86400"))  # seconds


def _fetch_csv(url: str, cache_name: str) -> str:
    """Return CSV content from *url* using a small on-disk cache.

    If the cache file exists and is younger than ``CACHE_MAX_AGE`` it is used
    directly.  Otherwise the data is downloaded from ``url`` and cached for
    future calls.  Should the download fail but a previous cache entry exist,
    the cached data is returned instead of raising an exception.
    """

    cache_file = CACHE_DIR / cache_name
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        text = resp.text
        try:
            cache_file.write_text(text)
        except Exception:
            # Caching is an optimisation only – ignore filesystem errors.
            pass
        return text
    except requests.RequestException:
        if cache_file.exists():
            age = time.time() - cache_file.stat().st_mtime
            if age < CACHE_MAX_AGE:
                return cache_file.read_text()
        raise

Key = Tuple[str, str]


@lru_cache(maxsize=1)
def _load_zerodha() -> Dict[Key, str]:
    """Return mapping of (symbol, exchange) to instrument token.

    Zerodha publishes an instrument dump containing tokens for all
    instruments across exchanges.  Only equity instruments from NSE and
    BSE are considered here.
    """

    csv_text = _fetch_csv(ZERODHA_URL, "zerodha_instruments.csv")

    reader = csv.DictReader(StringIO(csv_text))
    data: Dict[Key, str] = {}
    for row in reader:
        if row["segment"] in {"NSE", "BSE"} and row["instrument_type"] == "EQ":
            key = (row["tradingsymbol"], row["exchange"].upper())
            data[key] = row["instrument_token"]
    return data


@lru_cache(maxsize=1)
def _load_dhan() -> Dict[Key, str]:
    """Return mapping of (symbol, exchange) to Dhan security id."""

    csv_text = _fetch_csv(DHAN_URL, "dhan_scrip_master.csv")

    reader = csv.DictReader(StringIO(csv_text))
    data: Dict[Key, str] = {}
    for row in reader:
        if row["SEM_EXM_EXCH_ID"] in {"NSE", "BSE"} and row["SEM_SEGMENT"] == "E":
            key = (row["SEM_TRADING_SYMBOL"], row["SEM_EXM_EXCH_ID"].upper())
            # prefer the EQ series when available but fall back to any
            # other equity series so that newly listed or less common
            # scrips (e.g. BSE "X" group) are still included
            if key not in data or row["SEM_SERIES"] == "EQ":
                data[key] = row["SEM_SMST_SECURITY_ID"]
    return data


def build_symbol_map() -> Dict[str, Dict[str, Dict[str, Dict[str, str]]]]:
    """Construct the complete symbol map.

    The returned mapping is structured as ``{symbol -> {exchange -> broker
    -> fields}}`` so that symbols listed on multiple exchanges retain the data
    for each exchange separately.
    """

    zerodha = _load_zerodha()
    dhan = _load_dhan()

    mapping: Dict[str, Dict[str, Dict[str, Dict[str, str]]]] = {}

    for (symbol, exchange), token in zerodha.items():
        entry = {
            "zerodha": {
                "trading_symbol": symbol,
                "exchange": exchange,
                "token": token,
            },
            "aliceblue": {
                "trading_symbol": f"{symbol}-EQ",
                "symbol_id": token,
                "exch": exchange,
            },
            "fyers": {
                "symbol": f"{exchange}:{symbol}-EQ",
                "token": token,
            },
            "finvasia": {
                "symbol": f"{symbol}-EQ",
                "token": token,
                "exchange": exchange,
            },
            "flattrade": {
                "symbol": f"{symbol}-EQ",
                "token": token,
                "exchange": exchange,
            },
            "acagarwal": {
                "symbol": f"{symbol}-EQ",
                "token": token,
                "exchange": exchange,
            },
            "motilaloswal": {
                "symbol": symbol,
                "token": token,
                "exchange": exchange,
            },
            "kotakneo": {
                "symbol": symbol,
                "token": token,
                "exchange": exchange,
            },
            "tradejini": {
                "symbol": f"{symbol}-EQ",
                "token": token,
                "exchange": exchange,
            },
            "zebu": {
                "symbol": f"{symbol}-EQ",
                "token": token,
                "exchange": exchange,
            },
            "enrichmoney": {
                "symbol": f"{symbol}-EQ",
                "token": token,
                "exchange": exchange,
            },
        }

        dhan_id = dhan.get((symbol, exchange))
        if dhan_id:
            entry["dhan"] = {
                "security_id": dhan_id,
                "exchange_segment": f"{exchange}_EQ",
            }

        mapping.setdefault(symbol, {})[exchange] = entry

    return mapping


# Public symbol map used throughout the application.  It can be
# refreshed at runtime if newly listed symbols appear on the exchanges
# after the process has started.
SYMBOL_MAP = build_symbol_map()

_REFRESH_LOCK = threading.Lock()
_LAST_REFRESH = 0.0
_MIN_REFRESH_INTERVAL = 60.0  # seconds


def refresh_symbol_map(force: bool = False) -> None:
    """Reload the global :data:`SYMBOL_MAP` from upstream sources.

    The refresh is throttled to avoid hitting upstream rate limits.  If a
    refresh fails the previous symbol map is retained.
    """

    global SYMBOL_MAP, _LAST_REFRESH
    with _REFRESH_LOCK:
        if not force and time.time() - _LAST_REFRESH < _MIN_REFRESH_INTERVAL:
            return

        # Build the new map before replacing the old one so that existing
        # data remains available if the remote request fails.
        try:
            _load_zerodha.cache_clear()
            _load_dhan.cache_clear()
            new_map = build_symbol_map()
        except requests.RequestException as exc:  # pragma: no cover - network errors
            log.warning("Failed to refresh symbol map: %s", exc)
            return

        SYMBOL_MAP = new_map
        _LAST_REFRESH = time.time()


__all__ = ["SYMBOL_MAP", "build_symbol_map", "refresh_symbol_map"]


def get_symbol_for_broker(
    symbol: str, broker: str, exchange: str | None = None
) -> Dict[str, str]:
    """Return the mapping for *symbol* for the given *broker*.

    ``exchange`` may be supplied to explicitly select the exchange on
    which the symbol is listed.  When omitted the lookup attempts to
    infer the exchange from the symbol string or defaults to the NSE
    entry.
    """
    symbol = symbol.upper()
    broker = broker.lower()
    
    exchange_hint = None
    if exchange:
        exchange_hint = exchange.upper()
    elif ":" in symbol:
        exchange_hint, symbol = symbol.split(":", 1)

    base = symbol
    if base.endswith("-EQ"):
        base = base[:-3]

    base2 = base.split("-")[0]

    mapping = (
        SYMBOL_MAP.get(symbol)
        or SYMBOL_MAP.get(base)
        or SYMBOL_MAP.get(base2)
    )

    # If the symbol was not found we may be dealing with a newly listed
    # scrip.  Refresh the symbol map once so that recently added tokens are
    # picked up without requiring an application restart.
    if not mapping:
        refresh_symbol_map()
        mapping = (
            SYMBOL_MAP.get(symbol)
            or SYMBOL_MAP.get(base)
            or SYMBOL_MAP.get(base2)
        )

    if not mapping:
        return {}

    if exchange_hint and exchange_hint in mapping:
        exchange_map = mapping[exchange_hint]
    else:
        exchange_map = mapping.get("NSE") or next(iter(mapping.values()))

    return exchange_map.get(broker, {}) if exchange_map else {}


def get_symbol_by_token(token: str, broker: str) -> str | None:
    """Return the base symbol for a broker given its token/instrument id."""
    
    broker = broker.lower()
    token = str(token)

    for sym, exchanges in SYMBOL_MAP.items():
        for exchange_map in exchanges.values():
            broker_map = exchange_map.get(broker)
            if broker_map and str(broker_map.get("token")) == token:
                return sym
    return None


__all__.extend(["get_symbol_for_broker", "get_symbol_by_token"])
