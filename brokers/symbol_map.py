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
from typing import Dict, Tuple, Union
import logging
import threading
import time
import re
from datetime import datetime
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


def _fetch_csv(url: str, cache_name: str, force: bool = False) -> str:
    """Return CSV content from *url* using a small on-disk cache.

    When ``force`` is ``True`` the cache file is ignored and a fresh copy is
    downloaded from ``url``.  Otherwise the data is downloaded as usual and
    cached for future calls.  Should the download fail but a previous cache
    entry exist and is younger than :data:`CACHE_MAX_AGE`, the cached data is
    returned instead of raising an exception.
    """

    cache_file = CACHE_DIR / cache_name
    
    # The upstream endpoints occasionally respond with HTTP 429 when too many
    # concurrent requests are made.  To make the service more robust we retry
    # the download a couple of times before falling back to any cached data.
    for attempt in range(3):
        try:
            resp = requests.get(url, timeout=30)
            status = getattr(resp, "status_code", 200)
            if status == 429:
                # Respect the server's suggested retry delay if provided.
                retry_after = int(resp.headers.get("Retry-After", "1"))
                time.sleep(retry_after)
                continue
            resp.raise_for_status()
            text = resp.text
            try:
                cache_file.write_text(text)
            except Exception:
                # Caching is an optimisation only – ignore filesystem errors.
                pass
            return text
        except requests.RequestException:
            if not force and cache_file.exists():
                age = time.time() - cache_file.stat().st_mtime
                if age < CACHE_MAX_AGE:
                    return cache_file.read_text()
            # Wait a bit before the next retry in case of transient errors.
            time.sleep(2 ** attempt)
            continue
    # If we reach this point no cached data was available and all retries
    # failed.
    raise requests.RequestException(f"failed to fetch {url}")

Key = Tuple[str, str]

def _canonical_dhan_symbol(symbol: str, expiry_date: str | None = None) -> str:
    """Return canonical representation for Dhan derivative symbols.

    Dhan uses separators and CALL/PUT or CE/PE suffixes for option contracts,
    e.g. ``BASE-25NOV2023-35500-CALL``.  This normalises such names to the
    compact form used by other brokers while dropping the year component.
    If *symbol* does not match the expected pattern it is returned unchanged
    (aside from removal of separators).  When the day component is missing
    from the trading symbol the ``expiry_date`` field is used instead, ignoring
    any time portion.
    """

    symbol = symbol.strip().upper()

    # Option contracts: ``BASE-DDMMMYYYY-STRIKE-TYPE`` or variants where the
    # day and/or year may be omitted.  Since the canonical form drops the year
    # entirely, simply ignore it here.
    m = re.match(
        r"^([A-Z0-9]+)[-\s]+(?:(\d{1,2})?([A-Z]{3})(?:\d{2}|\d{4})?)[-\s]+(\d+(?:\.\d+)?)[-\s]+(CE|PE|CALL|PUT)$",
        symbol,
    )
    if m:
        base, day, month, strike, opt = m.groups()
        if not day and expiry_date:
            date_str = expiry_date.strip()
            try:
                day = f"{datetime.fromisoformat(date_str).day:02d}"
            except ValueError:
                date_str = date_str.split()[0]
                for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d-%m-%Y"):
                    try:
                        day = f"{datetime.strptime(date_str, fmt).day:02d}"
                        break
                    except ValueError:
                        continue
        if not day:
            return symbol.replace(" ", "").replace("-", "")
        opt = "CE" if opt in {"CALL", "CE"} else "PE"
        return f"{base}{day}{month}{strike}{opt}"

    # Futures contracts: ``BASE-DDMMMYYYY-FUT``
    m = re.match(
        r"^([A-Z0-9]+)[-\s]+(\d{1,2})([A-Z]{3})(\d{4})[-\s]+([A-Z]+)$",
        symbol,
    )
    if m:
        base, day, month, _year, fut = m.groups()
        return f"{base}{day}{month}{fut}"

    # Non-derivative symbols: strip separators.
    return symbol.replace(" ", "").replace("-", "")


@lru_cache(maxsize=1)
def _load_zerodha(force: bool = False) -> Dict[Key, Dict[str, Union[str, int]]]:
    """Return mapping of (symbol, exchange) to instrument info.

    Zerodha publishes an instrument dump containing tokens for all
    instruments across exchanges.  Only equity instruments from NSE and
    BSE are considered here.  For derivative instruments the ``lot_size``
    column is also parsed so that downstream consumers can size orders
    correctly.
    """

    csv_text = _fetch_csv(ZERODHA_URL, "zerodha_instruments.csv", force=force)

    reader = csv.DictReader(StringIO(csv_text))
    data: Dict[Key, Dict[str, Union[str, int]]] = {}
    for row in reader:
        segment = row["segment"].upper()
        inst_type = row["instrument_type"].upper()
        if segment in {"NSE", "BSE"} and inst_type == "EQ":
            key = (row["tradingsymbol"], row["exchange"].upper())
        elif segment in {"NFO", "BFO"} and inst_type in {
            "FUT", "FUTSTK", "FUTIDX", "OPT", "OPTSTK", "OPTIDX"
        }:
            key = (row["tradingsymbol"], segment)
        else:
            continue
        info = {"token": row["instrument_token"]}
        lot = row.get("lot_size")
        if lot:
            try:
                lot_int = int(float(lot))
            except ValueError:
                lot_int = None
            if lot_int:
                info["lot_size"] = lot_int
        data[key] = info
    return data


@lru_cache(maxsize=1)
def _load_dhan(force: bool = False) -> Dict[Key, Dict[str, Union[str, int]]]:
    """Return mapping of (symbol, exchange) to Dhan instrument info."""

    csv_text = _fetch_csv(DHAN_URL, "dhan_scrip_master.csv", force=force)

    reader = csv.DictReader(StringIO(csv_text))
    data: Dict[Key, Dict[str, Union[str, int]]] = {}
    for row in reader:
        exch = row["SEM_EXM_EXCH_ID"].upper()
        segment = row["SEM_SEGMENT"].upper()
        symbol = row["SEM_TRADING_SYMBOL"]
        lot = row.get("SEM_LOT_UNITS")
        lot_size = None
        if lot:
            try:
                lot_size = int(float(lot))
                if lot_size <= 0:
                    lot_size = None
            except (ValueError, TypeError):
                lot_size = None
        
        # Add logging for debugging lot size issues.
        if segment == "D" and lot_size is None:
            log.warning(f"Lot size is missing for derivative symbol {symbol} in Dhan CSV.")

        if exch in {"NSE", "BSE"} and segment == "E":
            key = (symbol, exch)
            if key not in data or row["SEM_SERIES"] == "EQ":
                info: Dict[str, str] = {"security_id": row["SEM_SMST_SECURITY_ID"]}
                if lot_size:
                    info["lot_size"] = lot_size
                existing = data.get(key)
                if existing and "lot_size" in existing and "lot_size" not in info:
                    info["lot_size"] = existing["lot_size"]
                if existing:
                    existing.update(info)
                    data[key] = existing
                else:
                    data[key] = info
        elif exch in {"NSE", "BSE"} and segment == "D":
            symbol = _canonical_dhan_symbol(symbol, row.get("SEM_EXPIRY_DATE"))
            key = (symbol, "NFO" if exch == "NSE" else "BFO")
            info = {"security_id": row["SEM_SMST_SECURITY_ID"]}
            if lot_size:
                info["lot_size"] = lot_size
            existing = data.get(key)
            if existing and "lot_size" in existing and "lot_size" not in info:
                info["lot_size"] = existing["lot_size"]
            if existing:
                existing.update(info)
                data[key] = existing
            else:
                data[key] = info
    return data


def build_symbol_map(force: bool = False) -> Dict[str, Dict[str, Dict[str, Dict[str, Union[str, int]]]]]:
    """Construct the complete symbol map.

    The returned mapping is structured as ``{symbol -> {exchange -> broker
    -> fields}}`` so that symbols listed on multiple exchanges retain the data
    for each exchange separately.
    """

    zerodha = _load_zerodha(force=force)
    dhan = _load_dhan(force=force)

    mapping: Dict[str, Dict[str, Dict[str, Dict[str, Union[str, int]]]]] = {}

    # First, build the map with Zerodha as the base, as before
    for (symbol, exchange), info in zerodha.items():
        token = info["token"]
        lot_size = info.get("lot_size")
        is_equity = exchange in {"NSE", "BSE"}
        if is_equity:
            ab_symbol = f"{symbol}-EQ"
            fyers_symbol = f"{exchange}:{symbol}-EQ"
        else:
            ab_symbol = symbol
            fyers_symbol = f"{exchange}:{symbol}"
        entry = {
            "zerodha": {
                "trading_symbol": symbol,
                "exchange": exchange,
                "token": token,
            },
            "aliceblue": {
                "trading_symbol": ab_symbol,
                "symbol_id": token,
                "exch": exchange,
            },
            "fyers": {
                "symbol": fyers_symbol,
                "token": token,
            },
            "finvasia": {
                "symbol": ab_symbol,
                "token": token,
                "exchange": exchange,
            },
            "flattrade": {
                "symbol": ab_symbol,
                "token": token,
                "exchange": exchange,
            },
            "acagarwal": {
                "symbol": ab_symbol,
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
                "symbol": ab_symbol,
                "token": token,
                "exchange": exchange,
            },
            "zebu": {
                "symbol": ab_symbol,
                "token": token,
                "exchange": exchange,
            },
            "enrichmoney": {
                "symbol": ab_symbol,
                "token": token,
                "exchange": exchange,
            },
        }

        dhan_info = dhan.get((symbol, exchange))
        if dhan_info:
            if exchange in {"NSE", "BSE"}:
                exch_segment = f"{exchange}_EQ"
            elif exchange in {"NFO", "BFO"}:
                exch_segment = f"{'NSE' if exchange == 'NFO' else 'BSE'}_FNO"
            else:
                exch_segment = exchange
            lot_size = lot_size or dhan_info.get("lot_size")
            dhan_entry = {
                "security_id": dhan_info["security_id"],
                "exchange_segment": exch_segment,
            }
            if lot_size:
                dhan_entry["lot_size"] = lot_size
            entry["dhan"] = dhan_entry

        if lot_size:
            for broker_map in entry.values():
                broker_map["lot_size"] = lot_size

        mapping.setdefault(symbol, {})[exchange] = entry
    
    # CORRECTED: Now, iterate through Dhan's data to add any missing symbols.
    # This ensures that derivatives with different naming conventions are included.
    for (symbol, exchange), info in dhan.items():
        # Check if this symbol/exchange combination was already added from Zerodha's data
        if symbol in mapping and exchange in mapping[symbol]:
            continue

        # If not, it's a symbol unique to Dhan's list or has a different name. Add it.
        if exchange in {"NSE", "BSE"}:
            exch_segment = f"{exchange}_EQ"
        elif exchange in {"NFO", "BFO"}:
            exch_segment = f"{'NSE' if exchange == 'NFO' else 'BSE'}_FNO"
        else:
            exch_segment = exchange
        
        entry = {
            "dhan": {
                "security_id": info["security_id"],
                "exchange_segment": exch_segment,
            }
        }
        lot_size = info.get("lot_size")
        if lot_size:
            entry["dhan"]["lot_size"] = lot_size
        mapping.setdefault(symbol, {})[exchange] = entry

    return mapping


# Public symbol map used throughout the application.  It is loaded on
# first access rather than at import time so that processes that do not
# require the data avoid the heavy upfront memory cost.  When the map is
# built before forking worker processes the pages are shared via
# copy‑on‑write which keeps the overall memory footprint low.
SYMBOL_MAP: Dict[str, Dict[str, Dict[str, Dict[str, Union[str, int]]]]] = {}

_REFRESH_LOCK = threading.Lock()
_LAST_REFRESH = 0.0
_MIN_REFRESH_INTERVAL = 60.0  # seconds

def _ensure_symbol_map() -> Dict[str, Dict[str, Dict[str, Dict[str, Union[str, int]]]]]:
    """Return the global symbol map, loading it if necessary."""

    global SYMBOL_MAP, _LAST_REFRESH
    if not SYMBOL_MAP:
        SYMBOL_MAP = build_symbol_map()
        _LAST_REFRESH = time.time()
    return SYMBOL_MAP


def get_symbol_map() -> Dict[str, Dict[str, Dict[str, Dict[str, Union[str, int]]]]]:
    """Public wrapper to obtain the global symbol map."""

    return _ensure_symbol_map()


def refresh_symbol_map(force: bool = False) -> None:
    """Reload the global :data:`SYMBOL_MAP` from upstream sources.

    The refresh is throttled to avoid hitting upstream rate limits.  When
    ``force`` is ``True`` the throttling is bypassed and the underlying CSV
    files are re-downloaded instead of using any cached copies.  If a refresh
    fails the previous symbol map is retained.
    """

    global SYMBOL_MAP, _LAST_REFRESH
    with _REFRESH_LOCK:
        if not force and SYMBOL_MAP and time.time() - _LAST_REFRESH < _MIN_REFRESH_INTERVAL:
            return

        # Build the new map before replacing the old one so that existing
        # data remains available if the remote request fails.
        try:
            _load_zerodha.cache_clear()
            _load_dhan.cache_clear()
            new_map = build_symbol_map(force=force)
        except requests.RequestException as exc:  # pragma: no cover - network errors
            log.warning("Failed to refresh symbol map: %s", exc)
            return

        SYMBOL_MAP = new_map
        _LAST_REFRESH = time.time()


__all__ = ["SYMBOL_MAP", "build_symbol_map", "refresh_symbol_map", "get_symbol_map"]


def get_symbol_for_broker(
    symbol: str, broker: str, exchange: str | None = None
) -> Dict[str, Union[str, int]]:
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

    symbol_map = _ensure_symbol_map()
    mapping = (
        symbol_map.get(symbol)
        or symbol_map.get(base)
        or symbol_map.get(base2)
    )

    # If the symbol was not found we may be dealing with a newly listed
    # scrip.  Refresh the symbol map once so that recently added tokens are
    # picked up without requiring an application restart.
    if not mapping:
        refresh_symbol_map()
        symbol_map = _ensure_symbol_map()
        mapping = (
            symbol_map.get(symbol)
            or symbol_map.get(base)
            or symbol_map.get(base2)
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

    symbol_map = _ensure_symbol_map()
    for sym, exchanges in symbol_map.items():
        for exchange_map in exchanges.values():
            broker_map = exchange_map.get(broker)
            if broker_map and str(broker_map.get("token")) == token:
                return sym
    return None


__all__.extend(["get_symbol_for_broker", "get_symbol_by_token"])
