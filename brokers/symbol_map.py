"""Dynamic symbol map for Indian equities with lot size support.

This module builds a mapping of every equity symbol available on the
Indian stock exchanges (NSE and BSE) across a number of supported
brokers, including lot size information for derivatives.
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
import re
from datetime import datetime

log = logging.getLogger(__name__)

# URLs that expose complete instrument dumps for the respective brokers.
ZERODHA_URL = "https://api.kite.trade/instruments"
DHAN_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

# Store downloaded instrument dumps locally
CACHE_DIR = Path(
    os.getenv("SYMBOL_MAP_CACHE_DIR", Path(__file__).parent / "_cache")
)
CACHE_DIR.mkdir(exist_ok=True)
CACHE_MAX_AGE = int(os.getenv("SYMBOL_MAP_CACHE_MAX_AGE", "86400"))  # seconds


def _fetch_csv(url: str, cache_name: str, force: bool = False) -> str:
    """Return CSV content from *url* using a small on-disk cache."""
    cache_file = CACHE_DIR / cache_name
    
    # Try to use cache if it's fresh
    if not force and cache_file.exists():
        age = time.time() - cache_file.stat().st_mtime
        if age < CACHE_MAX_AGE:
            return cache_file.read_text()
    
    # Download fresh data
    for attempt in range(3):
        try:
            resp = requests.get(url, timeout=30)
            status = getattr(resp, "status_code", 200)
            if status == 429:
                retry_after = int(resp.headers.get("Retry-After", "1"))
                time.sleep(retry_after)
                continue
            resp.raise_for_status()
            text = resp.text
            try:
                cache_file.write_text(text)
            except Exception:
                pass
            return text
        except requests.RequestException:
            if cache_file.exists() and not force:
                return cache_file.read_text()
            time.sleep(2 ** attempt)
            continue
    raise requests.RequestException(f"failed to fetch {url}")

Key = Tuple[str, str]


@lru_cache(maxsize=1)
def _load_zerodha(force: bool = False) -> Dict[Key, Dict[str, str]]:
    """Return mapping of (symbol, exchange) to instrument data including lot size."""
    csv_text = _fetch_csv(ZERODHA_URL, "zerodha_instruments.csv", force)
    
    reader = csv.DictReader(StringIO(csv_text))
    data: Dict[Key, Dict[str, str]] = {}
    
    for row in reader:
        segment = row["segment"].upper()
        inst_type = row["instrument_type"].upper()


        lot_size_raw = row.get("lot_size", "1")
        try:
            lot_size = int(lot_size_raw)
        except ValueError:
            lot_size = 1
    
        if segment in {"NSE", "BSE"} and inst_type == "EQ":
            key = (row["tradingsymbol"], row["exchange"].upper())
            data[key] = {
                "instrument_token": row["instrument_token"],
                "lot_size": lot_size  # Equity lot size is usually 1
            }
        elif segment in {"NFO", "BFO"} and inst_type in {
            "FUT", "FUTSTK", "FUTIDX", "OPT", "OPTSTK", "OPTIDX", "CE", "PE"
        }:
            key = (row["tradingsymbol"], segment)
            data[key] = {
                "instrument_token": row["instrument_token"],
                "lot_size": lot_size  # Equity lot size is usually 1
            }
    
    return data

def _canonical_dhan_symbol(trading_symbol: str, expiry_date: str | None = None) -> str:
    """Return a normalized Dhan trading symbol.

    The canonical form removes spaces and hyphens, normalizes option type
    (``CALL``/``PUT`` → ``CE``/``PE``) and drops any year component from the
    expiry. If the trading symbol omits the day, *expiry_date* may be provided
    (in ISO format) to supply it.
    """

    if not trading_symbol:
        return ""

    s = trading_symbol.upper().replace("CALL", "CE").replace("PUT", "PE")
    s = s.replace(" ", "").replace("-", "")

    # Determine suffix (CE/PE/FUT)
    if s.endswith(("CE", "PE")):
        suffix = s[-2:]
        body = s[:-2]
    elif s.endswith("FUT"):
        suffix = "FUT"
        body = s[:-3]
    else:
        return s

    # Extract strike price (last 5 or 4 digits)
    strike = ""
    for ln in (5, 4):
        if len(body) >= ln and body[-ln:].isdigit():
            strike = body[-ln:]
            body = body[:-ln]
            break

    # Parse expiry portion
    m = re.search(r'([A-Z]{3})(\d{2,4})?$', body)
    if m:
        month = m.group(1)
        prefix = body[: m.start()]
        day_match = re.search(r'(\d{1,2})$', prefix)
        if day_match and 1 <= int(day_match.group(1)) <= 31:
            day = day_match.group(1)
            underlying = prefix[: day_match.start()]
        else:
            day = ""
            underlying = prefix

        if not day and expiry_date:
            try:
                dt = datetime.fromisoformat(expiry_date.split()[0])
                day = f"{dt.day:02d}"
            except Exception:
                pass

        return f"{underlying}{day}{month}{strike}{suffix}"

    return s


@lru_cache(maxsize=1)
def _load_dhan(force: bool = False) -> Dict[Key, Dict[str, str]]:
    """Return mapping of (symbol, exchange) to Dhan security data including lot size."""
    csv_text = _fetch_csv(DHAN_URL, "dhan_scrip_master.csv", force)
    
    reader = csv.DictReader(StringIO(csv_text))
    data: Dict[Key, Dict[str, str]] = {}
    
    for row in reader:
        exch = row["SEM_EXM_EXCH_ID"].upper()
        segment = row["SEM_SEGMENT"].upper()
        symbol = _canonical_dhan_symbol(
            row.get("SEM_TRADING_SYMBOL", ""), row.get("SEM_EXPIRY_DATE")
        )

        lot_raw = row.get("SEM_LOT_UNITS")
        try:
            lot_size = int(lot_raw) if lot_raw else 1
        except ValueError:
            lot_size = 1

        if exch in {"NSE", "BSE"} and segment == "E":
            key = (symbol, exch)
            # Prefer EQ series when available
            if key not in data or row["SEM_SERIES"] == "EQ":
                if key in data and lot_size == 1:
                    lot_size = data[key]["lot_size"]
                data[key] = {
                    "security_id": row["SEM_SMST_SECURITY_ID"],
                    "lot_size": lot_size,
                }
        elif exch in {"NSE", "BSE"} and segment == "D":
            # Derivatives segment
            key = (symbol, "NFO" if exch == "NSE" else "BFO")
            data[key] = {
                "security_id": row["SEM_SMST_SECURITY_ID"],
                "lot_size": lot_size
            }
    
    return data


def build_symbol_map() -> Dict[str, Dict[str, Dict[str, Dict[str, str]]]]:
    """Construct the complete symbol map with lot size information."""
    zerodha_data = _load_zerodha(force=True)
    dhan_data = _load_dhan(force=True)
    
    mapping: Dict[str, Dict[str, Dict[str, Dict[str, str]]]] = {}
    
    # Build map with Zerodha as base
    for (symbol, exchange), zdata in zerodha_data.items():
        token = zdata.get("instrument_token", "")
        lot_size = zdata.get("lot_size", 1)

        dhan_info = dhan_data.get((symbol, exchange))
        if dhan_info and dhan_info.get("lot_size", 1) != 1:
            lot_size = dhan_info["lot_size"]

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
                "lot_size": lot_size
            },
            "aliceblue": {
                "trading_symbol": ab_symbol,
                "symbol_id": token,
                "exch": exchange,
                "lot_size": lot_size
            },
            "fyers": {
                "symbol": fyers_symbol,
                "token": token,
                "lot_size": lot_size
            },
            "finvasia": {
                "symbol": ab_symbol,
                "token": token,
                "exchange": exchange,
                "lot_size": lot_size
            },
            "flattrade": {
                "symbol": ab_symbol,
                "token": token,
                "exchange": exchange,
                "lot_size": lot_size
            },
            "acagarwal": {
                "symbol": ab_symbol,
                "token": token,
                "exchange": exchange,
                "lot_size": lot_size
            },
            "motilaloswal": {
                "symbol": symbol,
                "token": token,
                "exchange": exchange,
                "lot_size": lot_size
            },
            "kotakneo": {
                "symbol": symbol,
                "token": token,
                "exchange": exchange,
                "lot_size": lot_size
            },
            "tradejini": {
                "symbol": ab_symbol,
                "token": token,
                "exchange": exchange,
                "lot_size": lot_size
            },
            "zebu": {
                "symbol": ab_symbol,
                "token": token,
                "exchange": exchange,
                "lot_size": lot_size
            },
            "enrichmoney": {
                "symbol": ab_symbol,
                "token": token,
                "exchange": exchange,
                "lot_size": lot_size
            },
        }
        
        # Add Dhan data if available
        if dhan_info:
            if exchange in {"NSE", "BSE"}:
                exch_segment = f"{exchange}_EQ"
            elif exchange in {"NFO", "BFO"}:
                exch_segment = f"{'NSE' if exchange == 'NFO' else 'BSE'}_FNO"
            else:
                exch_segment = exchange
            
            entry["dhan"] = {
                "security_id": dhan_info["security_id"],
                "exchange_segment": exch_segment,
                "lot_size": dhan_info.get("lot_size", lot_size)  # Use Dhan's lot size or fallback
            }
        
        mapping.setdefault(symbol, {})[exchange] = entry
    
    # Add Dhan-only symbols (not in Zerodha)
    for (symbol, exchange), dhan_info in dhan_data.items():
        if symbol in mapping and exchange in mapping[symbol]:
            continue
        
        if exchange in {"NSE", "BSE"}:
            exch_segment = f"{exchange}_EQ"
        elif exchange in {"NFO", "BFO"}:
            exch_segment = f"{'NSE' if exchange == 'NFO' else 'BSE'}_FNO"
        else:
            exch_segment = exchange
        
        entry = {
            "dhan": {
                "security_id": dhan_info["security_id"],
                "exchange_segment": exch_segment,
                "lot_size": dhan_info.get("lot_size", "1")
            }
        }
        mapping.setdefault(symbol, {})[exchange] = entry
    
    return mapping


# Public symbol map
SYMBOL_MAP: Dict[str, Dict[str, Dict[str, Dict[str, str]]]] = {}

_REFRESH_LOCK = threading.Lock()
_LAST_REFRESH = 0.0
_MIN_REFRESH_INTERVAL = 60.0


def _ensure_symbol_map() -> Dict[str, Dict[str, Dict[str, Dict[str, str]]]]:
    """Return the global symbol map, loading it if necessary."""
    global SYMBOL_MAP, _LAST_REFRESH
    if not SYMBOL_MAP:
        SYMBOL_MAP = build_symbol_map()
        _LAST_REFRESH = time.time()
    return SYMBOL_MAP


def get_symbol_map() -> Dict[str, Dict[str, Dict[str, Dict[str, str]]]]:
    """Public wrapper to obtain the global symbol map."""
    return _ensure_symbol_map()


def refresh_symbol_map(force: bool = False) -> None:
    """Reload the global symbol map from upstream sources."""
    global SYMBOL_MAP, _LAST_REFRESH
    with _REFRESH_LOCK:
        if not force and SYMBOL_MAP and time.time() - _LAST_REFRESH < _MIN_REFRESH_INTERVAL:
            return
        
        try:
            _load_zerodha.cache_clear()
            _load_dhan.cache_clear()
            new_map = build_symbol_map()
        except requests.RequestException as exc:
            log.warning("Failed to refresh symbol map: %s", exc)
            return
        
        SYMBOL_MAP = new_map
        _LAST_REFRESH = time.time()


def get_symbol_for_broker(
    symbol: str, broker: str, exchange: str | None = None
) -> Dict[str, str]:
    """Return the mapping for *symbol* for the given *broker* including lot size.
    
    The returned dict will include a 'lot_size' key with the lot size from
    the broker's scrip master.
    """
    symbol = symbol.upper()
    broker = broker.lower()
    
    exchange_hint = None
    if exchange:
        exchange_hint = exchange.upper()
    elif ":" in symbol:
        exchange_hint, symbol = symbol.split(":", 1)
    
    # Handle symbol variants
    base = symbol
    if base.endswith("-EQ"):
        base = base[:-3]
    
    base_variants = [symbol, base]
    
    # For derivatives, try multiple variants
    if symbol.endswith(("FUT", "CE", "PE")):
        import re
        # Try to extract root symbol
        root_match = re.match(r"^([A-Z0-9]+?)(\d{2}(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC))", symbol)
        if root_match:
            root = root_match.group(1)
            base_variants.append(root)
    
    base2 = base.split("-")[0].split("_")[0]
    if base2 not in base_variants:
        base_variants.append(base2)

    # Include variants with spaces/hyphens for raw Dhan symbols
    if symbol.endswith(("FUT", "CE", "PE")):
        import re
        # Option format: ROOT + DD + MON + STRIKE + CE/PE
        opt_match = re.match(
            r"^([A-Z0-9]+?)(\d{1,2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d+)(CE|PE)$",
            base,
        )
        if opt_match:
            root, day, month, strike, opt = opt_match.groups()
            for v in (
                f"{root} {day} {month} {strike} {opt}",
                f"{root}-{day}-{month}-{strike}-{opt}",
            ):
                if v not in base_variants:
                    base_variants.append(v)
        else:
            # Future format: ROOT + DD + MON + YY + FUT
            fut_match = re.match(
                r"^([A-Z0-9]+?)(\d{1,2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d{2})(FUT)$",
                base,
            )
            if fut_match:
                root, day, month, year, fut = fut_match.groups()
                for v in (
                    f"{root} {day} {month} {year} {fut}",
                    f"{root}-{day}-{month}-{year}-{fut}",
                ):
                    if v not in base_variants:
                        base_variants.append(v)
                        
    symbol_map = _ensure_symbol_map()
    mapping = None
    
    # Try each variant
    for variant in base_variants:
        mapping = symbol_map.get(variant)
        if mapping:
            log.debug(f"Found symbol mapping for variant: {variant}")
            break
    
    # Refresh once if not found
    if not mapping:
        log.info(f"Symbol not found, refreshing symbol map for: {symbol}")
        refresh_symbol_map()
        symbol_map = _ensure_symbol_map()
        
        for variant in base_variants:
            mapping = symbol_map.get(variant)
            if mapping:
                log.info(f"Found symbol mapping after refresh for variant: {variant}")
                break
    
    if not mapping:
        log.warning(f"No symbol mapping found for {symbol} (tried variants: {base_variants})")
        return {}
    
    # Select the appropriate exchange
    if exchange_hint and exchange_hint in mapping:
        exchange_map = mapping[exchange_hint]
        log.debug(f"Using specified exchange: {exchange_hint}")
    else:
        # Prefer NFO for derivatives, NSE for equities
        if symbol.endswith(("FUT", "CE", "PE")) and "NFO" in mapping:
            exchange_map = mapping["NFO"]
            log.debug("Using NFO for derivative symbol")
        elif "NSE" in mapping:
            exchange_map = mapping["NSE"]
            log.debug("Using NSE exchange")
        else:
            exchange_map = next(iter(mapping.values()))
            log.debug(f"Using first available exchange: {list(mapping.keys())[0]}")
    
    broker_data = exchange_map.get(broker, {}) if exchange_map else {}
    
    if not broker_data:
        log.warning(f"No broker data found for {broker} with symbol {symbol}")
        if exchange_map:
            log.debug(f"Available brokers for {symbol}: {list(exchange_map.keys())}")
    else:
        # Log the lot size if found
        if "lot_size" in broker_data:
            log.debug(f"Found lot size {broker_data['lot_size']} for {symbol} on {broker}")
    
    return broker_data


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


def debug_symbol_lookup(symbol: str, broker: str = "dhan", exchange: str | None = None) -> Dict[str, any]:
    """Debug helper to show symbol lookup process and available data."""
    result = {
        "original_symbol": symbol,
        "broker": broker,
        "exchange": exchange,
        "found_mapping": False,
        "available_variants": [],
        "available_exchanges": [],
        "available_brokers": [],
        "broker_data": {},
        "lot_size": None,
        "debug_info": []
    }
    
    symbol = symbol.upper()
    broker = broker.lower()
    
    # Generate variants
    base = symbol
    if base.endswith("-EQ"):
        base = base[:-3]
    
    base_variants = [symbol, base]
    
    if symbol.endswith(("FUT", "CE", "PE")):
        import re
        root_match = re.match(r"^([A-Z0-9]+?)(\d{2}(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC))", symbol)
        if root_match:
            root = root_match.group(1)
            base_variants.append(root)
    
    base2 = base.split("-")[0].split("_")[0]
    if base2 not in base_variants:
        base_variants.append(base2)
    
    result["available_variants"] = base_variants
    
    symbol_map = _ensure_symbol_map()
    
    # Check each variant
    for variant in base_variants:
        if variant in symbol_map:
            result["found_mapping"] = True
            result["available_exchanges"] = list(symbol_map[variant].keys())
            
            # Check available brokers and lot sizes
            for exch, exch_data in symbol_map[variant].items():
                for broker_name, broker_info in exch_data.items():
                    if broker_name not in result["available_brokers"]:
                        result["available_brokers"].append(broker_name)
                    # Log lot size info
                    if broker_name == broker and "lot_size" in broker_info:
                        result["debug_info"].append(
                            f"Lot size for {broker} on {exch}: {broker_info['lot_size']}"
                        )
            
            result["debug_info"].append(f"Found variant '{variant}' in symbol map")
            
            # Get broker data
            if exchange and exchange.upper() in symbol_map[variant]:
                exchange_map = symbol_map[variant][exchange.upper()]
            elif "NFO" in symbol_map[variant] and symbol.endswith(("FUT", "CE", "PE")):
                exchange_map = symbol_map[variant]["NFO"]
                result["debug_info"].append("Selected NFO for derivative")
            elif "NSE" in symbol_map[variant]:
                exchange_map = symbol_map[variant]["NSE"]
                result["debug_info"].append("Selected NSE as default")
            else:
                exchange_map = next(iter(symbol_map[variant].values()))
                result["debug_info"].append(f"Selected first available exchange: {list(symbol_map[variant].keys())[0]}")
            
            result["broker_data"] = exchange_map.get(broker, {})
            if result["broker_data"]:
                result["lot_size"] = result["broker_data"].get("lot_size")
                result["debug_info"].append(f"Found broker data for {broker}, lot_size: {result['lot_size']}")
            else:
                result["debug_info"].append(f"No broker data found for {broker}")
            
            break
        else:
            result["debug_info"].append(f"Variant '{variant}' not found in symbol map")
    
    return result


__all__ = [
    "SYMBOL_MAP",
    "build_symbol_map",
    "refresh_symbol_map",
    "get_symbol_map",
    "get_symbol_for_broker",
    "get_symbol_by_token",
    "debug_symbol_lookup"
]
