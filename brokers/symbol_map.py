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


def _fetch_csv(url: str, cache_name: str) -> str:
    """Return CSV content from *url* using a small on-disk cache."""
    cache_file = CACHE_DIR / cache_name
    
    # Try to use cache if it's fresh
    if cache_file.exists():
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
            if cache_file.exists():
                return cache_file.read_text()
            time.sleep(2 ** attempt)
            continue
    raise requests.RequestException(f"failed to fetch {url}")

Key = Tuple[str, str]
SYMBOLS_KEY = "_symbols"

def extract_root_symbol(symbol: str) -> str:
    """Extract root symbol from derivative symbols.
    
    Examples:
    - NIFTYNXT50-Sep2025-FUT -> NIFTYNXT50
    - FINNIFTY-Sep2025-33300-CE -> FINNIFTY  
    - RELIANCE-EQ -> RELIANCE
    """
    if not symbol:
        return symbol
    
    # Remove common suffixes and extract root
    cleaned = symbol.upper()
    
    # Handle Dhan format with hyphens
    if '-' in cleaned:
        parts = cleaned.split('-')
        return parts[0]  # First part is always the root
    
    # Handle compact format without hyphens
    # Remove trailing FUT, CE, PE
    for suffix in ['FUT', 'CE', 'PE']:
        if cleaned.endswith(suffix):
            cleaned = cleaned[:-len(suffix)]
            break
    
    # Remove -EQ suffix
    if cleaned.endswith('EQ'):
        cleaned = cleaned[:-2]
    
    # Remove month and year patterns
    cleaned = re.sub(r'\d{2}(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)', '', cleaned)
    
    # Remove strike prices (numbers at the end)
    cleaned = re.sub(r'\d+$', '', cleaned)
    
    return cleaned


def parse_fo_symbol(symbol: str, broker: str) -> dict:
    """Parse F&O symbol into components based on broker format."""
    if not symbol:
        return None
    
    symbol = symbol.upper().strip()
    broker = broker.lower()
    
    if broker == 'dhan':
        # NIFTY-Dec2024-24000-CE or NIFTY-Dec2024-FUT format
        opt_match = re.match(r'^(.+?)-(\w{3})(\d{4})-(\d+)-(CE|PE)$', symbol)
        if opt_match:
            return {
                'underlying': opt_match.group(1),
                'month': opt_match.group(2),
                'year': opt_match.group(3),
                'strike': opt_match.group(4),
                'option_type': opt_match.group(5),
                'instrument': 'OPT'
            }
        
        fut_match = re.match(r'^(.+?)-(\w{3})(\d{4})-FUT$', symbol)
        if fut_match:
            return {
                'underlying': fut_match.group(1),
                'month': fut_match.group(2),
                'year': fut_match.group(3),
                'instrument': 'FUT'
            }
    
    elif broker in ['zerodha', 'aliceblue', 'fyers', 'finvasia']:
        # NIFTY24DEC24000CE format
        opt_match = re.match(r'^(.+?)(\d{2})(\w{3})(\d+)(CE|PE)$', symbol)
        if opt_match:
            return {
                'underlying': opt_match.group(1),
                'year': '20' + opt_match.group(2),
                'month': opt_match.group(3),
                'strike': opt_match.group(4),
                'option_type': opt_match.group(5),
                'instrument': 'OPT'
            }
        
        # NIFTY24DECFUT format
        fut_match = re.match(r'^(.+?)(\d{2})(\w{3})FUT$', symbol)
        if fut_match:
            return {
                'underlying': fut_match.group(1),
                'year': '20' + fut_match.group(2),
                'month': fut_match.group(3),
                'instrument': 'FUT'
            }
    
    return None


def format_fo_symbol(components: dict, to_broker: str) -> str:
    """Format symbol components for target broker."""
    if not components:
        return None
    
    to_broker = to_broker.lower()
    
    if to_broker == 'dhan':
        if components['instrument'] == 'OPT':
            return f"{components['underlying']}-{components['month']}{components['year']}-{components['strike']}-{components['option_type']}"
        elif components['instrument'] == 'FUT':
            return f"{components['underlying']}-{components['month']}{components['year']}-FUT"
    
    elif to_broker in ['zerodha', 'aliceblue', 'fyers', 'finvasia']:
        year_short = components['year'][-2:]  # Get last 2 digits
        if components['instrument'] == 'OPT':
            return f"{components['underlying']}{year_short}{components['month'].upper()}{components['strike']}{components['option_type']}"
        elif components['instrument'] == 'FUT':
            return f"{components['underlying']}{year_short}{components['month'].upper()}FUT"
    
    return None


def convert_symbol_between_brokers(symbol: str, from_broker: str, to_broker: str, instrument_type: str = None) -> str:
    """Convert F&O symbol from one broker format to another."""
    if not symbol or from_broker.lower() == to_broker.lower():
        return symbol
    
    # First, parse the symbol to extract components
    components = parse_fo_symbol(symbol, from_broker)
    
    if not components:
        log.debug(f"Could not parse F&O symbol: {symbol} for broker: {from_broker}")
        return symbol  # Return original if can't parse
    
    # Convert to target broker format
    converted = format_fo_symbol(components, to_broker)
    
    if converted:
        log.info(f"Converted F&O symbol from {symbol} ({from_broker}) to {converted} ({to_broker})")
        return converted
    
    log.warning(f"Could not convert symbol {symbol} from {from_broker} to {to_broker}")
    return symbol


@lru_cache(maxsize=1)
def _load_zerodha() -> Dict[Key, Dict[str, str]]:
    """Return mapping of (symbol, exchange) to instrument data including lot size."""
    csv_text = _fetch_csv(ZERODHA_URL, "zerodha_instruments.csv")
    
    reader = csv.DictReader(StringIO(csv_text))
    data: Dict[Key, Dict[str, str]] = {}
    
    for row in reader:
        segment = row["segment"].upper()
        inst_type = row["instrument_type"].upper()
        
        if segment in {"NSE", "BSE"} and inst_type == "EQ":
            key = (row["tradingsymbol"], row["exchange"].upper())
            data[key] = {
                "instrument_token": row["instrument_token"],
                "lot_size": row.get("lot_size", "1")  # Equity lot size is usually 1
            }
        elif segment in {"NFO", "BFO"} and inst_type in {
            "FUT", "FUTSTK", "FUTIDX", "OPT", "OPTSTK", "OPTIDX", "CE", "PE"
        }:
            key = (row["tradingsymbol"], segment)
            lot_size = row.get("lot_size", "1")
            # Ensure lot size is valid
            if not lot_size or lot_size == "" or lot_size == "0":
                lot_size = "1"
            data[key] = {
                "instrument_token": row["instrument_token"],
                "lot_size": lot_size
            }
    
    return data


@lru_cache(maxsize=1)
def _load_dhan() -> Dict[Key, Dict[str, str]]:
    """Return mapping of (symbol, exchange) to Dhan security data including lot size."""
    csv_text = _fetch_csv(DHAN_URL, "dhan_scrip_master.csv")
    
    reader = csv.DictReader(StringIO(csv_text))
    data: Dict[Key, Dict[str, str]] = {}
    
    for row in reader:
        exch = row["SEM_EXM_EXCH_ID"].upper()
        segment = row["SEM_SEGMENT"].upper()
        symbol = row["SEM_TRADING_SYMBOL"]
        
        # Get lot size from SEM_LOT_UNITS column
        lot_size = row.get("SEM_LOT_UNITS", "1")
        if not lot_size or lot_size == "" or lot_size == "0":
            lot_size = "1"
        
        if exch in {"NSE", "BSE"} and segment == "E":
            key = (symbol, exch)
            # Prefer EQ series when available
            if key not in data or row["SEM_SERIES"] == "EQ":
                data[key] = {
                    "security_id": row["SEM_SMST_SECURITY_ID"],
                    "lot_size": lot_size
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
    zerodha_data = _load_zerodha()
    dhan_data = _load_dhan()
    
    mapping: Dict[str, Dict[str, Dict[str, Dict[str, str]]]] = {}
    
    # Build map with Zerodha as base
    for (symbol, exchange), zdata in zerodha_data.items():
        token = zdata.get("instrument_token", "")
        lot_size = zdata.get("lot_size", "1")
        
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
        dhan_info = dhan_data.get((symbol, exchange))
        if dhan_info:
            if exchange in {"NSE", "BSE"}:
                exch_segment = f"{exchange}_EQ"
            elif exchange in {"NFO", "BFO"}:
                exch_segment = f"{'NSE' if exchange == 'NFO' else 'BSE'}_FNO"
            else:
                exch_segment = exchange
            
            # Use Dhan's lot size if available, otherwise fallback to Zerodha
            dhan_lot_size = dhan_info.get("lot_size", lot_size)
            
            entry["dhan"] = {
                "security_id": dhan_info["security_id"],
                "exchange_segment": exch_segment,
                "lot_size": dhan_lot_size,
                "trading_symbol": symbol
            }
            
            # Update all other brokers with Dhan's lot size if it's different and valid
            if dhan_lot_size and dhan_lot_size != lot_size and dhan_lot_size != "1":
                log.debug(f"Using Dhan lot size {dhan_lot_size} over Zerodha {lot_size} for {symbol}")
                for broker_data in entry.values():
                    if broker_data != entry["dhan"]:
                        broker_data["lot_size"] = dhan_lot_size
        
        # Extract root symbol for indexing
        root_symbol = extract_root_symbol(symbol)
        root_entry = mapping.setdefault(root_symbol, {})
        root_entry[exchange] = entry
        symbols = root_entry.setdefault(SYMBOLS_KEY, {})
        symbols.setdefault(exchange, {})[symbol] = entry
    
    # Add Dhan-only symbols (not in Zerodha)
    for (symbol, exchange), dhan_info in dhan_data.items():
        root_symbol = extract_root_symbol(symbol)
        if root_symbol in mapping and exchange in mapping[root_symbol]:
            continue
        
        if exchange in {"NSE", "BSE"}:
            exch_segment = f"{exchange}_EQ"
        elif exchange in {"NFO", "BFO"}:
            exch_segment = f"{'NSE' if exchange == 'NFO' else 'BSE'}_FNO"
        else:
            exch_segment = exchange
        
        lot_size = dhan_info.get("lot_size", "1")
        
        entry = {
            "dhan": {
                "security_id": dhan_info["security_id"],
                "exchange_segment": exch_segment,
                "lot_size": lot_size,
                "trading_symbol": symbol  # Store original Dhan symbol
            }
        }
        root_entry = mapping.setdefault(root_symbol, {})
        root_entry[exchange] = entry
        symbols = root_entry.setdefault(SYMBOLS_KEY, {})
        symbols.setdefault(exchange, {})[symbol] = entry
    
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


def _direct_symbol_lookup(symbol: str, broker: str, exchange: str | None = None) -> Dict[str, str]:
    """Direct symbol lookup without conversion."""
    if symbol is None:
        symbol = ""

    broker = broker.lower()

    # Preserve the original symbol for dictionary lookup while using an
    # uppercased variant for canonical comparisons.
    lookup_symbol = symbol.strip()
    
    exchange_hint = None
    if exchange:
        exchange_hint = exchange.upper()
    elif ":" in lookup_symbol:
        exchange_hint, lookup_symbol = lookup_symbol.split(":", 1)

    lookup_symbol = lookup_symbol.strip()
    upper_symbol = lookup_symbol.upper()

    # Extract root symbol for lookup
    root_symbol = extract_root_symbol(lookup_symbol)

    symbol_map = _ensure_symbol_map()
    mapping = symbol_map.get(root_symbol)
    
    if not mapping:
        return {}
    
    # Select the appropriate exchange
    exchange_map = None
    exchange_name = None
    if exchange_hint and exchange_hint in mapping:
        exchange_name = exchange_hint    
        exchange_map = mapping[exchange_hint]
        log.debug(f"Using specified exchange: {exchange_hint}")
    else:
        if upper_symbol.endswith(("FUT", "CE", "PE")):
            if "NFO" in mapping:
                exchange_name = "NFO"
                exchange_map = mapping["NFO"]
                log.debug("Using NFO for derivative symbol")
            elif "BFO" in mapping:
                exchange_name = "BFO"
                exchange_map = mapping["BFO"]
                log.debug("Using BFO for derivative symbol")

        if exchange_map is None:
            if "NSE" in mapping:
                exchange_name = "NSE"
                exchange_map = mapping["NSE"]
                log.debug("Using NSE exchange")
            elif "BSE" in mapping:
                exchange_name = "BSE"
                exchange_map = mapping["BSE"]
                log.debug("Using BSE exchange")
            else:
                for key in mapping:
                    if key == SYMBOLS_KEY:
                        continue
                    exchange_name = key
                    exchange_map = mapping[key]
                    log.debug(f"Using first available exchange: {key}")
                    break

    entry = None
    if exchange_name:
        symbols = mapping.get(SYMBOLS_KEY, {}).get(exchange_name, {})
        entry = symbols.get(lookup_symbol)
        if entry is None and lookup_symbol != upper_symbol:
            entry = symbols.get(upper_symbol)

    if entry is None:
        entry = exchange_map

    broker_data = entry.get(broker, {}) if entry else {}
    
    # Log lot size for debugging
    if broker_data and "lot_size" in broker_data:
        log.debug(
            "Found lot size %s for %s on %s",
            broker_data["lot_size"],
            lookup_symbol,
            broker,
        )
    
    return broker_data


def get_symbol_for_broker(
    symbol: str, broker: str, exchange: str | None = None
) -> Dict[str, str]:
    """Return the mapping for *symbol* for the given *broker* including lot size.

    Enhanced to handle F&O symbol conversion between different broker formats.
    """
    if symbol is None:
        symbol = ""

    original_symbol = symbol.strip()
    broker = broker.lower()

    exchange_hint = None
    if exchange:
        exchange_hint = exchange.upper()
    elif ":" in original_symbol:
        exchange_hint, original_symbol = original_symbol.split(":", 1)

    original_symbol = original_symbol.strip()
    upper_symbol = original_symbol.upper()

    # For F&O symbols, try different conversion approaches
    if re.search(r'(FUT|CE|PE)$', upper_symbol):
        # Try direct lookup first
        result = _direct_symbol_lookup(original_symbol, broker, exchange_hint)
        if result and "lot_size" in result:
            log.debug(
                "Direct lookup found lot size %s for %s",
                result["lot_size"],
                original_symbol,
            )
            return result

        # Try parsing and converting from different broker formats
        for source_broker in ['dhan', 'zerodha', 'aliceblue', 'fyers', 'finvasia']:
            if source_broker == broker:
                continue

            components = parse_fo_symbol(original_symbol, source_broker)
            if components:
                converted_symbol = format_fo_symbol(components, broker)
                if converted_symbol:
                    result = _direct_symbol_lookup(converted_symbol, broker, exchange_hint)
                    if result and "lot_size" in result:
                        log.info(
                            "Found F&O mapping via conversion: %s -> %s, lot size: %s",
                            original_symbol,
                            converted_symbol,
                            result["lot_size"],
                        )
                        return result

        # Try with root symbol lookup for F&O
        root_symbol = extract_root_symbol(original_symbol)
        symbol_map = _ensure_symbol_map()
        mapping = symbol_map.get(root_symbol)

        if mapping:
            # Select appropriate exchange
            if exchange_hint and exchange_hint in mapping:
                exchange_name = exchange_hint
                exchange_map = mapping[exchange_hint]
            elif "NFO" in mapping:
                exchange_name = "NFO"
                exchange_map = mapping["NFO"]
            elif "NSE" in mapping:
                exchange_name = "NSE"
                exchange_map = mapping["NSE"]
            else:
                exchange_name = None
                for key in mapping:
                    if key == SYMBOLS_KEY:
                        continue
                    exchange_name = key
                    break
                exchange_map = mapping.get(exchange_name) if exchange_name else None

            entry = None
            if exchange_name:
                symbols = mapping.get(SYMBOLS_KEY, {}).get(exchange_name, {})
                entry = symbols.get(original_symbol)
                if entry is None and original_symbol != upper_symbol:
                    entry = symbols.get(upper_symbol)
            if entry is None and exchange_map is not None:
                entry = exchange_map

            broker_data = entry.get(broker, {}) if entry else {}
            if broker_data and "lot_size" in broker_data:
                # Try to convert the symbol to the broker's format
                if broker == 'dhan' and 'trading_symbol' in broker_data:
                    # Convert from other format to Dhan format
                    dhan_symbol = broker_data.get('trading_symbol', original_symbol)
                    if dhan_symbol != original_symbol:
                        log.info(
                            "Using Dhan symbol mapping: %s -> %s, lot size: %s",
                            original_symbol,
                            dhan_symbol,
                            broker_data['lot_size'],
                        )
                        broker_data = dict(broker_data)
                        broker_data['symbol'] = dhan_symbol
                        broker_data['trading_symbol'] = dhan_symbol
                return broker_data

    # Fall back to original logic for equity and unmatched F&O
    return _direct_symbol_lookup(original_symbol, broker, exchange_hint)


def get_symbol_by_token(token: str, broker: str) -> str | None:
    """Return the base symbol for a broker given its token/instrument id."""
    broker = broker.lower()
    token = str(token)
    
    symbol_map = _ensure_symbol_map()
    for sym, exchanges in symbol_map.items():
        per_symbol = exchanges.get(SYMBOLS_KEY, {})
        for symbol_entries in per_symbol.values():
            for entry in symbol_entries.values():
                broker_map = entry.get(broker)
                if broker_map and str(broker_map.get("token")) == token:
                    return sym

        for exchange, entry in exchanges.items():
            if exchange == SYMBOLS_KEY:
                continue
            broker_map = entry.get(broker)
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
    
    # Extract root and generate variants
    root_symbol = extract_root_symbol(symbol)
    base_variants = [root_symbol]
    
    if symbol != root_symbol:
        base_variants.append(symbol)
    
    if symbol.endswith(("FUT", "CE", "PE")):
        clean_variant = re.sub(r'\d{2}(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)', '', symbol)
        if clean_variant not in base_variants:
            base_variants.append(clean_variant)
    
    result["available_variants"] = base_variants
    
    symbol_map = _ensure_symbol_map()
    
    # Check each variant
    for variant in base_variants:
        if variant in symbol_map:
            result["found_mapping"] = True
            variant_map = symbol_map[variant]
            exchanges = [key for key in variant_map.keys() if key != SYMBOLS_KEY]
            result["available_exchanges"] = exchanges
            
            # Check available brokers and lot sizes
            for exch, exch_data in variant_map.items():
                if exch == SYMBOLS_KEY:
                    for symbol_entries in exch_data.values():
                        for broker_name, broker_info in symbol_entries.items():
                            if broker_name not in result["available_brokers"]:
                                result["available_brokers"].append(broker_name)
                            if broker_name == broker and "lot_size" in broker_info:
                                result["debug_info"].append(
                                    f"Lot size for {broker} on {exch}: {broker_info['lot_size']}"
                                )
                    continue

                for broker_name, broker_info in exch_data.items():
                    if broker_name not in result["available_brokers"]:
                        result["available_brokers"].append(broker_name)
                    if broker_name == broker and "lot_size" in broker_info:
                        result["debug_info"].append(
                            f"Lot size for {broker} on {exch}: {broker_info['lot_size']}"
                        )
            
            result["debug_info"].append(f"Found variant '{variant}' in symbol map")
            
            # Get broker data
            exchange_map = None
            if exchange and exchange.upper() in variant_map:
                exchange_name = exchange.upper()
                exchange_map = variant_map[exchange_name]
            elif "NFO" in variant_map and symbol.endswith(("FUT", "CE", "PE")):
                exchange_name = "NFO"
                exchange_map = variant_map[exchange_name]
                result["debug_info"].append("Selected NFO for derivative")
            elif "NSE" in variant_map:
                exchange_name = "NSE"
                exchange_map = variant_map[exchange_name]
                result["debug_info"].append("Selected NSE as default")
            else:
                exchange_name = exchanges[0] if exchanges else None
                exchange_map = (
                    variant_map[exchange_name]
                    if exchange_name
                    else None
                )
                if exchange_name:
                    result["debug_info"].append(
                        f"Selected first available exchange: {exchange_name}"
                    )

            entry = None
            if exchange_name:
                entry = variant_map.get(SYMBOLS_KEY, {}).get(exchange_name, {}).get(symbol)
            if entry is None and exchange_map is not None:
                entry = exchange_map

            result["broker_data"] = entry.get(broker, {}) if entry else {}
            if result["broker_data"]:
                result["lot_size"] = result["broker_data"].get("lot_size")
                result["debug_info"].append(
                    f"Found broker data for {broker}, lot_size: {result['lot_size']}"
                )
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
    "debug_symbol_lookup",
    "extract_root_symbol",
    "parse_fo_symbol",
    "format_fo_symbol",
    "convert_symbol_between_brokers",
]
