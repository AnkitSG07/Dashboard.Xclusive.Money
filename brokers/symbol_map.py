"""Dynamic symbol map for Indian equities with lot size support.

This module builds a mapping of every equity symbol available on the
Indian stock exchanges (NSE and BSE) across a number of supported
brokers, including lot size information for derivatives.
"""

from __future__ import annotations

import csv
import os
import time
from datetime import datetime
from functools import lru_cache
from io import StringIO
from pathlib import Path
from typing import Dict, Tuple
import logging
import threading
import requests
import re
from contextlib import suppress

from services.fo_symbol_utils import (
    format_dhan_future_symbol,
    format_dhan_option_symbol,
)

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


def _find_symbol_in_zerodha_csv(token: str) -> str | None:
    """Return the base symbol for *token* by scanning the Zerodha CSV."""

    cache_file = _ensure_cached_csv(ZERODHA_URL, "zerodha_instruments.csv")

    with cache_file.open(newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            row_token = (row.get("instrument_token") or "").strip()
            if row_token != token:
                continue

            trading_symbol = (row.get("tradingsymbol") or "").strip()
            if not trading_symbol:
                return None

            base_symbol = extract_root_symbol(trading_symbol)
            if base_symbol:
                return base_symbol
            return None

    return None


def _find_symbol_in_dhan_csv(security_id: str) -> str | None:
    """Return the base symbol for *security_id* by scanning the Dhan CSV."""

    cache_file = _ensure_cached_csv(DHAN_URL, "dhan_scrip_master.csv")

    with cache_file.open(newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            row_id = (row.get("SEM_SMST_SECURITY_ID") or "").strip()
            if row_id != security_id:
                continue

            trading_symbol = (row.get("SEM_TRADING_SYMBOL") or "").strip()
            if not trading_symbol:
                trading_symbol = (row.get("SEM_CUSTOM_SYMBOL") or "").strip()

            if not trading_symbol:
                return None

            base_symbol = extract_root_symbol(trading_symbol)
            if base_symbol:
                return base_symbol
            return None

    return None


def _lookup_symbol_by_token_cached_csv(token: str, broker: str) -> str | None:
    """Return base symbol by scanning cached CSV data for *broker*."""

    broker = broker.lower()
    if broker == "zerodha":
        return _find_symbol_in_zerodha_csv(token)
    if broker == "dhan":
        return _find_symbol_in_dhan_csv(token)
    return None


@lru_cache(maxsize=256)
def _cached_token_lookup(token: str, broker: str) -> str | None:
    """LRU cached token lookup to avoid repeatedly scanning CSV files."""

    return _lookup_symbol_by_token_cached_csv(token, broker)


def _ensure_cached_csv(url: str, cache_name: str) -> Path:
    """Ensure a cached copy of *url* exists and return the path."""
    cache_file = CACHE_DIR / cache_name
    
    # Try to use cache if it's fresh
    if cache_file.exists():
        age = time.time() - cache_file.stat().st_mtime
        if age < CACHE_MAX_AGE:
            return cache_file

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
            return cache_file
        except requests.RequestException:
            if cache_file.exists():
                return cache_file
            time.sleep(2 ** attempt)
            continue
    raise requests.RequestException(f"failed to fetch {url}")


def _fetch_csv(url: str, cache_name: str) -> str:
    """Return CSV content from *url* using a small on-disk cache."""
    cache_file = _ensure_cached_csv(url, cache_name)
    return cache_file.read_text()


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
    if "-" in cleaned:
        parts = cleaned.split("-")
        return parts[0]  # First part is always the root
    
    # Remove trailing FUT when explicitly present
    if cleaned.endswith("FUT"):
        cleaned = cleaned[:-3]

    # Remove trailing CE/PE only when it looks like an option contract
    if cleaned.endswith(("CE", "PE")):
        base = cleaned[:-2]
        # Require obvious derivative markers (digits or explicit separators)
        if re.search(r"\d", base) or "-" in base:
            cleaned = base
            
    # Remove -EQ suffix
    if cleaned.endswith("EQ"):
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
        symbol = row["SEM_TRADING_SYMBOL"].strip()
        series = row.get("SEM_SERIES", "").upper()
        
        # Get lot size from SEM_LOT_UNITS column
        lot_size = row.get("SEM_LOT_UNITS", "1")
        if not lot_size or lot_size == "" or lot_size == "0":
            lot_size = "1"
        
        if exch in {"NSE", "BSE"} and segment == "E":
            keys = {(symbol, exch)}
            if symbol.upper().endswith("-EQ"):
                stripped = symbol[:-3].strip()
                if stripped:
                    keys.add((stripped, exch))

            # Prefer EQ series when available
            for key in keys:
                if key not in data or series == "EQ":
                    data[key] = {
                        "security_id": row["SEM_SMST_SECURITY_ID"],
                        "lot_size": lot_size,
                        "trading_symbol": symbol
                    }
        elif exch in {"NSE", "BSE"} and segment == "D":
            # Derivatives segment
            exchange_segment = "NFO" if exch == "NSE" else "BFO"
            expiry_flag = row.get("SEM_EXPIRY_FLAG", "").strip().upper()
            expiry_date_str = row.get("SEM_EXPIRY_DATE", "").strip()
            custom_symbol = row.get("SEM_CUSTOM_SYMBOL", "").strip()

            expiry_dt: datetime | None = None
            if expiry_date_str:
                cleaned_expiry = expiry_date_str.replace("Z", "").strip()
                try:
                    expiry_dt = datetime.fromisoformat(cleaned_expiry)
                except ValueError:
                    for fmt in (
                        "%Y-%m-%d %H:%M:%S",
                        "%Y-%m-%d",
                        "%d-%b-%Y %H:%M:%S",
                        "%d-%b-%Y",
                    ):
                        try:
                            expiry_dt = datetime.strptime(cleaned_expiry, fmt)
                            break
                        except ValueError:
                            continue

            alias_symbol = None
            alias_day: int | None = None
            if expiry_dt:
                alias_day = expiry_dt.day
                alias_month = expiry_dt.strftime("%b")
                alias_year = expiry_dt.strftime("%Y")

                parts = symbol.split("-")
                if len(parts) >= 3:
                    underlying = parts[0]
                    if len(parts) == 3 and parts[2].upper() == "FUT":
                        alias_symbol = format_dhan_future_symbol(
                            underlying,
                            alias_month,
                            alias_year,
                            day=alias_day,
                        )
                    elif len(parts) >= 4:
                        strike = parts[-2]
                        opt_type = parts[-1].upper()
                        alias_symbol = format_dhan_option_symbol(
                            underlying,
                            alias_month,
                            alias_year,
                            strike,
                            opt_type,
                            day=alias_day,
                        )

            if (
                alias_symbol is None
                and custom_symbol
                and expiry_flag
                and expiry_flag != "M"
            ):
                # Fallback for unexpected trading symbol formats using the
                # custom symbol field (e.g. "NIFTY 12 SEP 25500 CALL").
                custom_match = re.match(
                    r"^(?P<underlying>.+?)\s+(?P<day>\d{1,2})\s+(?P<month>[A-Z]{3})\s+(?P<strike>[\d.]+)\s+(?P<option>CALL|PUT|CE|PE|FUT)$",
                    custom_symbol.upper(),
                )
                if custom_match:
                    alias_day = int(custom_match.group("day"))
                    alias_month = custom_match.group("month").title()
                    alias_year = expiry_dt.strftime("%Y") if expiry_dt else row.get("SEM_EXPIRY_DATE", "")[:4]
                    underlying = symbol.split("-")[0] or custom_match.group("underlying")
                    option = custom_match.group("option")
                    strike = custom_match.group("strike")
                    if option == "FUT":
                        alias_symbol = format_dhan_future_symbol(
                            underlying,
                            alias_month,
                            alias_year,
                            day=alias_day,
                        )
                    else:
                        opt_code = "CE" if option in {"CALL", "CE"} else "PE"
                        alias_symbol = format_dhan_option_symbol(
                            underlying,
                            alias_month,
                            alias_year,
                            strike,
                            opt_code,
                            day=alias_day,
                        )

            entry = {
                "security_id": row["SEM_SMST_SECURITY_ID"],
                "lot_size": lot_size,
                "trading_symbol": symbol,
            }

            if expiry_flag:
                entry["expiry_flag"] = expiry_flag
            if expiry_dt:
                entry["expiry_date"] = expiry_dt.strftime("%Y-%m-%d")
                entry["expiry_day"] = expiry_dt.day

            if (
                alias_symbol
                and expiry_flag
                and expiry_flag != "M"
                and alias_symbol != symbol
            ):
                aliases = entry.setdefault("aliases", [])
                if alias_symbol not in aliases:
                    aliases.append(alias_symbol)

            key = (symbol, exchange_segment)
            if key not in data or expiry_flag == "M":
                data[key] = entry

            if entry.get("aliases"):
                for alias in entry["aliases"]:
                    data[(alias, exchange_segment)] = entry
    
    return data


def _load_zerodha_slice(
    symbol: str, exchange: str | None = None
) -> Dict[Key, Dict[str, str]]:
    """Return Zerodha entries for a specific *symbol*."""

    cache_file = _ensure_cached_csv(ZERODHA_URL, "zerodha_instruments.csv")
    data: Dict[Key, Dict[str, str]] = {}
    root_symbol = extract_root_symbol(symbol)
    exchange_hint = exchange.upper() if exchange else None

    with cache_file.open(newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            tradingsymbol = row.get("tradingsymbol", "").strip()
            if not tradingsymbol:
                continue

            candidate_root = extract_root_symbol(tradingsymbol)
            if candidate_root != root_symbol:
                continue

            segment = row.get("segment", "").upper()
            inst_type = row.get("instrument_type", "").upper()

            if segment in {"NSE", "BSE"} and inst_type == "EQ":
                exch = row.get("exchange", "").upper()
                if exchange_hint and exch != exchange_hint:
                    continue
                key = (tradingsymbol, exch)
            elif segment in {"NFO", "BFO"} and inst_type in {
                "FUT",
                "FUTSTK",
                "FUTIDX",
                "OPT",
                "OPTSTK",
                "OPTIDX",
                "CE",
                "PE",
            }:
                if exchange_hint and segment != exchange_hint:
                    continue
                key = (tradingsymbol, segment)
            else:
                continue

            lot_size = row.get("lot_size", "1")
            if not lot_size or lot_size in {"", "0"}:
                lot_size = "1"

            data[key] = {
                "instrument_token": row.get("instrument_token", ""),
                "lot_size": lot_size,
            }

    return data


def _load_dhan_slice(
    symbol: str, exchange: str | None = None
) -> Dict[Key, Dict[str, str]]:
    """Return Dhan entries for a specific *symbol*."""

    cache_file = _ensure_cached_csv(DHAN_URL, "dhan_scrip_master.csv")
    data: Dict[Key, Dict[str, str]] = {}
    root_symbol = extract_root_symbol(symbol)
    exchange_hint = exchange.upper() if exchange else None

    with cache_file.open(newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            exch = row.get("SEM_EXM_EXCH_ID", "").upper()
            segment = row.get("SEM_SEGMENT", "").upper()
            trading_symbol = row.get("SEM_TRADING_SYMBOL", "").strip()
            if not trading_symbol:
                continue

            candidate_root = extract_root_symbol(trading_symbol)
            if candidate_root != root_symbol:
                continue

            if exchange_hint:
                if segment == "D":
                    derived_exchange = "NFO" if exch == "NSE" else "BFO"
                else:
                    derived_exchange = exch
                if derived_exchange != exchange_hint:
                    continue

            lot_size = row.get("SEM_LOT_UNITS", "1")
            if not lot_size or lot_size in {"", "0"}:
                lot_size = "1"

            if exch in {"NSE", "BSE"} and segment == "E":
                keys = {(trading_symbol, exch)}
                if trading_symbol.upper().endswith("-EQ"):
                    stripped = trading_symbol[:-3].strip()
                    if stripped:
                        keys.add((stripped, exch))

                for key in keys:
                    if key not in data or row.get("SEM_SERIES", "").upper() == "EQ":
                        data[key] = {
                            "security_id": row.get("SEM_SMST_SECURITY_ID", ""),
                            "lot_size": lot_size,
                            "trading_symbol": trading_symbol,
                        }
            elif exch in {"NSE", "BSE"} and segment == "D":
                exchange_segment = "NFO" if exch == "NSE" else "BFO"
                expiry_flag = row.get("SEM_EXPIRY_FLAG", "").strip().upper()
                expiry_date_str = row.get("SEM_EXPIRY_DATE", "").strip()
                custom_symbol = row.get("SEM_CUSTOM_SYMBOL", "").strip()

                expiry_dt: datetime | None = None
                if expiry_date_str:
                    cleaned_expiry = expiry_date_str.replace("Z", "").strip()
                    try:
                        expiry_dt = datetime.fromisoformat(cleaned_expiry)
                    except ValueError:
                        for fmt in (
                            "%Y-%m-%d %H:%M:%S",
                            "%Y-%m-%d",
                            "%d-%b-%Y %H:%M:%S",
                            "%d-%b-%Y",
                        ):
                            try:
                                expiry_dt = datetime.strptime(cleaned_expiry, fmt)
                                break
                            except ValueError:
                                continue

                alias_symbol = None
                alias_day: int | None = None
                if expiry_dt:
                    alias_day = expiry_dt.day
                    alias_month = expiry_dt.strftime("%b")
                    alias_year = expiry_dt.strftime("%Y")

                    parts = trading_symbol.split("-")
                    if len(parts) >= 3:
                        underlying = parts[0]
                        if len(parts) == 3 and parts[2].upper() == "FUT":
                            alias_symbol = format_dhan_future_symbol(
                                underlying,
                                alias_month,
                                alias_year,
                                day=alias_day,
                            )
                        elif len(parts) >= 4:
                            strike = parts[-2]
                            opt_type = parts[-1].upper()
                            alias_symbol = format_dhan_option_symbol(
                                underlying,
                                alias_month,
                                alias_year,
                                strike,
                                opt_type,
                                day=alias_day,
                            )

                if (
                    alias_symbol is None
                    and custom_symbol
                    and expiry_flag
                    and expiry_flag != "M"
                ):
                    custom_match = re.match(
                        r"^(?P<underlying>.+?)\s+(?P<day>\d{1,2})\s+(?P<month>[A-Z]{3})\s+(?P<strike>[\d.]+)\s+(?P<option>CALL|PUT|CE|PE|FUT)$",
                        custom_symbol.upper(),
                    )
                    if custom_match:
                        alias_day = int(custom_match.group("day"))
                        alias_month = custom_match.group("month").title()
                        alias_year = (
                            expiry_dt.strftime("%Y")
                            if expiry_dt
                            else row.get("SEM_EXPIRY_DATE", "")[:4]
                        )
                        underlying = (
                            trading_symbol.split("-")[0]
                            or custom_match.group("underlying")
                        )
                        option = custom_match.group("option")
                        strike = custom_match.group("strike")
                        if option == "FUT":
                            alias_symbol = format_dhan_future_symbol(
                                underlying,
                                alias_month,
                                alias_year,
                                day=alias_day,
                            )
                        else:
                            opt_code = "CE" if option in {"CALL", "CE"} else "PE"
                            alias_symbol = format_dhan_option_symbol(
                                underlying,
                                alias_month,
                                alias_year,
                                strike,
                                opt_code,
                                day=alias_day,
                            )

                entry: Dict[str, str] = {
                    "security_id": row.get("SEM_SMST_SECURITY_ID", ""),
                    "lot_size": lot_size,
                    "trading_symbol": trading_symbol,
                }

                if expiry_flag:
                    entry["expiry_flag"] = expiry_flag
                if expiry_dt:
                    entry["expiry_date"] = expiry_dt.strftime("%Y-%m-%d")
                    entry["expiry_day"] = expiry_dt.day

                if (
                    alias_symbol
                    and expiry_flag
                    and expiry_flag != "M"
                    and alias_symbol != trading_symbol
                ):
                    aliases = entry.setdefault("aliases", [])
                    if alias_symbol not in aliases:
                        aliases.append(alias_symbol)

                key = (trading_symbol, exchange_segment)
                if key not in data or expiry_flag == "M":
                    data[key] = entry

                if entry.get("aliases"):
                    for alias in entry["aliases"]:
                        data[(alias, exchange_segment)] = entry

    return data


def load_symbol_slice(
    symbol: str, exchange: str | None = None
) -> Dict[str, Dict[str, Dict[str, str]]]:
    """Load a minimal mapping for *symbol* using cached instrument data."""

    root_symbol = extract_root_symbol(symbol)
    if not root_symbol:
        return {}

    zerodha_slice = _load_zerodha_slice(symbol, exchange)
    dhan_slice = _load_dhan_slice(symbol, exchange)
    slice_map = _assemble_symbol_map(zerodha_slice, dhan_slice)
    return slice_map.get(root_symbol, {})


def ensure_symbol_cache() -> None:
    """Ensure cached copies of instrument data exist."""

    _ensure_cached_csv(ZERODHA_URL, "zerodha_instruments.csv")
    _ensure_cached_csv(DHAN_URL, "dhan_scrip_master.csv")


def ensure_symbol_slice(
    symbol: str, exchange: str | None = None
) -> Dict[str, Dict[str, Dict[str, str]]]:
    """Ensure *symbol* is present in ``SYMBOL_MAP`` using cached slices."""

    root_symbol = extract_root_symbol(symbol)
    if not root_symbol:
        return {}

    existing = SYMBOL_MAP.get(root_symbol)
    exchange_hint = exchange.upper() if exchange else None
    if existing:
        if not exchange_hint:
            return existing
        per_exchange = existing.get(SYMBOLS_KEY, {})
        if exchange_hint in existing or exchange_hint in per_exchange:
            return existing

    slice_entry = load_symbol_slice(symbol, exchange)
    if not slice_entry:
        return {}

    target = SYMBOL_MAP.setdefault(root_symbol, {})
    for key, value in slice_entry.items():
        if key == SYMBOLS_KEY:
            symbols_map = target.setdefault(SYMBOLS_KEY, {})
            for exch_name, aliases in value.items():
                exchange_map = symbols_map.setdefault(exch_name, {})
                exchange_map.update(aliases)
        else:
            target[key] = value

    _load_zerodha.cache_clear()
    _load_dhan.cache_clear()
    _cached_token_lookup.cache_clear()
    return target


def refresh_symbol_slice(
    symbol: str, exchange: str | None = None
) -> Dict[str, Dict[str, Dict[str, Dict[str, str]]]]:
    """Refresh the cached mapping for the root of *symbol* only."""

    root_symbol = extract_root_symbol(symbol)
    if not root_symbol:
        return {}

    with _REFRESH_LOCK:
        SYMBOL_MAP.pop(root_symbol, None)

    for url, cache_name in (
        (ZERODHA_URL, "zerodha_instruments.csv"),
        (DHAN_URL, "dhan_scrip_master.csv"),
    ):
        with suppress(requests.RequestException):
            _ensure_cached_csv(url, cache_name)

    try:
        return ensure_symbol_slice(symbol, exchange)
    except requests.RequestException as exc:  # pragma: no cover - defensive
        log.warning("Failed to refresh symbol slice for %s: %s", symbol, exc)
        return {}


def get_symbol_for_broker_lazy(
    symbol: str, broker: str, exchange: str | None = None
) -> Dict[str, str]:
    """Lookup symbol data using cached slices without building the full map."""

    ensure_symbol_slice(symbol, exchange)
    return get_symbol_for_broker(symbol, broker, exchange)


def _assemble_symbol_map(
    zerodha_data: Dict[Key, Dict[str, str]],
    dhan_data: Dict[Key, Dict[str, str]],
) -> Dict[str, Dict[str, Dict[str, Dict[str, str]]]]:
    """Assemble the nested mapping used for lookups."""
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
        dhan_aliases: list[str] = []
        dhan_info = dhan_data.get((symbol, exchange))
        if (
            dhan_info is None
            and is_equity
            and exchange == "NSE"
        ):
            dhan_info = dhan_data.get((f"{symbol}-EQ", exchange))
        if dhan_info:
            raw_aliases = dhan_info.get("aliases")
            if raw_aliases:
                if isinstance(raw_aliases, str):
                    dhan_aliases = [raw_aliases]
                else:
                    dhan_aliases = list(raw_aliases)

            if exchange in {"NSE", "BSE"}:
                exch_segment = f"{exchange}_EQ"
            elif exchange in {"NFO", "BFO"}:
                exch_segment = f"{'NSE' if exchange == 'NFO' else 'BSE'}_FNO"
            else:
                exch_segment = exchange

            # Use Dhan's lot size if available, otherwise fallback to Zerodha
            dhan_lot_size = dhan_info.get("lot_size", lot_size)

            dhan_entry = {
                "security_id": dhan_info["security_id"],
                "exchange_segment": exch_segment,
                "lot_size": dhan_lot_size,
                "trading_symbol": dhan_info.get("trading_symbol", symbol)
            }

            if dhan_aliases:
                dhan_entry["aliases"] = dhan_aliases

            for optional_key in ("expiry_flag", "expiry_day", "expiry_date"):
                if optional_key in dhan_info:
                    dhan_entry[optional_key] = dhan_info[optional_key]

            entry["dhan"] = dhan_entry

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
        exchange_symbols = symbols.setdefault(exchange, {})

        if is_equity and exchange in {"NSE", "BSE"}:
            aliases = {symbol, f"{symbol}-EQ"}
        else:
            aliases = {symbol}

        if dhan_aliases:
            aliases.update(dhan_aliases)

        for alias in aliases:
            exchange_symbols[alias] = entry
    
    # Add Dhan-only symbols (not in Zerodha)
    for (symbol, exchange), dhan_info in dhan_data.items():
        root_symbol = extract_root_symbol(symbol)

        if exchange in {"NSE", "BSE"}:
            exch_segment = f"{exchange}_EQ"
        elif exchange in {"NFO", "BFO"}:
            exch_segment = f"{'NSE' if exchange == 'NFO' else 'BSE'}_FNO"
        else:
            exch_segment = exchange

        lot_size = dhan_info.get("lot_size", "1")

        raw_aliases = dhan_info.get("aliases")
        if raw_aliases:
            if isinstance(raw_aliases, str):
                dhan_aliases = [raw_aliases]
            else:
                dhan_aliases = list(raw_aliases)
        else:
            dhan_aliases = []

        entry = {
            "dhan": {
                "security_id": dhan_info["security_id"],
                "exchange_segment": exch_segment,
                "lot_size": lot_size,
                "trading_symbol": dhan_info.get("trading_symbol", symbol)
            }
        }

        if dhan_aliases:
            entry["dhan"]["aliases"] = dhan_aliases

        for optional_key in ("expiry_flag", "expiry_day", "expiry_date"):
            if optional_key in dhan_info:
                entry["dhan"][optional_key] = dhan_info[optional_key]

        root_entry = mapping.setdefault(root_symbol, {})
        if (
            exchange not in root_entry
            or dhan_info.get("expiry_flag", "").upper() == "M"
        ):
            root_entry[exchange] = entry
        symbols = root_entry.setdefault(SYMBOLS_KEY, {})
        exchange_symbols = symbols.setdefault(exchange, {})

        if exchange in {"NSE", "BSE"}:
            aliases = {symbol}
            if symbol.upper().endswith("-EQ"):
                stripped = symbol[:-3].strip()
                if stripped:
                    aliases.add(stripped)
            else:
                aliases.add(f"{symbol}-EQ")
        else:
            aliases = {symbol}

        if dhan_aliases:
            aliases.update(dhan_aliases)

        dhan_entry = entry["dhan"]
        for alias in aliases:
            existing = exchange_symbols.get(alias)
            if existing is not None:
                existing["dhan"] = dhan_entry
                continue

            if (
                alias == symbol
                and alias in exchange_symbols
                and dhan_info.get("expiry_flag", "").upper() != "M"
            ):
                continue
            exchange_symbols[alias] = entry
    
    return mapping

def build_symbol_map() -> Dict[str, Dict[str, Dict[str, Dict[str, str]]]]:
    """Construct the complete symbol map with lot size information."""
    zerodha_data = _load_zerodha()
    dhan_data = _load_dhan()

    return _assemble_symbol_map(zerodha_data, dhan_data)


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
            _cached_token_lookup.cache_clear()
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

    # Determine whether the lookup is for a derivative instrument so we can
    # prefer the corresponding F&O exchange segments when available.
    is_derivative = bool(re.search(r"(FUT|CE|PE)$", upper_symbol))

    # Extract root symbol for lookup
    root_symbol = extract_root_symbol(lookup_symbol)

    ensure_symbol_slice(lookup_symbol, exchange_hint)
    mapping = SYMBOL_MAP.get(root_symbol)
    
    if not mapping:
        return {}
    
    # Select the appropriate exchange
    exchange_map = None
    exchange_name = None
    exchange_hint_provided = exchange_hint is not None
    if exchange_hint and exchange_hint in mapping:
        exchange_name = exchange_hint
        exchange_map = mapping[exchange_hint]
        log.debug(f"Using specified exchange: {exchange_hint}")

        # Some callers still provide the cash market exchange (NSE/BSE) even
        # for derivatives. In those cases prefer the corresponding F&O
        # exchange if it is available to avoid incorrect fallbacks.
        if is_derivative and exchange_name in {"NSE", "BSE"}:
            fo_exchange = "NFO" if exchange_name == "NSE" else "BFO"
            if fo_exchange in mapping:
                exchange_name = fo_exchange
                exchange_map = mapping[fo_exchange]
                log.debug("Promoted derivative lookup to %s", fo_exchange)
    elif exchange_hint_provided:
        log.debug(
            "Exchange %s not available for symbol %s", exchange_hint, lookup_symbol
        )
        return {}
    else:
        if is_derivative:
            
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
    symbols = {}
    if exchange_name:
        symbols = mapping.get(SYMBOLS_KEY, {}).get(exchange_name, {})
        entry = symbols.get(lookup_symbol)
        if entry is None and lookup_symbol != upper_symbol:
            entry = symbols.get(upper_symbol)

    if entry is None:
        if is_derivative:
            if symbols:
                return {}
            if exchange_map is not None:
                entry = exchange_map
        elif exchange_map is not None:
            entry = exchange_map

    broker_data = entry.get(broker, {}) if entry else {}


    if exchange_hint_provided and not broker_data:
        log.debug(
            "No data for broker %s on exchange %s for symbol %s",
            broker,
            exchange_hint,
            lookup_symbol,
        )
        return {}
        
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
        ensure_symbol_slice(original_symbol, exchange_hint)
        mapping = SYMBOL_MAP.get(root_symbol)

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
            symbols = {}
            if exchange_name:
                symbols = mapping.get(SYMBOLS_KEY, {}).get(exchange_name, {})
                entry = symbols.get(original_symbol)
                if entry is None and original_symbol != upper_symbol:
                    entry = symbols.get(upper_symbol)
            if entry is None:
                if symbols:
                    return {}
                if exchange_map is not None:
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
    token = str(token).strip()
    if not token:
        return None

    cached_symbol = _cached_token_lookup(token, broker)
    if cached_symbol is not None:
        return cached_symbol

    if not SYMBOL_MAP:
        return None

    for sym, exchanges in SYMBOL_MAP.items():
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
