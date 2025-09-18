"""Shared utilities for F&O symbol conversion between different broker formats.

This module provides functions to parse, format, and convert F&O symbols
between different broker formats (Dhan, Zerodha, AliceBlue, etc.).
"""

from __future__ import annotations

import re
import logging
from datetime import date
from typing import Dict, Optional

log = logging.getLogger(__name__)


_DERIVATIVE_SUFFIX_TRIGGER = re.compile(r"(?:\d|-|CALL|PUT)\s*$")


def _has_derivative_suffix(symbol: str) -> bool:
    """Return ``True`` if the symbol ends with an F&O style suffix.

    The check is intentionally strict so that plain equity tickers such as
    ``RELIANCECE`` or ``SBVCLPE`` are not mistaken for options contracts. A
    trailing ``CE``/``PE`` only qualifies as derivative when it is preceded by
    a strike (digits), a hyphen, or explicit option keywords like ``CALL`` or
    ``PUT``.
    """

    if not symbol:
        return False

    stripped = symbol.rstrip().upper()

    if stripped.endswith("FUT") or stripped.endswith("CALL") or stripped.endswith("PUT"):
        return True

    if stripped.endswith("CE") or stripped.endswith("PE"):
        prefix = stripped[:-2].rstrip()
        if not prefix:
            return False
        if _DERIVATIVE_SUFFIX_TRIGGER.search(prefix):
            return True

    return False


def get_expiry_year(month: str, day: int = None) -> str:
    """Determine the correct expiry year for a given month and day.
    
    Args:
        month: Three-letter month code (JAN, FEB, etc.)
        day: Optional day of month for more accurate year determination
        
    Returns:
        Two-digit year string
    """
    current_date = date.today()
    current_year = current_date.year
    current_month = current_date.month
    current_day = current_date.day
    
    month_map = {
        'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
        'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
    }
    
    month_num = month_map[month]
    
    if day:
        if month_num < current_month:
            year = current_year + 1
        elif month_num == current_month and day < current_day:
            year = current_year + 1
        else:
            year = current_year
    else:
        if month_num < current_month:
            year = current_year + 1
        else:
            year = current_year
    
    return str(year % 100).zfill(2)


def parse_fo_symbol(symbol: str, broker: str) -> Optional[Dict[str, str]]:
    """Parse F&O symbol into components based on broker format.
    
    Args:
        symbol: The F&O symbol to parse
        broker: The broker format (dhan, zerodha, aliceblue, etc.)
        
    Returns:
        Dictionary with symbol components or None if parsing fails
    """
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
    
    elif broker in ['zerodha', 'aliceblue', 'fyers', 'finvasia', 'flattrade']:
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


def format_fo_symbol(components: Dict[str, str], to_broker: str) -> Optional[str]:
    """Format symbol components for target broker.
    
    Args:
        components: Dictionary with symbol components
        to_broker: Target broker format
        
    Returns:
        Formatted symbol or None if formatting fails
    """
    if not components:
        return None
    
    to_broker = to_broker.lower()
    
    if to_broker == 'dhan':
        if components['instrument'] == 'OPT':
            return f"{components['underlying']}-{components['month']}{components['year']}-{components['strike']}-{components['option_type']}"
        elif components['instrument'] == 'FUT':
            return f"{components['underlying']}-{components['month']}{components['year']}-FUT"
    
    elif to_broker in ['zerodha', 'aliceblue', 'fyers', 'finvasia', 'flattrade']:
        year_short = components['year'][-2:]  # Get last 2 digits
        if components['instrument'] == 'OPT':
            return f"{components['underlying']}{year_short}{components['month'].upper()}{components['strike']}{components['option_type']}"
        elif components['instrument'] == 'FUT':
            return f"{components['underlying']}{year_short}{components['month'].upper()}FUT"
    
    return None


def convert_symbol_between_brokers(symbol: str, from_broker: str, to_broker: str, instrument_type: str = None) -> str:
    """Convert F&O symbol from one broker format to another.
    
    Args:
        symbol: The original symbol
        from_broker: Source broker format
        to_broker: Target broker format
        instrument_type: Optional instrument type hint
        
    Returns:
        Converted symbol (returns original if conversion fails)
    """
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


def normalize_symbol_to_dhan_format(symbol: str) -> str:
    """Convert various symbol formats to Dhan's expected format.
    
    Examples:
        NIFTYNXT50SEPFUT -> NIFTYNXT50-Sep2025-FUT
        FINNIFTY25SEP33300CE -> FINNIFTY-Sep2025-33300-CE
    """
    if not symbol:
        return symbol
    
    original_symbol = symbol.strip()
    sym = original_symbol.upper()
    log.debug(f"Normalizing symbol: {sym}")

    # Handle already correctly formatted symbols (with hyphens)
    if '-' in sym and re.search(r'(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)20\d{2}', sym):
        pattern = re.compile(
            r'^(?P<root>.+?)-(?P<month>[A-Za-z]{3})(?P<year>\d{4})(?P<suffix>-(?:\d+-(?:CE|PE)|FUT))$',
            re.IGNORECASE,
        )
        match = pattern.match(original_symbol)
        if match:
            month = match.group('month').upper().title()
            normalized = f"{match.group('root')}-{month}{match.group('year')}{match.group('suffix')}"
            log.debug(
                "Symbol already in correct format, preserving casing: %s -> %s",
                original_symbol,
                normalized,
            )
            return normalized

        log.debug(
            "Symbol already in correct format, returning original: %s",
            original_symbol,
        )
        return original_symbol
        
    # Pattern 1: Compact futures format with explicit year: FINNIFTY25SEPFUT
    fut_with_year = re.match(
        r'^(.+?)(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)FUT$',
        sym
    )
    if fut_with_year:
        root, year, month = fut_with_year.groups()
        year_num = int(year)
        if 24 <= year_num <= 30:
            full_year = f"20{year}"
            normalized = f"{root}-{month.title()}{full_year}-FUT"
            log.info(f"Normalized futures with year from '{sym}' to '{normalized}'")
            return normalized
    
    # Pattern 2: Compact futures format without explicit year: NIFTYNXT50SEPFUT
    fut_no_year = re.match(
        r'^(.+?)(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)FUT$',
        sym
    )
    if fut_no_year:
        root, month = fut_no_year.groups()
        year = get_expiry_year(month)
        full_year = f"20{year}"
        normalized = f"{root}-{month.title()}{full_year}-FUT"
        log.info(f"Normalized futures without year from '{sym}' to '{normalized}'")
        return normalized
    
    # Pattern 3: Options with explicit year: FINNIFTY25SEP33300CE
    opt_with_year = re.match(
        r'^(.+?)(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d+)(CE|PE)$',
        sym
    )
    if opt_with_year:
        root, year, month, strike, opt_type = opt_with_year.groups()
        year_num = int(year)
        if 24 <= year_num <= 30:
            full_year = f"20{year}"
            normalized = f"{root}-{month.title()}{full_year}-{strike}-{opt_type}"
            log.info(f"Normalized options with year from '{sym}' to '{normalized}'")
            return normalized
    
    # Pattern 4: Options without explicit year: NIFTYNXT50SEP33300CE
    opt_no_year = re.match(
        r'^(.+?)(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d+)(CE|PE)$',
        sym
    )
    if opt_no_year:
        root, month, strike, opt_type = opt_no_year.groups()
        year = get_expiry_year(month)
        full_year = f"20{year}"
        normalized = f"{root}-{month.title()}{full_year}-{strike}-{opt_type}"
        log.info(f"Normalized options without year from '{sym}' to '{normalized}'")
        return normalized
    
    # Pattern 5: Handle equity symbols
    if not re.search(r'(FUT|CE|PE)$', sym) and not sym.endswith('-EQ'):
        if not re.search(r'\d', sym) or re.match(r'^[A-Z]+\d+$', sym):
            normalized = f"{sym}-EQ" if not sym.endswith('-EQ') else sym
            log.info(f"Normalized equity symbol: {normalized}")
            return normalized
    
    log.debug(f"No normalization pattern matched for: {sym}")
    return sym


def get_default_lot_size(symbol: str) -> int:
    """Get default lot size for common F&O instruments.
    
    Args:
        symbol: The F&O symbol
        
    Returns:
        Default lot size
    """
    symbol_upper = symbol.upper()
    
    lot_size_map = {
        'NIFTY': 50,
        'BANKNIFTY': 25, 
        'FINNIFTY': 40,
        'NIFTYNXT': 50,
        'MIDCPNIFTY': 75,
        'SENSEX': 10,
        'BANKEX': 15,
    }
    
    for underlying, size in lot_size_map.items():
        if underlying in symbol_upper:
            return size
    
    # Default for stock F&O
    return 1


def is_fo_symbol(symbol: str, instrument_type: str = None) -> bool:
    """Return ``True`` when the provided symbol represents an F&O contract."""
    if not symbol:
        return False
        
    # Instrument type hint takes precedence.
    if instrument_type:
        inst_upper = instrument_type.upper()
        if inst_upper in {"FUT", "FUTSTK", "FUTIDX", "OPT", "OPTSTK", "OPTIDX", "CE", "PE"}:
            return True
    
    return _has_derivative_suffix(symbol)


def extract_underlying_symbol(symbol: str) -> str:
    """Extract the underlying symbol from an F&O symbol.
    
    Args:
        symbol: The F&O symbol
        
    Returns:
        The underlying symbol
    """
    if not symbol:
        return symbol
    
    # Handle Dhan format with hyphens
    if '-' in symbol:
        return symbol.split('-')[0]
    
    # Handle compact format without hyphens
    cleaned = symbol.upper()
    
    # Remove trailing FUT, CE, PE
    for suffix in ['FUT', 'CE', 'PE']:
        if cleaned.endswith(suffix):
            cleaned = cleaned[:-len(suffix)]
            break
    
    # Remove month and year patterns
    cleaned = re.sub(r'\d{2}(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)', '', cleaned)
    
    # Remove strike prices (numbers at the end)
    cleaned = re.sub(r'\d+$', '', cleaned)
    
    return cleaned


__all__ = [
    "get_expiry_year",
    "parse_fo_symbol",
    "format_fo_symbol",
    "convert_symbol_between_brokers",
    "normalize_symbol_to_dhan_format",
    "get_default_lot_size",
    "is_fo_symbol",
    "extract_underlying_symbol",
]
