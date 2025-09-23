"""Shared utilities for F&O symbol conversion between different broker formats.

This module provides functions to parse, format, and convert F&O symbols
between different broker formats (Dhan, Zerodha, AliceBlue, etc.).
"""

from __future__ import annotations

import logging
import re
from decimal import Decimal, InvalidOperation
from datetime import date
from functools import lru_cache
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - import for type checkers only
    from brokers import symbol_map as symbol_map_module

log = logging.getLogger(__name__)

_symbol_map_module = None


_DERIVATIVE_SUFFIX_TRIGGER = re.compile(r"(?:\d|-|CALL|PUT)\s*$")


def _should_include_expiry_day(underlying: str) -> bool:
    """Return ``True`` when the expiry *day* should be encoded in the symbol."""

    if not underlying:
        return False

    root = underlying.upper()

    # Currency contracts keep their day component so that weekly expiries
    # remain distinguishable (e.g. ``USDINR-23Sep2024-83.00-CE``).
    if root.endswith("INR"):
        return True

    # Equity / index contracts (e.g. NIFTY, BANKNIFTY) omit the day component
    # in their canonical trading symbol while callers still retain the value in
    # metadata.
    return False


def format_dhan_option_symbol(
    underlying: str,
    month: str,
    year: str,
    strike: str | int,
    option_type: str,
    *,
    day: int | None = None,
) -> str:
    """Return a Dhan formatted option symbol including weekly expiry day when available."""

    strike_str = str(strike).strip()

    if strike_str:
        try:
            strike_decimal = Decimal(strike_str)
        except (InvalidOperation, ValueError):
            strike_decimal = None

        if strike_decimal is not None:
            if strike_decimal == strike_decimal.to_integral():
                strike_str = str(int(strike_decimal))
            else:
                strike_str = f"{strike_decimal.quantize(Decimal('0.01')):.2f}"

    opt_code = option_type.upper()
    month_part = month.title()
    include_day = day is not None and _should_include_expiry_day(underlying)
    if include_day:
        date_part = f"{int(day):02d}{month_part}{year}"
    else:
        date_part = f"{month_part}{year}"
    return f"{underlying}-{date_part}-{strike_str}-{opt_code}"


def format_dhan_future_symbol(
    underlying: str,
    month: str,
    year: str,
    *,
    day: int | None = None,
) -> str:
    """Return a Dhan formatted future symbol including weekly expiry day when available."""

    month_part = month.title()
    include_day = day is not None and _should_include_expiry_day(underlying)
    if include_day:
        date_part = f"{int(day):02d}{month_part}{year}"
    else:
        date_part = f"{month_part}{year}"
    return f"{underlying}-{date_part}-FUT"


@lru_cache(maxsize=128)
def _lookup_currency_future_expiry_day(
    root: str,
    month: str,
    year: str,
) -> int | None:
    """Return the expiry *day* for a currency future from the symbol map."""

    if not root or not month or not year:
        return None

    root_clean = root.strip().upper()
    month_clean = month.strip().upper()
    year_clean = str(year).strip()

    if not root_clean.endswith("INR"):
        return None

    if len(year_clean) == 2:
        year_clean = f"20{year_clean}"

    global _symbol_map_module

    if _symbol_map_module is None:
        from brokers import symbol_map as imported_symbol_map  # local import for lazy loading

        _symbol_map_module = imported_symbol_map

    symbol_map = _symbol_map_module

    try:
        symbol_map.ensure_symbol_slice(root_clean, None)
    except Exception as exc:  # pragma: no cover - defensive
        log.debug("Could not load symbol slice for %s: %s", root_clean, exc)

    mapping = getattr(symbol_map, "SYMBOL_MAP", {}).get(root_clean)
    if not mapping:
        return None

    exchange_aliases = mapping.get(symbol_map.SYMBOLS_KEY, {})
    day_pattern = re.compile(
        rf"-([0-9]{{1,2}}){month_clean}{year_clean}-FUT$",
        re.IGNORECASE,
    )

    for aliases in exchange_aliases.values():
        for alias, entry in aliases.items():
            alias_upper = alias.upper()
            if not alias_upper.endswith("-FUT"):
                continue
            if month_clean not in alias_upper or year_clean not in alias_upper:
                continue

            dhan_info = (entry or {}).get("dhan", {})
            expiry_day = dhan_info.get("expiry_day")
            if isinstance(expiry_day, str) and expiry_day.strip().isdigit():
                return int(expiry_day)
            if isinstance(expiry_day, (int, float)):
                return int(expiry_day)

            match = day_pattern.search(alias_upper)
            if match:
                try:
                    return int(match.group(1))
                except ValueError:
                    continue

    return None


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


def parse_fo_symbol(symbol: str, broker: str) -> dict[str, str] | None:
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
        # NIFTY-23Sep2024-24000-CE or NIFTY-Sep2024-24000-CE format
        opt_match = re.match(
            r'^(?P<underlying>.+?)-(?:(?P<day>\d{1,2}))?(?P<month>[A-Za-z]{3})(?P<year>\d{4})-(?P<strike>\d+(?:\.\d+)?)-(?P<option>CE|PE)$',
            symbol,
        )
        if opt_match:
            data = {
                'underlying': opt_match.group('underlying'),
                'month': opt_match.group('month'),
                'year': opt_match.group('year'),
                'strike': opt_match.group('strike'),
                'option_type': opt_match.group('option').upper(),
                'instrument': 'OPT'
            }
        
            day = opt_match.group('day')
            if day:
                data['day'] = int(day)
            return data

        fut_match = re.match(
            r'^(?P<underlying>.+?)-(?:(?P<day>\d{1,2}))?(?P<month>[A-Za-z]{3})(?P<year>\d{4})-FUT$',
            symbol,
        )
        if fut_match:
            data = {
                'underlying': fut_match.group('underlying'),
                'month': fut_match.group('month'),
                'year': fut_match.group('year'),
                'instrument': 'FUT'
            }
            day = fut_match.group('day')
            if day:
                data['day'] = int(day)
            return data
    
    elif broker in ['zerodha', 'aliceblue', 'fyers', 'finvasia', 'flattrade']:
        # NIFTY24DEC24000CE format
        opt_match = re.match(r'^(.+?)(\d{2})(\w{3})(\d+(?:\.\d+)?)(CE|PE)$', symbol)
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


def format_fo_symbol(components: dict[str, str], to_broker: str) -> str | None:
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
            return format_dhan_option_symbol(
                components['underlying'],
                components['month'],
                components['year'],
                components['strike'],
                components['option_type'],
                day=components.get('day'),
            )
        elif components['instrument'] == 'FUT':
            return format_dhan_future_symbol(
                components['underlying'],
                components['month'],
                components['year'],
                day=components.get('day'),
            )
    
    elif to_broker in ['zerodha', 'aliceblue', 'fyers', 'finvasia', 'flattrade']:
        year_short = components['year'][-2:]  # Get last 2 digits
        if components['instrument'] == 'OPT':
            return f"{components['underlying']}{year_short}{components['month'].upper()}{components['strike']}{components['option_type']}"
        elif components['instrument'] == 'FUT':
            return f"{components['underlying']}{year_short}{components['month'].upper()}FUT"
    
    return None


def convert_symbol_between_brokers(
    symbol: str, from_broker: str, to_broker: str, instrument_type: str | None = None
) -> str:
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
            r'^(?P<root>.+?)-(?:(?P<day>\d{1,2}))?(?P<month>[A-Za-z]{3})(?P<year>\d{4})(?P<suffix>-(?:\d+(?:\.\d+)?-(?:CE|PE)|FUT))$',
            re.IGNORECASE,
        )
        match = pattern.match(original_symbol)
        if match:
            root = match.group('root')
            month = match.group('month').upper()
            year = match.group('year')
            day = int(match.group('day')) if match.group('day') else None
            suffix = match.group('suffix')

            if suffix.upper().endswith('-FUT'):
                normalized = format_dhan_future_symbol(
                    root,
                    month,
                    year,
                    day=day,
                )
            else:
                strike, opt_type = suffix[1:].rsplit('-', 1)
                normalized = format_dhan_option_symbol(
                    root,
                    month,
                    year,
                    strike,
                    opt_type,
                    day=day,
                )

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

    # Explicit day-first option format: "NIFTY 23 SEP 25500 CALL"
    day_first_option = re.match(
        r'^([A-Z]+(?:\d+)?)\s+(\d{1,2})\s+(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+(\d+(?:\.\d+)?)\s+(CALL|PUT|CE|PE)$',
        sym,
    )
    if day_first_option:
        root, day_str, month, strike, opt_type = day_first_option.groups()
        day = int(day_str)
        year = get_expiry_year(month, day)
        full_year = f"20{year}"
        opt_code = "CE" if opt_type in ("CALL", "CE") else "PE"
        normalized = format_dhan_option_symbol(
            root,
            month,
            full_year,
            strike,
            opt_code,
            day=day,
        )
        log.info(f"Normalized from day-first format '{sym}' to '{normalized}'")
        return normalized

    # Futures with explicit day: "NIFTY 23 SEP FUT"
    fut_with_day = re.match(
        r'^([A-Z]+(?:\d+)?)\s+(\d{1,2})\s+(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+FUT$',
        sym,
    )
    if fut_with_day:
        root, day_str, month = fut_with_day.groups()
        day = int(day_str)
        year = get_expiry_year(month, day)
        full_year = f"20{year}"
        normalized = format_dhan_future_symbol(
            root,
            month,
            full_year,
            day=day,
        )
        log.info(f"Normalized futures with day from '{sym}' to '{normalized}'")
        return normalized

    # Futures written with a space but without explicit day: "NIFTY SEP FUT"
    fut_spaced_no_day = re.match(
        r'^([A-Z]+(?:\d+)?)\s+(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+FUT$',
        sym,
    )
    if fut_spaced_no_day:
        root, month = fut_spaced_no_day.groups()
        year = get_expiry_year(month)
        full_year = f"20{year}"
        expiry_day = None
        if root.upper().endswith("INR"):
            expiry_day = _lookup_currency_future_expiry_day(root, month, full_year)
        normalized = format_dhan_future_symbol(
            root,
            month,
            full_year,
            day=expiry_day,
        )
        log.info(f"Normalized spaced futures from '{sym}' to '{normalized}'")
        return normalized

    # Pattern 1: Compact futures format with explicit year: FINNIFTY25SEPFUT
    fut_with_year = re.match(
        r'^(.+?)(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)FUT$',
        sym,
    )
    if fut_with_year:
        root, year, month = fut_with_year.groups()
        year_num = int(year)
        if 24 <= year_num <= 30:
            full_year = f"20{year}"
            normalized = format_dhan_future_symbol(root, month, full_year)
            log.info(f"Normalized futures with year from '{sym}' to '{normalized}'")
            return normalized
    
    # Pattern 2: Compact futures format without explicit year: NIFTYNXT50SEPFUT
    fut_no_year = re.match(
        r'^(.+?)(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)FUT$',
        sym,
    )
    if fut_no_year:
        root, month = fut_no_year.groups()
        year = get_expiry_year(month)
        full_year = f"20{year}"
        expiry_day = None
        if root.upper().endswith("INR"):
            expiry_day = _lookup_currency_future_expiry_day(root, month, full_year)
        normalized = format_dhan_future_symbol(
            root,
            month,
            full_year,
            day=expiry_day,
        )
        log.info(f"Normalized futures without year from '{sym}' to '{normalized}'")
        return normalized
    
    # Pattern 3: Options with explicit year: FINNIFTY25SEP33300CE
    opt_with_year = re.match(
        r'^(.+?)(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d+(?:\.\d+)?)(CE|PE)$',
        sym,
    )
    if opt_with_year:
        root, year, month, strike, opt_type = opt_with_year.groups()
        year_num = int(year)
        if 24 <= year_num <= 30:
            full_year = f"20{year}"
            normalized = format_dhan_option_symbol(
                root,
                month,
                full_year,
                strike,
                opt_type,
            )
            log.info(f"Normalized options with year from '{sym}' to '{normalized}'")
            return normalized
    
    # Pattern 4: Options without explicit year: NIFTYNXT50SEP33300CE
    opt_no_year = re.match(
        r'^(.+?)(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d+(?:\.\d+)?)(CE|PE)$',
        sym,
    )
    if opt_no_year:
        root, month, strike, opt_type = opt_no_year.groups()
        year = get_expiry_year(month)
        full_year = f"20{year}"
        normalized = format_dhan_option_symbol(
            root,
            month,
            full_year,
            strike,
            opt_type,
        )
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
    "is_fo_symbol",
    "extract_underlying_symbol",
]
