"""Webhook receiver service - Enhanced symbol normalization for F&O.

This module provides minimal validation and serialization of webhook
payloads and publishes validated events to a low-latency queue. Redis
Streams is used as the backing queue so downstream workers can consume
events asynchronously.
"""

from __future__ import annotations

import os
import logging
import json
import re
from datetime import datetime, date
import uuid
from typing import Optional, Dict, Any

import redis
from marshmallow import Schema, fields, ValidationError, pre_load

from .alert_guard import check_duplicate_and_risk
from brokers import symbol_map

logger = logging.getLogger(__name__)

# Redis client used for publishing events
redis_client: Optional[redis.Redis] = None


def get_redis_client() -> redis.Redis:
    """Return a Redis client configured from ``REDIS_URL``."""
    global redis_client
    if redis_client is None:
        redis_url = os.getenv("REDIS_URL")
        if not redis_url:
            msg = "REDIS_URL environment variable must be set to connect to Redis"
            logger.error(msg)
            raise RuntimeError(msg)
        redis_client = redis.Redis.from_url(redis_url)
    return redis_client


def get_expiry_year(month: str, day: int = None) -> str:
    """Determine the correct expiry year for a given month and day."""
    current_date = date.today()
    current_year = current_date.year
    current_month = current_date.month
    current_day = current_date.day
    
    month_num = {
        'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
        'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
    }[month]
    
    # If we have a specific day, use it for more accurate determination
    if day:
        # Check if the expiry date has passed this year
        if month_num < current_month:
            # Month has passed, must be next year
            year = current_year + 1
        elif month_num == current_month and day < current_day:
            # Same month but day has passed, must be next year
            year = current_year + 1
        else:
            # Future date this year
            year = current_year
    else:
        # No specific day, use simple month comparison
        if month_num < current_month:
            year = current_year + 1
        else:
            year = current_year
    
    return str(year % 100).zfill(2)


def get_lot_size_from_symbol_map(symbol: str, exchange: str = None) -> int:
    """Get lot size from the symbol map for a given symbol."""
    try:
        # Ensure symbol map is loaded
        if not symbol_map.SYMBOL_MAP:
            symbol_map.SYMBOL_MAP = symbol_map.build_symbol_map()
        
        # Get the symbol mapping
        mapping = symbol_map.get_symbol_for_broker(symbol, "dhan", exchange)
        
        if mapping and "lot_size" in mapping:
            lot_size = mapping["lot_size"]
            if lot_size:
                try:
                    return int(lot_size)
                except (ValueError, TypeError):
                    pass
        
        logger.warning(f"Could not find lot size for symbol {symbol} in symbol map")
        return None
        
    except Exception as e:
        logger.error(f"Error fetching lot size from symbol map for {symbol}: {e}")
        return None


def normalize_fo_symbol(symbol: str) -> tuple[str, dict]:
    """Normalize F&O symbol to standardized format and extract metadata.
    
    Returns:
        tuple: (normalized_symbol, metadata_dict)
    """
    if not symbol:
        return symbol, {}
    
    sym = symbol.upper().strip()
    metadata = {}
    
    # Pattern 1: Already normalized Dhan format with hyphens
    if '-' in sym and re.search(r'(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)20\d{2}', sym):
        # Extract metadata from Dhan format
        opt_match = re.match(r'^(.+?)-(\w{3})(\d{4})-(\d+)-(CE|PE)$', sym)
        if opt_match:
            underlying = opt_match.group(1)
            metadata = {
                'underlying': underlying,
                'expiry_month': opt_match.group(2),
                'expiry_year': opt_match.group(3),
                'strike': int(opt_match.group(4)),
                'option_type': opt_match.group(5),
                'instrument_type': 'OPTIDX' if 'NIFTY' in underlying else 'OPTSTK'
            }
            # Get lot size from symbol map
            lot_size = get_lot_size_from_symbol_map(sym, "NFO")
            if lot_size:
                metadata['lot_size'] = lot_size
        
        fut_match = re.match(r'^(.+?)-(\w{3})(\d{4})-FUT$', sym)
        if fut_match:
            underlying = fut_match.group(1)
            metadata = {
                'underlying': underlying,
                'expiry_month': fut_match.group(2),
                'expiry_year': fut_match.group(3),
                'instrument_type': 'FUTIDX' if 'NIFTY' in underlying else 'FUTSTK'
            }
            # Get lot size from symbol map
            lot_size = get_lot_size_from_symbol_map(sym, "NFO")
            if lot_size:
                metadata['lot_size'] = lot_size
        
        return sym, metadata
    
    # Pattern 2: Special format "FINNIFTY 30 SEP 33300 CALL"
    special_pattern = re.match(
        r'^([A-Z]+)\s+(\d{1,2})\s+(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+(\d+)\s+(CALL|PUT|CE|PE)
    if special_pattern:
        root = special_pattern.group(1)
        day = int(special_pattern.group(2))
        month = special_pattern.group(3)
        strike = special_pattern.group(4)
        opt_type = special_pattern.group(5)
        
        year = get_expiry_year(month, day)
        full_year = f"20{year}"
        opt_code = "CE" if opt_type in ("CALL", "CE") else "PE"
        
        normalized = f"{root}-{month.title()}{full_year}-{strike}-{opt_code}"
        metadata = {
            'underlying': root,
            'expiry_month': month,
            'expiry_year': full_year,
            'expiry_day': day,
            'strike': int(strike),
            'option_type': opt_code,
            'instrument_type': 'OPTIDX' if 'NIFTY' in root else 'OPTSTK'
        }
        # Get lot size from symbol map
        lot_size = get_lot_size_from_symbol_map(normalized, "NFO")
        if lot_size:
            metadata['lot_size'] = lot_size
        
        return normalized, metadata
    
    # Pattern 3: Futures with day: "FINNIFTY 30 SEP FUT"
    fut_with_day = re.match(
        r'^([A-Z]+)\s+(\d{1,2})\s+(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+FUT$',
        sym
    )
    if fut_with_day:
        root = fut_with_day.group(1)
        day = int(fut_with_day.group(2))
        month = fut_with_day.group(3)
        
        year = get_expiry_year(month, day)
        full_year = f"20{year}"
        normalized = f"{root}-{month.title()}{full_year}-FUT"
        metadata = {
            'underlying': root,
            'expiry_month': month,
            'expiry_year': full_year,
            'expiry_day': day,
            'instrument_type': 'FUTIDX' if 'NIFTY' in root else 'FUTSTK'
        }
        # Get lot size from symbol map
        lot_size = get_lot_size_from_symbol_map(normalized, "NFO")
        if lot_size:
            metadata['lot_size'] = lot_size
        
        return normalized, metadata
    
    # Pattern 4: Compact options with year: FINNIFTY25SEP33300CE, NIFTYNXT5025NOV35500CALL
    compact_opt_match = re.match(
        r'^(.+?)(\d{2})?(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d+)(CALL|PUT|CE|PE)$',
        sym
    )
    if compact_opt_match:
        root, year, month, strike, opt_type = compact_opt_match.groups()
        if not year:
            year = get_expiry_year(month)
        full_year = f"20{year}"
        opt_code = "CE" if opt_type in ("CALL", "CE") else "PE"
        
        normalized = f"{root}-{month.title()}{full_year}-{strike}-{opt_code}"
        metadata = {
            'underlying': root,
            'expiry_month': month,
            'expiry_year': full_year,
            'strike': int(strike),
            'option_type': opt_code,
            'instrument_type': 'OPTIDX' if 'NIFTY' in root else 'OPTSTK'
        }
        # Get lot size from symbol map
        lot_size = get_lot_size_from_symbol_map(normalized, "NFO")
        if lot_size:
            metadata['lot_size'] = lot_size
        
        return normalized, metadata
    
    # Pattern 5: Compact futures: FINNIFTY25SEPFUT
    compact_fut_match = re.match(
        r'^(.+?)(\d{2})?(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)FUT$',
        sym
    )
    if compact_fut_match:
        root, year, month = compact_fut_match.groups()
        if not year:
            year = get_expiry_year(month)
        full_year = f"20{year}"
        
        normalized = f"{root}-{month.title()}{full_year}-FUT"
        metadata = {
            'underlying': root,
            'expiry_month': month,
            'expiry_year': full_year,
            'instrument_type': 'FUTIDX' if 'NIFTY' in root else 'FUTSTK'
        }
        # Get lot size from symbol map
        lot_size = get_lot_size_from_symbol_map(normalized, "NFO")
        if lot_size:
            metadata['lot_size'] = lot_size
        
        return normalized, metadata
    
    # Pattern 6: Spaced options: "FINNIFTY 25 SEP 33300 CALL"
    spaced_opt_match = re.match(
        r'^([A-Z]+)\s+(?:(\d{2})\s+)?(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+(\d+)\s+(CALL|PUT|CE|PE)$',
        sym
    )
    if spaced_opt_match:
        root, year, month, strike, opt_type = spaced_opt_match.groups()
        if not year:
            year = get_expiry_year(month)
        full_year = f"20{year}"
        opt_code = "CE" if opt_type in ("CALL", "CE") else "PE"
        
        normalized = f"{root}-{month.title()}{full_year}-{strike}-{opt_code}"
        metadata = {
            'underlying': root,
            'expiry_month': month,
            'expiry_year': full_year,
            'strike': int(strike),
            'option_type': opt_code,
            'instrument_type': 'OPTIDX' if 'NIFTY' in root else 'OPTSTK'
        }
        # Get lot size from symbol map
        lot_size = get_lot_size_from_symbol_map(normalized, "NFO")
        if lot_size:
            metadata['lot_size'] = lot_size
        
        return normalized, metadata
    
    # Pattern 7: Spaced futures: "FINNIFTY 25 SEP FUT"
    spaced_fut_match = re.match(
        r'^([A-Z]+)\s+(?:(\d{2})\s+)?(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+FUT$',
        sym
    )
    if spaced_fut_match:
        root, year, month = spaced_fut_match.groups()
        if not year:
            year = get_expiry_year(month)
        full_year = f"20{year}"
        
        normalized = f"{root}-{month.title()}{full_year}-FUT"
        metadata = {
            'underlying': root,
            'expiry_month': month,
            'expiry_year': full_year,
            'instrument_type': 'FUTIDX' if 'NIFTY' in root else 'FUTSTK'
        }
        # Get lot size from symbol map
        lot_size = get_lot_size_from_symbol_map(normalized, "NFO")
        if lot_size:
            metadata['lot_size'] = lot_size
        
        return normalized, metadata
    
    # Pattern 8: Already normalized derivative symbols (compact format)
    if re.match(r'^[A-Z]+\d{2}(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d+)?(FUT|CE|PE)$', sym):
        # Extract components for metadata
        opt_match = re.match(r'^(.+?)(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d+)(CE|PE)$', sym)
        if opt_match:
            root, year, month, strike, opt_type = opt_match.groups()
            metadata = {
                'underlying': root,
                'expiry_month': month,
                'expiry_year': f"20{year}",
                'strike': int(strike),
                'option_type': opt_type,
                'instrument_type': 'OPTIDX' if 'NIFTY' in root else 'OPTSTK'
            }
            # Convert to Dhan format and get lot size
            normalized_dhan = f"{root}-{month.title()}20{year}-{strike}-{opt_type}"
            lot_size = get_lot_size_from_symbol_map(normalized_dhan, "NFO")
            if lot_size:
                metadata['lot_size'] = lot_size
        
        fut_match = re.match(r'^(.+?)(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)FUT$', sym)
        if fut_match:
            root, year, month = fut_match.groups()
            metadata = {
                'underlying': root,
                'expiry_month': month,
                'expiry_year': f"20{year}",
                'instrument_type': 'FUTIDX' if 'NIFTY' in root else 'FUTSTK'
            }
            # Convert to Dhan format and get lot size
            normalized_dhan = f"{root}-{month.title()}20{year}-FUT"
            lot_size = get_lot_size_from_symbol_map(normalized_dhan, "NFO")
            if lot_size:
                metadata['lot_size'] = lot_size
        
        return sym, metadata
    
    # Pattern 9: Equity symbols
    if not re.search(r'(FUT|CE|PE)$', sym) and not sym.endswith('-EQ'):
        if not re.search(r'\d', sym) or re.match(r'^[A-Z]+\d+$', sym):
            normalized = f"{sym}-EQ"
            metadata = {
                'underlying': sym,
                'instrument_type': 'EQ'
            }
            # Get lot size from symbol map (usually 1 for equity)
            lot_size = get_lot_size_from_symbol_map(normalized, "NSE")
            if lot_size:
                metadata['lot_size'] = lot_size
            else:
                metadata['lot_size'] = 1  # Default for equity
            
            return normalized, metadata
    
    # Return original if no pattern matches
    return sym, metadata


class WebhookEventSchema(Schema):
    """Schema for validating webhook events."""

    user_id = fields.Int(required=True)
    strategy_id = fields.Int(allow_none=True)
    symbol = fields.Str(required=True)
    action = fields.Str(required=True)
    qty = fields.Int(required=True)
    exchange = fields.Str(load_default="NSE")
    order_type = fields.Str(load_default=None)
    alert_id = fields.Str(load_default=None)
    # Broker specific fields
    orderType = fields.Str(load_default=None)
    orderValidity = fields.Str(load_default=None)
    productType = fields.Str(load_default=None)
    masterAccounts = fields.List(fields.Str(), load_default=None)
    transactionType = fields.Str(load_default=None)
    orderQty = fields.Int(load_default=None)
    tradingSymbols = fields.List(fields.Str(), load_default=None)
    instrument_type = fields.Str(load_default="EQ")
    expiry = fields.Str(load_default=None)
    strike = fields.Int(load_default=None)
    option_type = fields.Str(load_default=None)
    lot_size = fields.Int(load_default=None)

    @pre_load
    def normalize(self, data: Dict[str, Any], **_: Any) -> Dict[str, Any]:
        """Normalise alternate field names before validation."""

        # Accept either ``ticker`` or ``symbol`` as the symbol field
        if "symbol" not in data and "ticker" in data:
            data["symbol"] = data["ticker"]
        data.pop("ticker", None)

        # Accept TradingView array ``tradingSymbols`` as the symbol field
        if "symbol" not in data and "tradingSymbols" in data:
            ts = data.get("tradingSymbols")
            if isinstance(ts, list) and ts:
                data["symbol"] = ts[0]

        # Accept ``transactionType`` as an alias for ``action``
        if "action" not in data and "transactionType" in data:
            data["action"] = data["transactionType"]

        # Accept ``orderQty`` as an alias for ``qty``
        if "qty" not in data and "orderQty" in data:
            data["qty"] = data["orderQty"]

        # Accept camelCase ``orderType`` for ``order_type``
        if "order_type" not in data and "orderType" in data:
            data["order_type"] = data["orderType"]

        # Accept ``quantity`` or ``qty`` for the quantity field
        if "qty" not in data and "quantity" in data:
            data["qty"] = data["quantity"]
        data.pop("quantity", None)

        # Accept ``side`` as an alias for ``action``
        if "action" not in data and "side" in data:
            data["action"] = data["side"]
        data.pop("side", None)

        # Upper-case the action for consistency
        if "action" in data and isinstance(data["action"], str):
            data["action"] = data["action"].upper()

        # Upper-case broker-specific fields
        for key in ["productType", "orderValidity", "order_type", "instrument_type", "option_type"]:
            if key in data and isinstance(data[key], str):
                data[key] = data[key].upper()

        # Enhanced symbol normalization
        if "symbol" in data and isinstance(data["symbol"], str):
            raw_sym = data["symbol"].strip().upper()
            
            logger.info(f"Processing symbol: {raw_sym}")
            
            # Normalize F&O symbol and extract metadata
            normalized_symbol, metadata = normalize_fo_symbol(raw_sym)
            
            if normalized_symbol != raw_sym:
                logger.info(f"Normalized symbol from '{raw_sym}' to '{normalized_symbol}'")
                data["symbol"] = normalized_symbol
                
                # Apply metadata to the data
                if metadata:
                    # Set exchange based on instrument type
                    if metadata.get('instrument_type') in ['FUTIDX', 'FUTSTK', 'OPTIDX', 'OPTSTK']:
                        data["exchange"] = "NFO"  # F&O trades on NFO
                    elif metadata.get('instrument_type') == 'EQ':
                        data["exchange"] = "NSE"  # Default equity exchange
                    
                    # Set instrument type
                    if "instrument_type" not in data or data["instrument_type"] == "EQ":
                        data["instrument_type"] = metadata.get('instrument_type', 'EQ')
                    
                    # Set strike and option type for options
                    if metadata.get('strike'):
                        data["strike"] = metadata['strike']
                    if metadata.get('option_type'):
                        data["option_type"] = metadata['option_type']
                    
                    # Set expiry information
                    if metadata.get('expiry_month') and metadata.get('expiry_year'):
                        expiry_str = f"{metadata['expiry_month']}{metadata['expiry_year']}"
                        if metadata.get('expiry_day'):
                            expiry_str = f"{metadata['expiry_day']}{expiry_str}"
                        data["expiry"] = expiry_str
                    
                    # CRITICAL: Set lot size from symbol map
                    if metadata.get('lot_size'):
                        data["lot_size"] = metadata['lot_size']
                        logger.info(f"Set lot size to {metadata['lot_size']} for symbol {normalized_symbol}")
            else:
                logger.info(f"Symbol already normalized or no normalization needed: {raw_sym}")
                # Even if not normalized, try to get lot size for F&O symbols
                if re.search(r'(FUT|CE|PE)$', raw_sym):
                    lot_size = get_lot_size_from_symbol_map(raw_sym, data.get("exchange", "NFO"))
                    if lot_size:
                        data["lot_size"] = lot_size
                        logger.info(f"Found lot size {lot_size} for existing symbol {raw_sym}")
        
        return data


def enqueue_webhook(
    user_id: int,
    strategy_id: Optional[int],
    payload: Dict[str, Any],
    stream: str = "webhook_events",
    none_placeholder: str = "",
) -> Dict[str, Any]:
    """Validate *payload* and publish it to *stream*.

    Args:
        user_id: Identifier of the user receiving the webhook.
        strategy_id: Optional strategy identifier.
        payload: Raw webhook payload received from the HTTP request.
        stream: Redis Stream name to publish to.
        none_placeholder: Substitute value for ``None`` fields.

    Returns:
        The validated event dictionary.

    Raises:
        ValidationError: If the payload does not conform to schema.
        redis.RedisError: If the event could not be published.
    """

    event = dict(payload)
    event["user_id"] = user_id
    event["strategy_id"] = strategy_id

    schema = WebhookEventSchema()
    validated = schema.load(event)
    if not validated.get("alert_id"):
        validated["alert_id"] = uuid.uuid4().hex

    logger.info(
        "Received alert %s payload=%s",
        validated.get("alert_id"),
        json.dumps(validated, separators=(",", ":")),
    )
    
    # Run duplicate and risk checks before publishing
    check_duplicate_and_risk(validated)

    # Serialize event to the Redis Stream
    sanitized: Dict[str, Any] = {}
    for k, v in validated.items():
        if v is None:
            sanitized[k] = none_placeholder
        elif isinstance(v, (str, int, float, bytes)):
            sanitized[k] = v
        else:
            sanitized[k] = json.dumps(v, separators=(",", ":"))
    try:
        client = get_redis_client()
        client.xadd(stream, sanitized)
    except redis.exceptions.RedisError:
        logger.exception("Failed to publish webhook event to Redis")
        raise

    return validated


__all__ = [
    "enqueue_webhook",
    "WebhookEventSchema",
    "get_redis_client",
    "redis_client",
    "ValidationError",
    "normalize_fo_symbol",
    "get_expiry_year",
    "get_lot_size_from_symbol_map",
],
        sym
    )
    if special_pattern:
        root = special_pattern.group(1)
        day = int(special_pattern.group(2))
        month = special_pattern.group(3)
        strike = special_pattern.group(4)
        opt_type = special_pattern.group(5)
        
        year = get_expiry_year(month, day)
        full_year = f"20{year}"
        opt_code = "CE" if opt_type in ("CALL", "CE") else "PE"
        
        normalized = f"{root}-{month.title()}{full_year}-{strike}-{opt_code}"
        metadata = {
            'underlying': root,
            'expiry_month': month,
            'expiry_year': full_year,
            'expiry_day': day,
            'strike': int(strike),
            'option_type': opt_code,
            'instrument_type': 'OPTIDX' if 'NIFTY' in root else 'OPTSTK'
        }
        # Get lot size from symbol map
        lot_size = get_lot_size_from_symbol_map(normalized, "NFO")
        if lot_size:
            metadata['lot_size'] = lot_size
        
        return normalized, metadata
    
    # Pattern 3: Futures with day: "FINNIFTY 30 SEP FUT"
    fut_with_day = re.match(
        r'^([A-Z]+)\s+(\d{1,2})\s+(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+FUT$',
        sym
    )
    if fut_with_day:
        root = fut_with_day.group(1)
        day = int(fut_with_day.group(2))
        month = fut_with_day.group(3)
        
        year = get_expiry_year(month, day)
        full_year = f"20{year}"
        normalized = f"{root}-{month.title()}{full_year}-FUT"
        metadata = {
            'underlying': root,
            'expiry_month': month,
            'expiry_year': full_year,
            'expiry_day': day,
            'instrument_type': 'FUTIDX' if 'NIFTY' in root else 'FUTSTK'
        }
        # Get lot size from symbol map
        lot_size = get_lot_size_from_symbol_map(normalized, "NFO")
        if lot_size:
            metadata['lot_size'] = lot_size
        
        return normalized, metadata
    
    # Pattern 4: Compact options with year: FINNIFTY25SEP33300CE, NIFTYNXT5025NOV35500CALL
    compact_opt_match = re.match(
        r'^(.+?)(\d{2})?(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d+)(CALL|PUT|CE|PE)$',
        sym
    )
    if compact_opt_match:
        root, year, month, strike, opt_type = compact_opt_match.groups()
        if not year:
            year = get_expiry_year(month)
        full_year = f"20{year}"
        opt_code = "CE" if opt_type in ("CALL", "CE") else "PE"
        
        normalized = f"{root}-{month.title()}{full_year}-{strike}-{opt_code}"
        metadata = {
            'underlying': root,
            'expiry_month': month,
            'expiry_year': full_year,
            'strike': int(strike),
            'option_type': opt_code,
            'instrument_type': 'OPTIDX' if 'NIFTY' in root else 'OPTSTK'
        }
        # Get lot size from symbol map
        lot_size = get_lot_size_from_symbol_map(normalized, "NFO")
        if lot_size:
            metadata['lot_size'] = lot_size
        
        return normalized, metadata
    
    # Pattern 5: Compact futures: FINNIFTY25SEPFUT
    compact_fut_match = re.match(
        r'^(.+?)(\d{2})?(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)FUT$',
        sym
    )
    if compact_fut_match:
        root, year, month = compact_fut_match.groups()
        if not year:
            year = get_expiry_year(month)
        full_year = f"20{year}"
        
        normalized = f"{root}-{month.title()}{full_year}-FUT"
        metadata = {
            'underlying': root,
            'expiry_month': month,
            'expiry_year': full_year,
            'instrument_type': 'FUTIDX' if 'NIFTY' in root else 'FUTSTK'
        }
        # Get lot size from symbol map
        lot_size = get_lot_size_from_symbol_map(normalized, "NFO")
        if lot_size:
            metadata['lot_size'] = lot_size
        
        return normalized, metadata
    
    # Pattern 6: Spaced options: "FINNIFTY 25 SEP 33300 CALL"
    spaced_opt_match = re.match(
        r'^([A-Z]+)\s+(?:(\d{2})\s+)?(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+(\d+)\s+(CALL|PUT|CE|PE)$',
        sym
    )
    if spaced_opt_match:
        root, year, month, strike, opt_type = spaced_opt_match.groups()
        if not year:
            year = get_expiry_year(month)
        full_year = f"20{year}"
        opt_code = "CE" if opt_type in ("CALL", "CE") else "PE"
        
        normalized = f"{root}-{month.title()}{full_year}-{strike}-{opt_code}"
        metadata = {
            'underlying': root,
            'expiry_month': month,
            'expiry_year': full_year,
            'strike': int(strike),
            'option_type': opt_code,
            'instrument_type': 'OPTIDX' if 'NIFTY' in root else 'OPTSTK'
        }
        # Get lot size from symbol map
        lot_size = get_lot_size_from_symbol_map(normalized, "NFO")
        if lot_size:
            metadata['lot_size'] = lot_size
        
        return normalized, metadata
    
    # Pattern 7: Spaced futures: "FINNIFTY 25 SEP FUT"
    spaced_fut_match = re.match(
        r'^([A-Z]+)\s+(?:(\d{2})\s+)?(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+FUT$',
        sym
    )
    if spaced_fut_match:
        root, year, month = spaced_fut_match.groups()
        if not year:
            year = get_expiry_year(month)
        full_year = f"20{year}"
        
        normalized = f"{root}-{month.title()}{full_year}-FUT"
        metadata = {
            'underlying': root,
            'expiry_month': month,
            'expiry_year': full_year,
            'instrument_type': 'FUTIDX' if 'NIFTY' in root else 'FUTSTK'
        }
        # Get lot size from symbol map
        lot_size = get_lot_size_from_symbol_map(normalized, "NFO")
        if lot_size:
            metadata['lot_size'] = lot_size
        
        return normalized, metadata
    
    # Pattern 8: Already normalized derivative symbols (compact format)
    if re.match(r'^[A-Z]+\d{2}(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d+)?(FUT|CE|PE)$', sym):
        # Extract components for metadata
        opt_match = re.match(r'^(.+?)(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d+)(CE|PE)$', sym)
        if opt_match:
            root, year, month, strike, opt_type = opt_match.groups()
            metadata = {
                'underlying': root,
                'expiry_month': month,
                'expiry_year': f"20{year}",
                'strike': int(strike),
                'option_type': opt_type,
                'instrument_type': 'OPTIDX' if 'NIFTY' in root else 'OPTSTK'
            }
            # Convert to Dhan format and get lot size
            normalized_dhan = f"{root}-{month.title()}20{year}-{strike}-{opt_type}"
            lot_size = get_lot_size_from_symbol_map(normalized_dhan, "NFO")
            if lot_size:
                metadata['lot_size'] = lot_size
        
        fut_match = re.match(r'^(.+?)(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)FUT$', sym)
        if fut_match:
            root, year, month = fut_match.groups()
            metadata = {
                'underlying': root,
                'expiry_month': month,
                'expiry_year': f"20{year}",
                'instrument_type': 'FUTIDX' if 'NIFTY' in root else 'FUTSTK'
            }
            # Convert to Dhan format and get lot size
            normalized_dhan = f"{root}-{month.title()}20{year}-FUT"
            lot_size = get_lot_size_from_symbol_map(normalized_dhan, "NFO")
            if lot_size:
                metadata['lot_size'] = lot_size
        
        return sym, metadata
    
    # Pattern 9: Equity symbols
    if not re.search(r'(FUT|CE|PE)$', sym) and not sym.endswith('-EQ'):
        if not re.search(r'\d', sym) or re.match(r'^[A-Z]+\d+$', sym):
            normalized = f"{sym}-EQ"
            metadata = {
                'underlying': sym,
                'instrument_type': 'EQ'
            }
            # Get lot size from symbol map (usually 1 for equity)
            lot_size = get_lot_size_from_symbol_map(normalized, "NSE")
            if lot_size:
                metadata['lot_size'] = lot_size
            else:
                metadata['lot_size'] = 1  # Default for equity
            
            return normalized, metadata
    
    # Return original if no pattern matches
    return sym, metadata


class WebhookEventSchema(Schema):
    """Schema for validating webhook events."""

    user_id = fields.Int(required=True)
    strategy_id = fields.Int(allow_none=True)
    symbol = fields.Str(required=True)
    action = fields.Str(required=True)
    qty = fields.Int(required=True)
    exchange = fields.Str(load_default="NSE")
    order_type = fields.Str(load_default=None)
    alert_id = fields.Str(load_default=None)
    # Broker specific fields
    orderType = fields.Str(load_default=None)
    orderValidity = fields.Str(load_default=None)
    productType = fields.Str(load_default=None)
    masterAccounts = fields.List(fields.Str(), load_default=None)
    transactionType = fields.Str(load_default=None)
    orderQty = fields.Int(load_default=None)
    tradingSymbols = fields.List(fields.Str(), load_default=None)
    instrument_type = fields.Str(load_default="EQ")
    expiry = fields.Str(load_default=None)
    strike = fields.Int(load_default=None)
    option_type = fields.Str(load_default=None)
    lot_size = fields.Int(load_default=None)

    @pre_load
    def normalize(self, data: Dict[str, Any], **_: Any) -> Dict[str, Any]:
        """Normalise alternate field names before validation."""

        # Accept either ``ticker`` or ``symbol`` as the symbol field
        if "symbol" not in data and "ticker" in data:
            data["symbol"] = data["ticker"]
        data.pop("ticker", None)

        # Accept TradingView array ``tradingSymbols`` as the symbol field
        if "symbol" not in data and "tradingSymbols" in data:
            ts = data.get("tradingSymbols")
            if isinstance(ts, list) and ts:
                data["symbol"] = ts[0]

        # Accept ``transactionType`` as an alias for ``action``
        if "action" not in data and "transactionType" in data:
            data["action"] = data["transactionType"]

        # Accept ``orderQty`` as an alias for ``qty``
        if "qty" not in data and "orderQty" in data:
            data["qty"] = data["orderQty"]

        # Accept camelCase ``orderType`` for ``order_type``
        if "order_type" not in data and "orderType" in data:
            data["order_type"] = data["orderType"]

        # Accept ``quantity`` or ``qty`` for the quantity field
        if "qty" not in data and "quantity" in data:
            data["qty"] = data["quantity"]
        data.pop("quantity", None)

        # Accept ``side`` as an alias for ``action``
        if "action" not in data and "side" in data:
            data["action"] = data["side"]
        data.pop("side", None)

        # Upper-case the action for consistency
        if "action" in data and isinstance(data["action"], str):
            data["action"] = data["action"].upper()

        # Upper-case broker-specific fields
        for key in ["productType", "orderValidity", "order_type", "instrument_type", "option_type"]:
            if key in data and isinstance(data[key], str):
                data[key] = data[key].upper()

        # Enhanced symbol normalization
        if "symbol" in data and isinstance(data["symbol"], str):
            raw_sym = data["symbol"].strip().upper()
            
            logger.info(f"Processing symbol: {raw_sym}")
            
            # Normalize F&O symbol and extract metadata
            normalized_symbol, metadata = normalize_fo_symbol(raw_sym)
            
            if normalized_symbol != raw_sym:
                logger.info(f"Normalized symbol from '{raw_sym}' to '{normalized_symbol}'")
                data["symbol"] = normalized_symbol
                
                # Apply metadata to the data
                if metadata:
                    # Set exchange based on instrument type
                    if metadata.get('instrument_type') in ['FUTIDX', 'FUTSTK', 'OPTIDX', 'OPTSTK']:
                        data["exchange"] = "NFO"  # F&O trades on NFO
                    elif metadata.get('instrument_type') == 'EQ':
                        data["exchange"] = "NSE"  # Default equity exchange
                    
                    # Set instrument type
                    if "instrument_type" not in data or data["instrument_type"] == "EQ":
                        data["instrument_type"] = metadata.get('instrument_type', 'EQ')
                    
                    # Set strike and option type for options
                    if metadata.get('strike'):
                        data["strike"] = metadata['strike']
                    if metadata.get('option_type'):
                        data["option_type"] = metadata['option_type']
                    
                    # Set expiry information
                    if metadata.get('expiry_month') and metadata.get('expiry_year'):
                        expiry_str = f"{metadata['expiry_month']}{metadata['expiry_year']}"
                        if metadata.get('expiry_day'):
                            expiry_str = f"{metadata['expiry_day']}{expiry_str}"
                        data["expiry"] = expiry_str
                    
                    # CRITICAL: Set lot size from symbol map
                    if metadata.get('lot_size'):
                        data["lot_size"] = metadata['lot_size']
                        logger.info(f"Set lot size to {metadata['lot_size']} for symbol {normalized_symbol}")
            else:
                logger.info(f"Symbol already normalized or no normalization needed: {raw_sym}")
                # Even if not normalized, try to get lot size for F&O symbols
                if re.search(r'(FUT|CE|PE)$', raw_sym):
                    lot_size = get_lot_size_from_symbol_map(raw_sym, data.get("exchange", "NFO"))
                    if lot_size:
                        data["lot_size"] = lot_size
                        logger.info(f"Found lot size {lot_size} for existing symbol {raw_sym}")
        
        return data


def enqueue_webhook(
    user_id: int,
    strategy_id: Optional[int],
    payload: Dict[str, Any],
    stream: str = "webhook_events",
    none_placeholder: str = "",
) -> Dict[str, Any]:
    """Validate *payload* and publish it to *stream*.

    Args:
        user_id: Identifier of the user receiving the webhook.
        strategy_id: Optional strategy identifier.
        payload: Raw webhook payload received from the HTTP request.
        stream: Redis Stream name to publish to.
        none_placeholder: Substitute value for ``None`` fields.

    Returns:
        The validated event dictionary.

    Raises:
        ValidationError: If the payload does not conform to schema.
        redis.RedisError: If the event could not be published.
    """

    event = dict(payload)
    event["user_id"] = user_id
    event["strategy_id"] = strategy_id

    schema = WebhookEventSchema()
    validated = schema.load(event)
    if not validated.get("alert_id"):
        validated["alert_id"] = uuid.uuid4().hex

    logger.info(
        "Received alert %s payload=%s",
        validated.get("alert_id"),
        json.dumps(validated, separators=(",", ":")),
    )
    
    # Run duplicate and risk checks before publishing
    check_duplicate_and_risk(validated)

    # Serialize event to the Redis Stream
    sanitized: Dict[str, Any] = {}
    for k, v in validated.items():
        if v is None:
            sanitized[k] = none_placeholder
        elif isinstance(v, (str, int, float, bytes)):
            sanitized[k] = v
        else:
            sanitized[k] = json.dumps(v, separators=(",", ":"))
    try:
        client = get_redis_client()
        client.xadd(stream, sanitized)
    except redis.exceptions.RedisError:
        logger.exception("Failed to publish webhook event to Redis")
        raise

    return validated


__all__ = [
    "enqueue_webhook",
    "WebhookEventSchema",
    "get_redis_client",
    "redis_client",
    "ValidationError",
    "normalize_fo_symbol",
    "get_expiry_year",
    "get_lot_size_from_symbol_map",
]
