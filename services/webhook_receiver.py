"""Webhook receiver service - Fixed symbol normalization.

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

        if "exchange" in data and isinstance(data["exchange"], str):
            data["exchange"] = data["exchange"].upper()

        # Symbol normalization
        if "symbol" in data and isinstance(data["symbol"], str):
            raw_sym = data["symbol"].strip().upper()

            # Support symbols prefixed with an exchange (e.g. ``NSE:SBIN``)
            if ":" in raw_sym:
                exch, sym = raw_sym.split(":", 1)
                raw_sym = sym
                if not data.get("exchange"):
                    data["exchange"] = exch

            logger.info(f"Processing symbol: {raw_sym}")
            
            # Handle already correctly formatted equity symbols
            if raw_sym.endswith("-EQ"):
                data["symbol"] = raw_sym
                data.setdefault("exchange", "NSE")
                data["instrument_type"] = "EQ"
                logger.info(f"Equity symbol already formatted: {raw_sym}")
                return data
            
            # Special pattern for "FINNIFTY 30 SEP 33300 CALL" format
            # This matches: SYMBOL + DAY + MONTH + STRIKE + OPTION_TYPE
            special_pattern = re.match(
                r'^([A-Z0-9]+)\s+(\d{1,2})\s+(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+(\d+)\s+(CALL|PUT|CE|PE)$',
                raw_sym
            )
            if special_pattern:
                root = special_pattern.group(1)
                day = int(special_pattern.group(2))
                month = special_pattern.group(3)
                strike = special_pattern.group(4)
                opt_type = special_pattern.group(5)
                
                # Get the year based on month and day
                year = get_expiry_year(month, day)
                
                # Convert CALL/PUT to CE/PE
                opt_code = "CE" if opt_type in ("CALL", "CE") else "PE"
                
                # Standard format: ROOT + YY + MMM + STRIKE + CE/PE
                # Note: We're not including the day in the final symbol
                normalized_symbol = f"{root}{year}{month}{strike}{opt_code}"
                
                data["symbol"] = normalized_symbol
                data["exchange"] = "NFO"
                data["instrument_type"] = "OPTIDX" if "NIFTY" in root else "OPTSTK"
                data["strike"] = int(strike)
                data["option_type"] = opt_code
                data["expiry"] = f"{day}{month}{year}"  # Store full expiry info
                
                logger.info(f"Normalized special format from '{raw_sym}' to '{normalized_symbol}'")
                return data
            
            # Pattern for futures with day: "FINNIFTY 30 SEP FUT"
            fut_with_day = re.match(
                r'^([A-Z0-9]+)\s+(\d{1,2})\s+(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+FUT$',
                raw_sym
            )
            if fut_with_day:
                root = fut_with_day.group(1)
                day = int(fut_with_day.group(2))
                month = fut_with_day.group(3)
                
                year = get_expiry_year(month, day)
                normalized_symbol = f"{root}{year}{month}FUT"
                
                data["symbol"] = normalized_symbol
                data["exchange"] = "NFO"
                data["instrument_type"] = "FUTIDX" if "NIFTY" in root else "FUTSTK"
                data["expiry"] = f"{day}{month}{year}"
                
                logger.info(f"Normalized futures with day from '{raw_sym}' to '{normalized_symbol}'")
                return data
            
            # Compact formats without spaces
            # Options: FINNIFTY25SEP33300CE or FINNIFTY30SEP33300CE
            compact_opt_match = re.match(
                r'^([A-Z0-9]+?)(\d{2})?(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d+)(CALL|PUT|CE|PE)$',
                raw_sym
            )
            if compact_opt_match:
                root, year, month, strike, opt_type = compact_opt_match.groups()
                if not year:
                    year = get_expiry_year(month)
                opt_code = "CE" if opt_type in ("CALL", "CE") else "PE"
                normalized_symbol = f"{root}{year}{month}{strike}{opt_code}"
                data["symbol"] = normalized_symbol
                data["exchange"] = "NFO"
                data["instrument_type"] = "OPTIDX" if "NIFTY" in root else "OPTSTK"
                data["strike"] = int(strike)
                data["option_type"] = opt_code
                data["expiry"] = f"{year}{month}"
                logger.info(f"Normalized compact option: {normalized_symbol}")
                return data
            
            # Futures: FINNIFTY25SEPFUT or FINNIFTY30SEPFUT
            compact_fut_match = re.match(
                r'^([A-Z0-9]+?)(\d{2})?(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)FUT$',
                raw_sym
            )
            if compact_fut_match:
                root, year, month = compact_fut_match.groups()
                if not year:
                    year = get_expiry_year(month)
                normalized_symbol = f"{root}{year}{month}FUT"
                data["symbol"] = normalized_symbol
                data["exchange"] = "NFO"
                data["instrument_type"] = "FUTIDX" if "NIFTY" in root else "FUTSTK"
                data["expiry"] = f"{year}{month}"
                logger.info(f"Normalized compact futures: {normalized_symbol}")
                return data
            
            # Standard spaced formats (without day)
            # Options: "FINNIFTY 25 SEP 33300 CALL" or "FINNIFTY SEP 33300 CALL"
            spaced_opt_match = re.match(
                r'^([A-Z0-9]+)\s+(?:(\d{2})\s+)?(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+(\d+)\s+(CALL|PUT|CE|PE)$',
                raw_sym
            )
            if spaced_opt_match:
                root, year, month, strike, opt_type = spaced_opt_match.groups()
                if not year:
                    year = get_expiry_year(month)
                opt_code = "CE" if opt_type in ("CALL", "CE") else "PE"
                normalized_symbol = f"{root}{year}{month}{strike}{opt_code}"
                data["symbol"] = normalized_symbol
                data["exchange"] = "NFO"
                data["instrument_type"] = "OPTIDX" if "NIFTY" in root else "OPTSTK"
                data["strike"] = int(strike)
                data["option_type"] = opt_code
                data["expiry"] = f"{year}{month}"
                logger.info(f"Normalized spaced options: {normalized_symbol}")
                return data
            
            # Futures: "FINNIFTY 25 SEP FUT" or "FINNIFTY SEP FUT"
            spaced_fut_match = re.match(
                r'^([A-Z0-9]+)\s+(?:(\d{2})\s+)?(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+FUT$',
                raw_sym
            )
            if spaced_fut_match:
                root, year, month = spaced_fut_match.groups()
                if not year:
                    year = get_expiry_year(month)
                normalized_symbol = f"{root}{year}{month}FUT"
                data["symbol"] = normalized_symbol
                data["exchange"] = "NFO"
                data["instrument_type"] = "FUTIDX" if "NIFTY" in root else "FUTSTK"
                data["expiry"] = f"{year}{month}"
                logger.info(f"Normalized spaced futures: {normalized_symbol}")
                return data
            
            # Handle already normalized derivative symbols
            if re.match(r'^[A-Z0-9]+\d{2}(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d+)?(FUT|CE|PE)$', raw_sym):
                data["symbol"] = raw_sym
                data["exchange"] = "NFO"
                if raw_sym.endswith("FUT"):
                    data["instrument_type"] = "FUTIDX" if "NIFTY" in raw_sym else "FUTSTK"
                else:
                    data["instrument_type"] = "OPTIDX" if "NIFTY" in raw_sym else "OPTSTK"
                    if raw_sym.endswith(("CE", "PE")):
                        strike_match = re.search(r'(\d+)(CE|PE)$', raw_sym)
                        if strike_match:
                            data["strike"] = int(strike_match.group(1))
                            data["option_type"] = strike_match.group(2)
                logger.info(f"Symbol already normalized: {raw_sym}")
                return data
            
            # Pure equity symbols (no numbers, no derivative suffixes)
            if not re.search(r'\d', raw_sym) and not raw_sym.endswith(("FUT", "CE", "PE")):
                normalized_symbol = f"{raw_sym}-EQ"
                data["symbol"] = normalized_symbol
                data.setdefault("exchange", "NSE")
                data["instrument_type"] = "EQ"
                logger.info(f"Normalized equity symbol: {normalized_symbol}")
                return data
            
            # Symbols with numbers but not derivatives
            if not raw_sym.endswith(("FUT", "CE", "PE", "-EQ")) and not re.search(r'(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)', raw_sym):
                normalized_symbol = f"{raw_sym}-EQ"
                data["symbol"] = normalized_symbol
                data.setdefault("exchange", "NSE")
                data["instrument_type"] = "EQ"
                logger.info(f"Assumed equity symbol: {normalized_symbol}")
                return data
            
            # If no pattern matches, reject strings that look derivative-like
            if re.search(r"(FUT|CALL|PUT|CE|PE)", raw_sym):
                raise ValidationError(f"Unrecognized derivative symbol: {raw_sym}")

            # Otherwise keep the original symbol but log a warning
            data["symbol"] = raw_sym
            logger.warning(f"No normalization pattern matched for symbol: {raw_sym}")
            
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
]
