"""Webhook receiver service.

This module provides minimal validation and serialization of webhook
payloads and publishes validated events to a low-latency queue. Redis
Streams is used as the backing queue so downstream workers can consume
events asynchronously.

The expected event schema is documented in ``docs/webhook_events.md``.
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

# Redis client used for publishing events. In tests this object can be
# monkeypatched with a stub that implements ``xadd``. It is initialised lazily
# so the module can be imported without a ``REDIS_URL`` being configured.
redis_client: Optional[redis.Redis] = None


def get_redis_client() -> redis.Redis:
    """Return a Redis client configured from ``REDIS_URL``.

    Raises:
        RuntimeError: If ``REDIS_URL`` is not set.
    """

    global redis_client
    if redis_client is None:
        redis_url = os.getenv("REDIS_URL")
        if not redis_url:
            msg = "REDIS_URL environment variable must be set to connect to Redis"
            logger.error(msg)
            raise RuntimeError(msg)
        redis_client = redis.Redis.from_url(redis_url)
    return redis_client

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
    # Broker specific fields expected by some strategies
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

        # Accept either ``ticker`` or ``symbol`` as the symbol field.
        if "symbol" not in data and "ticker" in data:
            data["symbol"] = data["ticker"]
        data.pop("ticker", None)

        # Accept TradingView array ``tradingSymbols`` as the symbol field.
        if "symbol" not in data and "tradingSymbols" in data:
            ts = data.get("tradingSymbols")
            if isinstance(ts, list) and ts:
                data["symbol"] = ts[0]

        # Accept ``transactionType`` as an alias for ``action``.
        if "action" not in data and "transactionType" in data:
            data["action"] = data["transactionType"]

        # Accept ``orderQty`` as an alias for ``qty``.
        if "qty" not in data and "orderQty" in data:
            data["qty"] = data["orderQty"]

        # Accept camelCase ``orderType`` for ``order_type``.
        if "order_type" not in data and "orderType" in data:
            data["order_type"] = data["orderType"]

        # Accept ``quantity`` or ``qty`` for the quantity field.
        if "qty" not in data and "quantity" in data:
            data["qty"] = data["quantity"]
        data.pop("quantity", None)

        # Accept ``side`` as an alias for ``action``.
        if "action" not in data and "side" in data:
            data["action"] = data["side"]
        data.pop("side", None)

        # Upper-case the action for consistency.
        if "action" in data and isinstance(data["action"], str):
            data["action"] = data["action"].upper()

        # Upper-case broker-specific fields expected in a canonical form.
        for key in ["productType", "orderValidity", "order_type", "instrument_type", "option_type"]:
            if key in data and isinstance(data[key], str):
                data[key] = data[key].upper()

        # Corrected: Normalise and upper-case the symbol to Dhan's format.
        if "symbol" in data and isinstance(data["symbol"], str):
            raw_sym = data["symbol"].strip().upper()
            
            # Match futures: NIFTYNXT50 25 NOV FUT
            fut_match = re.fullmatch(
                r"^([A-Z]+)\s*(\d{2})?\s*(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s*FUT$",
                raw_sym,
            )
            if fut_match:
                root, year, month = fut_match.groups()
                if not year:
                    year = str(date.today().year % 100)
                data["symbol"] = f"{root}{year}{month}FUT"
                data["exchange"] = "NFO"
                return data

            # Match options: NIFTYNXT50 25 NOV 35500 CALL
            opt_match = re.fullmatch(
                r"^([A-Z]+)\s*(\d{2})?\s*(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s*(\d+)\s*(CALL|PUT|CE|PE)$",
                raw_sym,
            )
            if opt_match:
                root, year, month, strike, opt_type = opt_match.groups()
                if not year:
                    year = str(date.today().year % 100)
                opt_code = "CE" if opt_type in ("CALL", "CE") else "PE"
                data["symbol"] = f"{root}{year}{month}{strike}{opt_code}"
                data["exchange"] = "NFO"
                return data

            # If no derivative pattern matches, assume it's an equity symbol
            if not re.search(r"\d", raw_sym) and not raw_sym.endswith(("-EQ", "FUT", "CE", "PE")):
                data["symbol"] = f"{raw_sym}-EQ"
            else:
                 data["symbol"] = raw_sym
            
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
        strategy_id: Optional strategy identifier associated with the
            webhook. ``None`` if not provided.
        payload: Raw webhook payload received from the HTTP request.
        stream: Redis Stream name to publish to. Defaults to
            ``"webhook_events"``.
        none_placeholder: Substitute value for ``None`` fields before
            publishing. Defaults to an empty string.

    Returns:
        The validated event dictionary.

    Raises:
        ValidationError: If the payload does not conform to the expected
            schema.
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
    
    # Run duplicate and risk checks before publishing.
    check_duplicate_and_risk(validated)

    # Serialize event to the Redis Stream.
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
