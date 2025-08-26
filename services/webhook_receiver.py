"""Webhook receiver service.

This module provides minimal validation and serialization of webhook
payloads and publishes validated events to a low-latency queue. Redis
Streams is used as the backing queue so downstream workers can consume
events asynchronously.

The expected event schema is documented in ``docs/webhook_events.md``.
"""

from __future__ import annotations

import os
from collections import deque
from typing import Optional, Dict, Any, Deque

import redis
from marshmallow import Schema, fields, ValidationError, pre_load

from .alert_guard import check_duplicate_and_risk


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Redis client used for publishing events. In tests this object can be
# monkeypatched with a stub that implements ``xadd``.
redis_client = redis.Redis.from_url(REDIS_URL)
# Fallback in-memory streams used when Redis is unavailable.
_LOCAL_STREAMS: Dict[str, Deque[Dict[str, Any]]] = {}

class WebhookEventSchema(Schema):
    """Schema for validating webhook events."""

    user_id = fields.Int(required=True)
    strategy_id = fields.Int(allow_none=True)
    symbol = fields.Str(required=True)
    action = fields.Str(required=True)
    qty = fields.Int(required=True)
    exchange = fields.Str(load_default=None)
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

    @pre_load
    def normalize(self, data: Dict[str, Any], **_: Any) -> Dict[str, Any]:
        """Normalise alternate field names before validation."""

        # Accept either ``ticker`` or ``symbol`` as the symbol field.
        if "symbol" not in data and "ticker" in data:
            data["symbol"] = data["ticker"]
        data.pop("ticker", None)

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

        return data


def enqueue_webhook(
    user_id: int,
    strategy_id: Optional[int],
    payload: Dict[str, Any],
    stream: str = "webhook_events",
) -> Dict[str, Any]:
    """Validate *payload* and publish it to *stream*.

    Args:
        user_id: Identifier of the user receiving the webhook.
        strategy_id: Optional strategy identifier associated with the
            webhook. ``None`` if not provided.
        payload: Raw webhook payload received from the HTTP request.
        stream: Redis Stream name to publish to. Defaults to
            ``"webhook_events"``.

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

    # Run duplicate and risk checks before publishing.
    check_duplicate_and_risk(validated)

    # Serialize event to the Redis Stream. Redis expects a mapping of
    # field/value pairs. ``xadd`` returns the generated ID which we don't
    # use but keeping the call ensures the event is queued.
    try:
        redis_client.xadd(stream, validated)
    except redis.exceptions.RedisError:
        _LOCAL_STREAMS.setdefault(stream, deque()).append(validated)

    return validated


__all__ = [
    "enqueue_webhook",
    "WebhookEventSchema",
    "redis_client",
    "ValidationError",
    "_LOCAL_STREAMS",
]
