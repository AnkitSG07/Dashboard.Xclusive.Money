from __future__ import annotations

"""Asynchronous worker that consumes webhook events and places orders."""

import logging
from typing import Any, Dict, Iterable

from prometheus_client import Counter

from brokers.factory import get_broker_client

from .alert_guard import check_duplicate_and_risk, USER_SETTINGS
from .webhook_receiver import redis_client


log = logging.getLogger(__name__)

orders_success = Counter(
    "order_consumer_success_total",
    "Number of webhook events processed successfully",
)
orders_failed = Counter(
    "order_consumer_failure_total",
    "Number of webhook events that failed processing",
)


def _decode_event(raw: Dict[Any, Any]) -> Dict[str, Any]:
    """Decode Redis bytes into a plain ``dict``."""

    event: Dict[str, Any] = {}
    for k, v in raw.items():
        key = k.decode() if isinstance(k, bytes) else k
        if isinstance(v, bytes):
            try:
                v = int(v)
            except ValueError:
                v = v.decode()
        event[key] = v
    return event


def consume_webhook_events(
    *,
    stream: str = "webhook_events",
    group: str = "order_consumer",
    consumer: str = "worker-1",
    redis_client=redis_client,
    max_messages: int | None = None,
    block: int = 0,
) -> int:
    """Consume events from *stream* using a consumer group and place orders.

    Args:
        stream: Redis Stream name to consume from.
        group: Consumer group name used for coordinated consumption.
        consumer: Consumer name within the group.
        redis_client: Redis client instance.  Tests may supply a stub.
        max_messages: Optional limit for the number of messages processed.  If
            ``None`` the consumer runs until the stream is exhausted.
        block: Milliseconds to block waiting for new events.  ``0`` means do not
            block.

    Returns the number of messages processed.
    """

    # Ensure the consumer group exists. Ignore error if it already exists.
    try:
        redis_client.xgroup_create(stream, group, id="0", mkstream=True)
    except Exception as exc:  # pragma: no cover - stub may not raise
        if "BUSYGROUP" not in str(exc):
            raise

    processed = 0
    while max_messages is None or processed < max_messages:
        messages: Iterable = redis_client.xreadgroup(
            group, consumer, {stream: ">"}, count=1, block=block
        )
        if not messages:
            break
        for _stream, events in messages:
            for msg_id, data in events:
                event = _decode_event(data)
                try:
                    check_duplicate_and_risk(event)
                    settings = USER_SETTINGS.get(event["user_id"], {})
                    brokers = settings.get("brokers", [])
                    for broker_cfg in brokers:
                        client_cls = get_broker_client(broker_cfg["name"])
                        client = client_cls(
                            broker_cfg.get("client_id"),
                            broker_cfg.get("access_token", ""),
                            **broker_cfg.get("extras", {})
                        )
                        client.place_order(
                            symbol=event["symbol"],
                            action=event["action"],
                            qty=event["qty"],
                            exchange=event.get("exchange"),
                            order_type=event.get("order_type"),
                        )

                        # Publish master order to trade copier stream so child
                        # accounts can replicate the trade asynchronously.
                        redis_client.xadd(
                            "trade_events",
                            {
                                "master_id": broker_cfg.get("client_id"),
                                "symbol": event["symbol"],
                                "action": event["action"],
                                "qty": event["qty"],
                                "exchange": event.get("exchange"),
                                "order_type": event.get("order_type"),
                            },
                        )
                    orders_success.inc()
                    log.info("processed webhook event", extra={"event": event})
                except Exception:
                    orders_failed.inc()
                    log.exception(
                        "failed to process webhook event", extra={"event": event}
                    )
                redis_client.xack(stream, group, msg_id)
                processed += 1
                if max_messages is not None and processed >= max_messages:
                    break
    return processed


__all__ = [
    "consume_webhook_events",
    "orders_success",
    "orders_failed",
]
