from __future__ import annotations

"""Asynchronous worker that consumes webhook events and places orders."""

import asyncio
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, wait
from typing import Any, Dict, Iterable, List

from prometheus_client import Counter

from brokers.factory import get_broker_client
import redis

from .alert_guard import check_risk_limits, get_user_settings
from .webhook_receiver import redis_client
from .utils import _decode_event
from .db import get_session
from models import Strategy, Account

log = logging.getLogger(__name__)

orders_success = Counter(
    "order_consumer_success_total",
    "Number of webhook events processed successfully",
)
orders_failed = Counter(
    "order_consumer_failure_total",
    "Number of webhook events that failed processing",
)


DEFAULT_MAX_WORKERS = int(os.getenv("ORDER_CONSUMER_MAX_WORKERS", "10"))


def consume_webhook_events(
    *,
    stream: str = "webhook_events",
    group: str = "order_consumer",
    consumer: str = "worker-1",
    redis_client=redis_client,
    max_messages: int | None = None,
    block: int = 0,
    batch_size: int = 10,
    max_workers: int | None = None,
    order_timeout: float | None = None,
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
        batch_size: Number of messages to fetch per ``xreadgroup`` call.
        max_workers: Maximum number of broker submissions dispatched
            concurrently for a single webhook event.  Defaults to the value of
            the ``ORDER_CONSUMER_MAX_WORKERS`` environment variable.
        order_timeout: Maximum time in seconds to wait for a broker API
            response. ``None`` disables the timeout.

    Returns the number of messages processed.
    """
    
    max_workers = max_workers or DEFAULT_MAX_WORKERS
    if order_timeout is None:
        # Wait slightly longer than broker HTTP calls so the worker doesn't
        # cancel orders prematurely.  Defaults to the broker timeout (25s) but
        # can be overridden via ``ORDER_CONSUMER_TIMEOUT``.
        order_timeout = float(
            os.getenv(
                "ORDER_CONSUMER_TIMEOUT",
                os.getenv("BROKER_TIMEOUT", "20"),
            )
        )

    # Ensure the consumer group exists. Ignore error if it already exists.
    try:
        redis_client.xgroup_create(stream, group, id="0", mkstream=True)
    except Exception as exc:  # pragma: no cover - stub may not raise
        if "BUSYGROUP" not in str(exc):
            raise

    executor = ThreadPoolExecutor(max_workers=max_workers)

    def process_message(msg_id: str, data: Dict[Any, Any]) -> None:
        event = _decode_event(data)
        try:
            check_risk_limits(event)
            settings = get_user_settings(event["user_id"])
            brokers = settings.get("brokers", [])

            allowed_accounts = event.get("masterAccounts") or []
            if isinstance(allowed_accounts, str):
                try:
                    allowed_accounts = json.loads(allowed_accounts)
                except json.JSONDecodeError:
                    log.error(
                        "invalid masterAccounts JSON: %s",
                        allowed_accounts,
                        extra={"event": event},
                    )
                    orders_failed.inc()
                    return
                if not isinstance(allowed_accounts, list):
                    log.error(
                        "masterAccounts JSON was not a list: %s",
                        allowed_accounts,
                        extra={"event": event},
                    )
                    orders_failed.inc()
                    return
                event["masterAccounts"] = allowed_accounts
            if not allowed_accounts:
                strategy_id = event.get("strategy_id")
                if strategy_id is not None:
                    session = get_session()
                    try:
                        strategy = session.query(Strategy).get(strategy_id)
                        if strategy and strategy.master_accounts:
                            allowed_accounts = [
                                a.strip()
                                for a in str(strategy.master_accounts).split(",")
                                if a.strip()
                            ]
                    finally:
                        session.close()
            if allowed_accounts:
                invalid_ids = [
                    acc_id for acc_id in allowed_accounts if not str(acc_id).isdigit()
                ]
                if invalid_ids:
                    log.error(
                        "non-numeric master account id(s): %s",
                        invalid_ids,
                        extra={"event": event},
                    )
                    orders_failed.inc()
                    return

                ids: List[int] = [int(acc_id) for acc_id in allowed_accounts]
                session = get_session()
                try:
                    rows = (
                        session.query(Account)
                        .filter(Account.id.in_(ids))
                        .all()
                    )
                finally:
                    session.close()
                allowed_pairs = {(r.broker, r.client_id) for r in rows}
                brokers = [
                    b
                    for b in brokers
                    if (b.get("name"), b.get("client_id")) in allowed_pairs
                ]

                if not brokers:
                    log.error(
                        "no brokers permitted for user", extra={"event": event}
                    )
                    orders_failed.inc()
                    return
            elif not brokers:
                log.error(
                    "no brokers configured for user", extra={"event": event}
                )
                orders_failed.inc()
                return
                
            def submit(broker_cfg: Dict[str, Any]) -> Dict[str, Any]:
                client_cls = get_broker_client(broker_cfg["name"])
                client = client_cls(
                    broker_cfg.get("client_id"),
                    broker_cfg.get("access_token", ""),
                    **broker_cfg.get("extras", {})
                )
                order_params = {
                    "symbol": event["symbol"],
                    "action": event["action"],
                    "qty": event["qty"],
                }
                if event.get("exchange") is not None:
                    order_params["exchange"] = event["exchange"]
                if event.get("order_type") is not None:
                    order_params["order_type"] = event["order_type"]
                optional_map = {
                    "productType": "product_type",
                    "orderValidity": "validity",
                    "masterAccounts": "master_accounts",
                }
                for src, dest in optional_map.items():
                    if event.get(src) is not None:
                        order_params[dest] = event[src]
                result = client.place_order(**order_params)
                order_id = None
                if isinstance(result, dict):
                    order_id = (
                        result.get("order_id")
                        or result.get("id")
                        or result.get("data", {}).get("order_id")
                        or result.get("orderId")
                        or result.get("data", {}).get("orderId")
                    )
                if not isinstance(result, dict) or result.get("status") != "success" or not order_id:
                    raise RuntimeError(f"broker order failed: {result}")
                trade_event = {
                    "master_id": broker_cfg.get("client_id"),
                    **{k: v for k, v in order_params.items() if k != "master_accounts"},
                }
                return trade_event
                
            trade_events: List[Dict[str, Any]] = []
            if brokers:
                futures = {executor.submit(submit, cfg): cfg for cfg in brokers}
                done, pending = wait(futures, timeout=order_timeout)
                for future in done:
                    cfg = futures[future]
                    try:
                        trade_events.append(future.result())
                    except Exception:
                        orders_failed.inc()
                        log.exception(
                            "failed to place master order", extra={"event": event, "broker": cfg}
                        )
                for future in pending:
                    cfg = futures[future]
                    orders_failed.inc()
                    log.warning(
                        "broker order timed out", extra={"event": event, "broker": cfg}
                    )
                    future.cancel()
            else:
                # Provide explicit feedback when no brokers are configured for a user.
                orders_failed.inc()
                log.warning("no brokers configured for user", extra={"event": event})
                
            for trade_event in trade_events:
                # Publish master order to trade copier stream so child
                # accounts can replicate the trade asynchronously.
                redis_client.xadd("trade_events", trade_event)

            if trade_events:
                orders_success.inc()
                log.info("processed webhook event", extra={"event": event})
            else:
                orders_failed.inc()
        except Exception:
            orders_failed.inc()
            log.exception("failed to process webhook event", extra={"event": event})
        finally:
            redis_client.xack(stream, group, msg_id)

    async def _consume() -> int:
        processed = 0
        while max_messages is None or processed < max_messages:
            count = batch_size
            if max_messages is not None:
                count = min(count, max_messages - processed)
            messages: Iterable = redis_client.xreadgroup(
                group, consumer, {stream: ">"}, count=count, block=block
            )
            if not messages:
                break

            tasks = []
            for _stream, events in messages:
                for msg_id, data in events:
                    tasks.append(asyncio.to_thread(process_message, msg_id, data))
            if tasks:
                await asyncio.gather(*tasks)
                processed += len(tasks)

        return processed

    try:
        return asyncio.run(_consume())
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


__all__ = [
    "consume_webhook_events",
    "orders_success",
    "orders_failed",
]

def main() -> None:
    """Run the consumer indefinitely.

    ``consume_webhook_events`` exits once the Redis stream is exhausted.
    Wrapping it in an endless loop ensures the worker process stays alive
    and continues to block for new webhook events.
    """

    while True:
        # Block for up to 5 seconds waiting for new events so the loop
        # doesn't spin when the stream is idle.
        try:
            consume_webhook_events(block=5000)
        except redis.exceptions.RedisError:
            log.exception("redis unavailable, retrying", exc_info=True)
            time.sleep(5)


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    try:
        main()
    except KeyboardInterrupt:  # pragma: no cover - interactive use
        pass
