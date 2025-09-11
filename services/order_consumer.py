from __future__ import annotations

"""Asynchronous worker that consumes webhook events and places orders."""

import asyncio
import json
import logging
import os
import re
import time
import inspect
from concurrent.futures import ThreadPoolExecutor, wait
from functools import partial
from typing import Any, Dict, Iterable, List

from prometheus_client import Counter

from brokers.factory import get_broker_client
from brokers import symbol_map
import redis

from .alert_guard import check_risk_limits, get_user_settings
from .webhook_receiver import redis_client, get_redis_client
from .utils import _decode_event
from .db import get_session
from models import Strategy, Account
from .master_trade_monitor import COMPLETED_STATUSES

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

REJECTED_STATUSES = {"REJECTED", "CANCELLED", "CANCELED", "FAILED"}

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

    # Lazily create a Redis client if one was not supplied.
    if redis_client is None:
        redis_client = get_redis_client()

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

            def _normalize_keys(data: Dict[str, Any]) -> Dict[str, Any]:
                """Return a dict with camelCase keys converted to snake_case."""
                normalized: Dict[str, Any] = {}
                for key, value in data.items():
                    new_key = re.sub(r"([A-Z])", lambda m: "_" + m.group(1).lower(), key)
                    normalized[new_key] = value
                return normalized

            def submit(broker_cfg: Dict[str, Any]) -> Dict[str, Any]:
                client_cls = get_broker_client(broker_cfg["name"])
                credentials = _normalize_keys(dict(broker_cfg))
                access_token = credentials.pop("access_token", "")
                client_id = credentials.pop("client_id", None)
                credentials.pop("name", None)
                # Validate that all required credentials are present before
                # instantiating the broker client.  This avoids cryptic
                # ``TypeError`` exceptions when mandatory parameters like
                # ``api_key`` are missing from the configuration.
                try:
                    sig = inspect.signature(client_cls)
                except (TypeError, ValueError):
                    sig = None
                if sig is not None:
                    required = [
                        p.name
                        for p in sig.parameters.values()
                        if p.kind
                        in (
                            inspect.Parameter.POSITIONAL_OR_KEYWORD,
                            inspect.Parameter.KEYWORD_ONLY,
                        )
                        and p.default is inspect._empty
                        and p.name not in ("self", "client_id", "access_token")
                    ]
                    missing = [p for p in required if p not in credentials]
                    if missing:
                        raise ValueError(
                            "missing required broker credential(s): "
                            + ", ".join(missing)
                        )

                client = client_cls(
                    client_id=client_id,
                    access_token=access_token,
                    **credentials,
                )
                order_params = {
                    "symbol": event["symbol"],
                    "action": event["action"],
                    "qty": event["qty"],
                }
                instrument_type = event.get("instrument_type")
                symbol = str(event.get("symbol", ""))
                inst_upper = instrument_type.upper() if instrument_type else ""
                sym_upper = symbol.upper()
                is_derivative = bool(re.search(r"(FUT|CE|PE)$", inst_upper)) or bool(
                    re.search(r"(FUT|CE|PE)$", sym_upper)
                )
                exchange = event.get("exchange")
                if event.get("security_id") is not None:
                    order_params["security_id"] = event["security_id"]
                if exchange is not None:
                    if exchange in {"NSE", "BSE"} and is_derivative:
                        exchange = {"NSE": "NFO", "BSE": "BFO"}[exchange]
                    order_params["exchange"] = exchange
                if event.get("order_type") is not None:
                    order_params["order_type"] = event["order_type"]
                for field in [
                    "instrument_type",
                    "expiry",
                    "strike",
                    "option_type",
                    "lot_size",
                ]:
                    if event.get(field) is not None:
                        order_params[field] = event[field]
                optional_map = {
                    "productType": "product_type",
                    "orderValidity": "validity",
                    "masterAccounts": "master_accounts",
                    "securityId": "security_id",
                }
                for src, dest in optional_map.items():
                    if event.get(src) is not None:
                        order_params[dest] = event[src]
                if is_derivative:
                    lot_size = event.get("lot_size")
                    if lot_size is None:
                        mapping = symbol_map.get_symbol_for_broker(
                            event.get("symbol", ""), broker_cfg["name"], exchange
                        )
                        lot_size = mapping.get("lot_size") or mapping.get("lotSize")
                    if not lot_size:
                        log.error(
                            "unable to resolve lot size for %s (%s) on broker %s",
                            event.get("symbol"),
                            exchange,
                            broker_cfg["name"],
                            extra={"event": event, "broker": broker_cfg},
                        )
                        return None
                    order_params["qty"] = int(event["qty"]) * int(lot_size)
                    if "lot_size" not in order_params:
                        order_params["lot_size"] = lot_size
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
                status = None
                try:
                    if hasattr(client, "get_order"):
                        info = client.get_order(order_id)
                        if isinstance(info, dict):
                            status = info.get("status") or info.get("data", {}).get("status")
                    elif hasattr(client, "list_orders"):
                        orders = client.list_orders()
                        for order in orders:
                            oid = (
                                order.get("id")
                                or order.get("order_id")
                                or order.get("orderId")
                                or order.get("data", {}).get("order_id")
                            )
                            if str(oid) == str(order_id):
                                status = order.get("status") or order.get("data", {}).get("status")
                                break
                except Exception:
                    log.warning(
                        "failed to fetch order status",
                        extra={"order_id": order_id},
                        exc_info=True,
                    )
                status_upper = str(status).upper() if status is not None else None
                if status_upper in REJECTED_STATUSES:
                    log.info(
                        "skipping trade event due to rejected status",
                        extra={"order_id": order_id, "status": status},
                    )
                    return None
                if status_upper not in COMPLETED_STATUSES:
                    log.info(
                        "publishing trade event with incomplete status",
                        extra={"order_id": order_id, "status": status},
                    )
                    
                trade_event = {
                    "master_id": client_id,
                    **{k: v for k, v in order_params.items() if k != "master_accounts"},
                }
                return trade_event


            def _late_completion(cfg, fut):
                try:
                    result = fut.result()
                    log.warning(
                        "broker %s completed after timeout: %s",
                        cfg["name"],
                        result,
                    )
                except BaseException as exc:
                    log.warning(
                        "broker %s failed after timeout: %s",
                        cfg["name"],
                        exc,
                        exc_info=exc,
                    )

            trade_events: List[Dict[str, Any]] = []
            if brokers:
                futures = {executor.submit(submit, cfg): cfg for cfg in brokers}
                done, pending = wait(futures, timeout=order_timeout)
                timed_out = []
                for future in done:
                    cfg = futures[future]
                    try:
                        trade_event = future.result()
                        if trade_event:
                            trade_events.append(trade_event)
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
                    if not future.cancel():
                        timed_out.append((future, cfg))
                for fut, cfg in timed_out:
                    fut.add_done_callback(partial(_late_completion, cfg))
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
