"""Trade copier service that consumes master order events from Redis.

This module listens to the ``trade_events`` Redis stream and replicates
orders from master accounts to their linked child accounts.  It no longer
relies on an in-memory queue or Flask application context, allowing it to be
run as an independent microservice.
"""

from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
import logging
import os
from functools import partial
from typing import Any, Callable, Dict, Iterable, Optional

from prometheus_client import Histogram
from sqlalchemy.orm import Session

from models import Account
from brokers.factory import get_broker_client
from .webhook_receiver import redis_client
from .utils import _decode_event
from helpers import active_children_for_master

LATENCY = Histogram(
    "trade_copier_latency_seconds", "Seconds spent processing a master order event"
)

log = logging.getLogger(__name__)

DEFAULT_MAX_WORKERS = 10
DEFAULT_CHILD_TIMEOUT = 5.0

def copy_order(master: Account, child: Account, order: Dict[str, Any]) -> Any:
    """Instantiate the appropriate broker client for *child* and copy *order*.

    Parameters
    ----------
    master: Account
        Master account that generated the trade. Currently unused but part of
        the public callback API.
    child: Account
        Child account that should receive the replicated order.
    order: Dict[str, Any]
        Normalised order payload decoded from the ``trade_events`` stream.
    """

    # Look up the concrete broker implementation or service client.
    client_cls = get_broker_client(child.broker)

    # Credentials are stored on the ``Account`` model as a JSON blob.  We only
    # require an access token for the tests so missing keys default to ``""``.
    credentials = child.credentials or {}
    access_token = credentials.get("access_token", "")
    extras = credentials.get("extras", {})

    broker = client_cls(child.client_id, access_token, **extras)

    # Apply quantity multiplier for the child account.  Default multiplier is
    # 1.0 if not specified.
    multiplier = getattr(child, "multiplier", 1) or 1
    qty = int(order.get("qty", 0))
    qty = int(qty * multiplier)

    params = {
        "symbol": order.get("symbol"),
        "action": order.get("action"),
        "qty": qty,
    }
    if order.get("exchange") is not None:
        params["exchange"] = order["exchange"]
    if order.get("order_type") is not None:
        params["order_type"] = order["order_type"]
    return broker.place_order(**params)

async def _replicate_to_children(
    db_session: Session,
    master: Account,
    order: Dict[str, Any],
    processor: Callable[[Account, Account, Dict[str, Any]], Any],
    *,
    executor: ThreadPoolExecutor | None = None,
    max_workers: int | None = None,
    timeout: float | None = None,
) -> None:
    """Execute ``processor`` concurrently for all active children.

    Parameters
    ----------
    executor:
        Optional pre-created :class:`ThreadPoolExecutor` to use for broker
        submissions.  If not supplied a new executor is created for the call.
    max_workers:
        Thread pool size when a new executor is created. Defaults to the
        number of child accounts.
    timeout:
        Maximum time in seconds to wait for each child broker call. ``None``
        disables the timeout.
    """

    try:
        children = active_children_for_master(master, db_session)
    except TypeError:
        children = active_children_for_master(master)

    if not children:
        return

    loop = asyncio.get_running_loop()
    own_executor = False
    if executor is None:
        max_workers = max_workers or len(children) or 1
        executor = ThreadPoolExecutor(max_workers=max_workers)
        own_executor = True

    def _late_completion(child, fut):
        client_id = getattr(child, "client_id", None)
        try:
            fut.result()
            log.warning(
                "child %s copy completed after timeout",
                client_id,
                extra={"child": client_id},
            )
        except Exception as exc:
            log.warning(
                "child %s copy failed after timeout",
                client_id,
                exc_info=exc,
                extra={"child": client_id, "error": repr(exc)},
            )
    
    try:
        async_tasks = []
        timed_out: list[tuple[Account, asyncio.Future]] = []
        for child in children:
            orig_fut = loop.run_in_executor(
                executor, processor, master, child, order
            )
            wrapped = asyncio.wait_for(orig_fut, timeout) if timeout is not None else orig_fut
            async_tasks.append((child, wrapped, orig_fut))

        results = await asyncio.gather(
            *(f for _c, f, _of in async_tasks), return_exceptions=True
        )

        for (child, _wrapped, orig_fut), result in zip(async_tasks, results):
            client_id = getattr(child, "client_id", None)
            if isinstance(result, asyncio.TimeoutError):
                cancelled = orig_fut.cancel()
                log.warning(
                    "child %s copy timed out; order may still have executed",
                    client_id,
                    extra={"child": client_id, "error": "TimeoutError"},
                )
                if not cancelled:
                    timed_out.append((child, orig_fut))
            elif isinstance(result, Exception):
                
                log.error(
                    "child %s copy failed",
                    client_id,
                    exc_info=result,
                    extra={"child": client_id, "error": repr(result)},
                )

        for child, fut in timed_out:
            fut.add_done_callback(partial(_late_completion, child))
    finally:
        if own_executor:
            executor.shutdown(wait=False)


def poll_and_copy_trades(
    db_session: Session,
    processor: Optional[Callable[[Account, Account, Dict[str, Any]], Any]] = None,
    *,
    stream: str = "trade_events",
    group: str = "trade_copier",
    consumer: str = "worker-1",
    redis_client=redis_client,
    max_messages: int | None = None,
    block: int = 0,
    batch_size: int = 10,
    max_workers: int | None = None,
    child_timeout: float | None = None,
) -> int:
    """Consume trade events from *stream* using a consumer group.

    Parameters
    ----------
    db_session:
        SQLAlchemy session used for querying accounts.
    processor:
        Optional callback that executes the actual copy operation for each
        child.  If omitted a no-op processor is used.
    stream:
        Redis Stream to consume events from. Defaults to ``"trade_events"``.
    group:
        Consumer group name used for coordinated processing.
    consumer:
        Consumer name within the group.
    redis_client:
        Redis client instance; a stub may be supplied for testing.
    max_messages:
        Optional limit for the number of messages processed. ``None`` means
        process until the stream is exhausted.
    block:
        Milliseconds to block waiting for new events. ``0`` means do not block.
    batch_size:
        Maximum number of trade events fetched per call to ``xreadgroup``.
    max_workers:
        Optional thread pool size used when dispatching copy operations.
        Defaults to the number of child accounts.
    child_timeout:
        Maximum time in seconds to wait for each child broker submission.
        Defaults to the value of the ``TRADE_COPIER_TIMEOUT`` environment
        variable.

    Returns
    -------
    int
        Number of messages processed.
    """

    processor = processor or (lambda m, c, o: None)
    max_workers = max_workers or int(
        os.getenv("TRADE_COPIER_MAX_WORKERS", str(DEFAULT_MAX_WORKERS))
    )
    child_timeout = (
        child_timeout
        if child_timeout is not None
        else float(os.getenv("TRADE_COPIER_TIMEOUT", str(DEFAULT_CHILD_TIMEOUT)))
    )

    executor = ThreadPoolExecutor(max_workers=max_workers)
    try:
        # Ensure the consumer group exists. Ignore errors if already created.
        try:
            redis_client.xgroup_create(stream, group, id="0", mkstream=True)
        except Exception as exc:  # pragma: no cover - stub may not raise
            if "BUSYGROUP" not in str(exc):
                raise

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
                count = 0
                for _stream, events in messages:
                    for msg_id, data in events:
                        event = _decode_event(data)

                        async def handle(msg_id=msg_id, event=event):
                            start = time.time()
                            try:
                                master = (
                                    db_session.query(Account)
                                    .filter_by(client_id=str(event["master_id"]), role="master")
                                    .first()
                                )
                                if master:
                                    await _replicate_to_children(
                                        db_session,
                                        master,
                                        event,
                                        processor,
                                        executor=executor,
                                        timeout=child_timeout,
                                    )
                            except Exception as exc:  # pragma: no cover - exercised in tests
                                log.exception(
                                    "error processing trade event %s", msg_id, exc_info=exc
                                )
                                raise
                            finally:
                                LATENCY.observe(time.time() - start)
                                redis_client.xack(stream, group, msg_id)

                        tasks.append((msg_id, asyncio.create_task(handle())))
                        count += 1

                if tasks:
                    results = await asyncio.gather(
                        *(t for _, t in tasks), return_exceptions=True
                    )
                    for (msg_id, _), result in zip(tasks, results):
                        if isinstance(result, Exception):
                            log.exception(
                                "error processing trade event %s", msg_id, exc_info=result
                            )
                processed += count
                if max_messages is not None and processed >= max_messages:
                    break

            return processed

        return asyncio.run(_consume())
    finally:
        executor.shutdown(wait=False)


__all__ = ["poll_and_copy_trades", "copy_order", "LATENCY"]
