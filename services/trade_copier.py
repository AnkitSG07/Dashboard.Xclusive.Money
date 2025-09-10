"""Trade copier service that consumes master order events from Redis.

This module listens to the ``trade_events`` Redis stream and replicates
orders from master accounts to their linked child accounts.  It no longer
relies on an in-memory queue or Flask application context, allowing it to be
run as an independent microservice.
"""

from __future__ import annotations

import argparse
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
from brokers import symbol_map
import requests
from .webhook_receiver import redis_client, get_redis_client
from .db import get_session
from .utils import _decode_event
from helpers import active_children_for_master
import redis

LATENCY = Histogram(
    "trade_copier_latency_seconds", "Seconds spent processing a master order event"
)

log = logging.getLogger(__name__)

DEFAULT_MAX_WORKERS = 10
# Default per-child broker submission timeout in seconds. The value can be
# overridden at runtime via the ``TRADE_COPIER_TIMEOUT`` environment variable
# for brokers with slower APIs.  Setting the variable to ``0``, a negative
# number or ``"none"`` disables the timeout entirely.
DEFAULT_CHILD_TIMEOUT = 20.0

def _load_symbol_map_or_exit() -> None:
    """Pre-load the broker symbol map, aborting on failure.

    The trade copier relies on instrument dumps from Zerodha and Dhan to map
    symbols for various child brokers such as AliceBlue.  If these dumps cannot
    be retrieved (e.g. because neither network access nor cached copies are
    available) the service would otherwise continue and silently fail later when
    placing orders.  To prevent this we attempt to build the symbol map up front
    and terminate the process if it cannot be loaded.
    """

    try:
        symbol_map.SYMBOL_MAP = symbol_map.build_symbol_map()
    except requests.RequestException as exc:  # pragma: no cover - network errors
        log.error("failed to load instrument data: %s", exc)
        raise SystemExit(1)

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
    credentials = dict(child.credentials or {})
    access_token = credentials.pop("access_token", "")
    credentials.pop("client_id", None)

    broker = client_cls(
        client_id=child.client_id, access_token=access_token, **credentials
    )

    # Apply fixed quantity override for the child account if provided.
    qty = (
        int(child.copy_qty)
        if getattr(child, "copy_qty", None) is not None
        else int(order.get("qty", 0))
    )

    params = {
        "symbol": order.get("symbol"),
        "action": order.get("action"),
        "qty": qty,
    }
    ignore_fields = {"symbol", "action", "qty", "master_id", "id"}
    for key, value in order.items():
        if key in ignore_fields or value is None:
            continue
        params[key] = value
    try:
        return broker.place_order(**params)
    except Exception as exc:
        # Re-raise with the original broker message so that callers can log
        # a concise error.  Many broker SDKs attach additional metadata to
        # exceptions which makes the default ``repr`` noisy; here we capture
        # just the human readable message.
        message = getattr(exc, "message", None) or (
            exc.args[0] if getattr(exc, "args", None) else str(exc)
        )
        raise RuntimeError(message) from exc

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
        broker_name = getattr(child, "broker", None)
        try:
            fut.result()
            log.warning(
                "child %s (%s) copy completed after timeout",
                client_id,
                broker_name,
                extra={"child": client_id, "broker": broker_name},
            )
        except asyncio.CancelledError:
            log.warning(
                "child %s (%s) copy cancelled after timeout",
                client_id,
                broker_name,
                extra={"child": client_id, "broker": broker_name},
            )
            return
        except Exception as exc:
            msg = str(exc)
            log.warning(
                "child %s (%s) copy failed after timeout: %s",
                client_id,
                broker_name,
                msg,
                exc_info=exc,
                extra={"child": client_id, "broker": broker_name, "error": msg},
            )
    
    try:
        async_tasks = []
        timed_out: list[tuple[Account, asyncio.Future]] = []
        for child in children:
            orig_fut = loop.run_in_executor(
                executor, processor, master, child, order
            )
            shielded = asyncio.shield(orig_fut)
            wrapped = asyncio.wait_for(shielded, timeout) if timeout is not None else shielded
            async_tasks.append((child, wrapped, orig_fut))

        results = await asyncio.gather(
            *(f for _c, f, _of in async_tasks), return_exceptions=True
        )

        for (child, _wrapped, orig_fut), result in zip(async_tasks, results):
            client_id = getattr(child, "client_id", None)
            if isinstance(result, (asyncio.TimeoutError, TimeoutError)):
                if isinstance(result, asyncio.TimeoutError) and not orig_fut.done():
                    timed_out.append((child, orig_fut))
                log.warning(
                    "child %s copy timed out; order may still have executed",
                    client_id,
                    extra={"child": client_id, "error": "TimeoutError"},
                )
            elif isinstance(result, Exception):
                broker_name = getattr(child, "broker", None)
                msg = str(result)
                log.error(
                    "child %s (%s) copy failed: %s",
                    client_id,
                    broker_name,
                    msg,
                    exc_info=result,
                    extra={
                        "child": client_id,
                        "broker": broker_name,
                        "error": msg,
                    },
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
        Defaults to the ``TRADE_COPIER_TIMEOUT`` environment variable or
        ``20`` seconds if unset.  Values of ``0``, negative numbers or the
        string ``"none"`` disable the timeout.

    Returns
    -------
    int
        Number of messages processed.
    """

    processor = processor or (lambda m, c, o: None)
    max_workers = max_workers or int(
        os.getenv("TRADE_COPIER_MAX_WORKERS", str(DEFAULT_MAX_WORKERS))
    )
    if child_timeout is None:
        env_timeout = os.getenv("TRADE_COPIER_TIMEOUT", str(DEFAULT_CHILD_TIMEOUT))
        env_timeout = env_timeout.strip().lower()
        if env_timeout in {"none", ""}:
            child_timeout = None
        else:
            try:
                parsed = float(env_timeout)
            except ValueError:
                parsed = DEFAULT_CHILD_TIMEOUT
            child_timeout = None if parsed <= 0 else parsed
    elif child_timeout <= 0:
        child_timeout = None

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
                                db_session.rollback()
                                log.info(
                                    "database session rolled back after error processing trade event %s",
                                    msg_id,
                                )    
                                raise
                            else:
                                redis_client.xack(stream, group, msg_id)
                            finally:
                                LATENCY.observe(time.time() - start)

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

def main() -> None:
    """Entry point for the trade copier service."""
    
    block = int(os.getenv("TRADE_COPIER_BLOCK_MS", "5000"))

    # Ensure symbol mappings are available before processing any trades.  The
    # build uses instrument dumps from upstream brokers and falls back to
    # cached data.  If neither is accessible the trade copier aborts so that
    # orders are not submitted with missing tokens.
    _load_symbol_map_or_exit()
    log.info("trade copier worker starting")

    while True:
        session = get_session()
        client = get_redis_client()
        try:
            processed = poll_and_copy_trades(
                session,
                processor=copy_order,
                redis_client=client,
                block=block,
            )
            log.info("processed %d trade event(s)", processed)
        except redis.exceptions.RedisError:
            log.exception("redis error while copying trades")
        finally:
            session.close()


__all__ = ["poll_and_copy_trades", "copy_order", "LATENCY", "main"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the trade copier worker")
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="enable debug logging",
    )
    args = parser.parse_args()
    level = "DEBUG" if args.verbose else os.getenv("LOG_LEVEL", "INFO")
    logging.basicConfig(level=level)
    try:
        main()
    except KeyboardInterrupt:
        pass
