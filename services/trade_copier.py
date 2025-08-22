"""Trade copier service that consumes master order events from Redis.

This module listens to the ``trade_events`` Redis stream and replicates
orders from master accounts to their linked child accounts.  It no longer
relies on an in-memory queue or Flask application context, allowing it to be
run as an independent microservice.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Dict, Iterable, Optional

from prometheus_client import Histogram
from sqlalchemy.orm import Session

from models import Account
from .webhook_receiver import redis_client

LATENCY = Histogram(
    "trade_copier_latency_seconds", "Seconds spent processing a master order event"
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


async def _replicate_to_children(
    db_session: Session,
    master: Account,
    order: Dict[str, Any],
    processor: Callable[[Account, Account, Dict[str, Any]], Any],
) -> None:
    """Execute ``processor`` concurrently for all active children."""

    children = (
        db_session.query(Account)
        .filter_by(linked_master_id=master.client_id, role="child", copy_status="On")
        .all()
    )

    async def _copy(child: Account) -> Any:
        return await asyncio.to_thread(processor, master, child, order)

    if children:
        await asyncio.gather(*[_copy(child) for child in children])


def poll_and_copy_trades(
    db_session: Session,
    processor: Optional[Callable[[Account, Account, Dict[str, Any]], Any]] = None,
    *,
    stream: str = "trade_events",
    redis_client=redis_client,
    max_messages: int | None = None,
    block: int = 0,
) -> int:
    """Consume trade events from *stream* and replicate them to children.

    Parameters
    ----------
    db_session:
        SQLAlchemy session used for querying accounts.
    processor:
        Optional callback that executes the actual copy operation for each
        child.  If omitted a no-op processor is used.
    stream:
        Redis Stream to consume events from. Defaults to ``"trade_events"``.
    redis_client:
        Redis client instance; a stub may be supplied for testing.
    max_messages:
        Optional limit for the number of messages processed. ``None`` means
        process until the stream is exhausted.
    block:
        Milliseconds to block waiting for new events. ``0`` means do not block.

    Returns
    -------
    int
        Number of messages processed.
    """

    processor = processor or (lambda m, c, o: None)
    last_id = "0"
    processed = 0
    while max_messages is None or processed < max_messages:
        messages: Iterable = redis_client.xread({stream: last_id}, count=1, block=block)
        if not messages:
            break
        for _stream, events in messages:
            for msg_id, data in events:
                last_id = msg_id
                event = _decode_event(data)
                start = time.time()
                master = (
                    db_session.query(Account)
                    .filter_by(client_id=event["master_id"], role="master")
                    .first()
                )
                if master:
                    asyncio.run(_replicate_to_children(db_session, master, event, processor))
                LATENCY.observe(time.time() - start)
                processed += 1
                if max_messages is not None and processed >= max_messages:
                    break
    return processed


__all__ = ["poll_and_copy_trades", "LATENCY"]
