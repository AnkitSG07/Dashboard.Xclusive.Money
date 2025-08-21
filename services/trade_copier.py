"""Asynchronous trade copying service backed by an in-memory queue."""
from __future__ import annotations

import asyncio
import time
from queue import Queue, Empty
from typing import Any, Callable, Dict, Optional

from prometheus_client import Gauge, Histogram
from flask import current_app

from models import Account

# Global queue used by the trade copier.  Events contain a ``master_id`` and
# arbitrary order ``data`` that should be replicated to child accounts.
trade_event_queue: "Queue[Dict[str, Any]]" = Queue()

# Prometheus metrics for operational visibility.  ``QUEUE_DEPTH`` tracks the
# number of pending events while ``LATENCY`` measures end-to-end processing
# time for each event.
QUEUE_DEPTH = Gauge(
    "trade_copier_queue_depth",
    "Number of master order events waiting to be processed",
)
LATENCY = Histogram(
    "trade_copier_latency_seconds",
    "Seconds spent processing a master order event",
)


def enqueue_master_order(master_id: str, order: Dict[str, Any]) -> None:
    """Push a master order event onto the queue.

    Parameters
    ----------
    master_id:
        Client identifier for the master account producing the order.
    order:
        Raw order payload that should be replicated to child accounts.
    """
    trade_event_queue.put({"master_id": master_id, "order": order, "enqueued": time.time()})
    QUEUE_DEPTH.set(trade_event_queue.qsize())


async def _replicate_to_children(
    master: Account, order: Dict[str, Any], processor: Callable[[Account, Account, Dict[str, Any]], Any]
) -> None:
    """Execute ``processor`` concurrently for all active children of ``master``."""
    children = Account.query.filter_by(
        linked_master_id=master.client_id, role="child", copy_status="On"
    ).all()

    async def _copy(child: Account) -> Any:
        # ``processor`` is executed in a thread to avoid blocking the event loop
        return await asyncio.to_thread(processor, master, child, order)

    await asyncio.gather(*[_copy(child) for child in children])


def poll_and_copy_trades(
    processor: Optional[Callable[[Account, Account, Dict[str, Any]], Any]] = None
) -> None:
    """Consume master-order events and replicate trades.

    Parameters
    ----------
    processor:
        Optional callback used to execute the actual copy operation for each
        child.  If omitted a no-op processor is used which enables tests to
        supply their own implementation.
    """
    processor = processor or (lambda m, c, o: None)
    app = current_app._get_current_object()
    with app.app_context():
        while True:
            try:
                event = trade_event_queue.get_nowait()
            except Empty:
                break

            start = time.time()
            master = Account.query.filter_by(
                client_id=event["master_id"], role="master"
            ).first()
            if master:
                asyncio.run(_replicate_to_children(master, event["order"], processor))

            LATENCY.observe(time.time() - event["enqueued"])
            trade_event_queue.task_done()
            QUEUE_DEPTH.set(trade_event_queue.qsize())
