from __future__ import annotations

"""Monitor master broker accounts for manually placed trades.

This service periodically queries each master account's broker API for new
executed orders and publishes matching events to the ``trade_events`` Redis
stream so that the trade copier can replicate them to child accounts.
"""

import asyncio
import redis
import logging
import os
import time
from collections import defaultdict
from typing import Any, Dict, Iterable, Set

from brokers.factory import get_broker_client
from models import Account
from sqlalchemy.orm import Session

from .webhook_receiver import get_redis_client
from .db import get_session
from helpers import normalize_order

log = logging.getLogger(__name__)

# Order statuses that indicate a completed or filled order.  Orders in any
# other state (e.g. ``REJECTED`` or ``PENDING``) are ignored.  Include
# broker-specific values such as Dhan's ``TRADED`` status.
COMPLETED_STATUSES = {
    "COMPLETE",
    "COMPLETED",
    "FILLED",
    "EXECUTED",
    "TRADED",
    "FULL_EXECUTED",
    "FULLY_EXECUTED",
    "2",
    "CONFIRMED",
    "SUCCESS",
    "OK",
    "FULLY_FILLED",
    "PARTIAL_FILLED",  # Also include partial fills for progressive copying
    "PARTIALLY FILLED",
    "PARTIALLY_FILLED",
    "REPLACED",
    "NEW",
    "ACCEPTED",
    "OPEN",
    "TRIGGERED",
}

# Keep track of processed orders to avoid duplicates across restarts
PROCESSED_ORDERS_KEY = "processed_manual_orders:{master_id}"
PROCESSED_ORDERS_TTL = 86400 * 7  # 7 days

def monitor_master_trades(
    db_session: Session,
    *,
    redis_client=None,
    poll_interval: float | None = None,
    max_iterations: int | None = None,
) -> None:
    """Poll master accounts for new orders and publish to ``trade_events``.

    Parameters
    ----------
    db_session:
        SQLAlchemy session used to look up master accounts.
    redis_client:
        Redis client instance used for publishing trade events. If ``None`` the
        client is created from :func:`services.webhook_receiver.get_redis_client`.
    poll_interval:
        Seconds to wait between polling iterations. If ``None`` the value of
        the ``ORDER_MONITOR_INTERVAL`` environment variable is used (default
        ``10`` seconds).
    max_iterations:
        Optional number of polling cycles to run.  ``None`` means run
        indefinitely.  Primarily intended for tests.
    """

    if poll_interval is None:
        poll_interval = float(os.getenv("ORDER_MONITOR_INTERVAL", "10"))

    if redis_client is None:
        redis_client = get_redis_client()

    async def _monitor() -> None:
        iterations = 0
        # Track order IDs we've already published for each master to avoid
        # emitting duplicate trade events when polling repeatedly.
        seen_order_ids: Dict[str, Set[str]] = defaultdict(set)
        # Remember masters that have invalid/expired credentials so we don't
        # spam the logs on every iteration.  The value is the last access token
        # seen for that master.  If the token changes we will attempt to fetch
        # orders again.
        invalid_creds: Dict[str, str] = {}
        
        # Load previously processed orders from Redis to handle service restarts
        def load_processed_orders(master_id: str) -> Set[str]:
            try:
                key = PROCESSED_ORDERS_KEY.format(master_id=master_id)
                processed = redis_client.smembers(key)
                return {p.decode() if isinstance(p, bytes) else str(p) for p in processed}
            except Exception:
                return set()
        
        def mark_order_processed(master_id: str, order_id: str):
            try:
                key = PROCESSED_ORDERS_KEY.format(master_id=master_id)
                redis_client.sadd(key, order_id)
                redis_client.expire(key, PROCESSED_ORDERS_TTL)
            except Exception:
                pass

        while max_iterations is None or iterations < max_iterations:
            # Ensure we see any credential updates made by other processes.
            # Without ending the previous transaction the session may return
            # stale Account objects, causing the monitor to keep using an
            # expired access token even after it has been refreshed in the
            # database.
            db_session.rollback()
            db_session.expire_all()
            masters: Iterable[Account] = (
                db_session.query(Account).filter_by(role="master").all()
            )
            
            for master in masters:
                # Skip masters that are not active for copy trading
                if getattr(master, 'copy_status', None) != 'On':
                    continue
                    
                credentials: Dict[str, Any] = dict(master.credentials or {})
                access_token = credentials.pop("access_token", "")
                # Some accounts may persist ``client_id`` within the credentials
                # blob.  Passing it alongside the explicit ``master.client_id``
                # argument would result in ``TypeError: multiple values for
                # client_id`` when instantiating the broker client, so drop it
                # here.
                credentials.pop("client_id", None)
                
                # Skip masters whose credentials are known to be invalid unless
                # the access token has changed since the last failure.
                cached_token = invalid_creds.get(master.client_id)
                if cached_token and cached_token == access_token:
                    continue

                client_cls = get_broker_client(master.broker)
                # Pass the access token as a keyword argument so that broker
                # clients which expect other positional parameters (like
                # ``api_key`` for AliceBlue) don't receive the token as a
                # second positional value.  Any additional credentials are
                # expanded as keyword arguments as well.
                try:
                    client = client_cls(
                        master.client_id, access_token=access_token, **credentials
                    )
                    orders = client.list_orders()
                    # If we successfully fetched orders, clear any cached
                    # invalid credential flag for this master.
                    invalid_creds.pop(master.client_id, None)
                except Exception as exc:  # pragma: no cover - defensive
                    msg = str(exc)
                    if any(keyword in msg.lower() for keyword in ["invalid", "token", "expired", "unauthorized"]):
                        # Log once per token and remember the failing token so
                        # future iterations skip this master until the token
                        # changes.
                        if cached_token != access_token:
                            log.error(
                                "failed to fetch manual orders for %s: %s",
                                master.client_id,
                                msg,
                            )
                        invalid_creds[master.client_id] = access_token
                    else:
                        log.exception(
                            "failed to fetch manual orders",
                            extra={"master": master.client_id},
                        )
                    continue

                # Load previously processed orders for this master
                if master.client_id not in seen_order_ids:
                    seen_order_ids[master.client_id] = load_processed_orders(master.client_id)

                seen = seen_order_ids[master.client_id]
                new_orders_found = 0
                
                for raw_order in orders:
                    order = normalize_order(raw_order, master.broker)
                    status = order.get("status")
                    if status and status not in COMPLETED_STATUSES:
                        continue

                    order_id = order.get("order_id")
                    if order_id is None:
                        order_id = order.get("order_time")

                    # Skip orders that were already published
                    if order_id is not None and str(order_id) in seen:
                        continue

                    # Skip orders missing essential information
                    if not order.get("symbol") or not order.get("action") or not order.get("qty"):
                        log.warning(
                            "Skipping manual order with missing essential data: %s",
                            order
                        )
                        continue

                    raw_event = {
                        "master_id": master.client_id,
                        "symbol": order.get("symbol"),
                        "action": order.get("action"),
                        "qty": order.get("qty"),
                        "exchange": order.get("exchange"),
                        "order_type": order.get("order_type"),
                        "instrument_type": order.get("instrument_type"),
                        "expiry": order.get("expiry"),
                        "strike": order.get("strike"),
                        "option_type": order.get("option_type"),
                        "lot_size": order.get("lot_size"),
                        "price": order.get("price"),
                        "order_id": order_id,
                        "order_time": order.get("order_time"),
                        "source": "manual_trade_monitor",  # Mark as manual trade
                    }
                    
                    # Filter out ``None`` values and convert the rest to strings so
                    # the Redis client never receives ``None``.
                    event = {k: str(v) for k, v in raw_event.items() if v is not None}
                    
                    try:
                        # Publish to trade_events stream for copying to children
                        redis_client.xadd("trade_events", event)
                        log.info(
                            "Published manual trade event for master %s: %s %s %s at %s",
                            master.client_id, order.get("action"), order.get("qty"), order.get("symbol"), order.get("exchange")
                        )
                        new_orders_found += 1
                    except redis.exceptions.RedisError:
                        log.exception("failed to publish manual trade event")
                        continue

                    # Mark order as processed
                    if order_id is not None:
                        seen.add(str(order_id))
                        mark_order_processed(master.client_id, str(order_id))

                if new_orders_found > 0:
                    log.info(
                        "Found %d new manual orders for master %s", 
                        new_orders_found, master.client_id
                    )

            iterations += 1
            if max_iterations is not None and iterations >= max_iterations:
                break
            await asyncio.sleep(poll_interval)

    asyncio.run(_monitor())


def main() -> None:
    """Run the master trade monitor service."""
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    log.info("Starting master trade monitor service...")
    
    session = get_session()
    try:
        monitor_master_trades(session)
    finally:
        session.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:  # pragma: no cover - interactive use
        log.info("Master trade monitor service stopped by user")
        pass


__all__ = ["monitor_master_trades", "main"]
