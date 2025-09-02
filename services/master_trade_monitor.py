from __future__ import annotations

"""Monitor master broker accounts for manually placed trades.

This service periodically queries each master account's broker API for newly
executed orders and publishes matching events to the ``trade_events`` Redis
stream so that the trade copier can replicate them to child accounts.
"""

import asyncio
import redis
import logging
import os
from collections import defaultdict
from typing import Any, Dict, Iterable, Set

from brokers.factory import get_broker_client
from models import Account
from sqlalchemy.orm import Session

from .webhook_receiver import get_redis_client
from .db import get_session

log = logging.getLogger(__name__)

# Order statuses that indicate a completed or filled order.  Orders in any
# other state (e.g. ``REJECTED`` or ``PENDING``) are ignored.
COMPLETED_STATUSES = {"COMPLETE", "COMPLETED", "FILLED", "EXECUTED"}

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
        ``5`` seconds).
    max_iterations:
        Optional number of polling cycles to run.  ``None`` means run
        indefinitely.  Primarily intended for tests.
    """

    if poll_interval is None:
        poll_interval = float(os.getenv("ORDER_MONITOR_INTERVAL", "5"))

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
                credentials: Dict[str, Any] = dict(master.credentials or {})
                access_token = credentials.pop("access_token", "")

                # Skip masters whose credentials are known to be invalid unless
                # the access token has changed since the last failure.
                cached_token = invalid_creds.get(master.client_id)
                if cached_token and cached_token == access_token:
                    continue

                client_cls = get_broker_client(master.broker)
                client = client_cls(
                    master.client_id, access_token, **credentials
                )
                try:
                    orders = client.list_orders()
                    # If we successfully fetched orders, clear any cached
                    # invalid credential flag for this master.
                    invalid_creds.pop(master.client_id, None)
                except Exception as exc:  # pragma: no cover - defensive
                    msg = str(exc)
                    if "invalid" in msg.lower() and "token" in msg.lower():
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

                seen = seen_order_ids[master.client_id]
                for order in orders:
                    status = str(order.get("status") or "").upper()
                    if status and status not in COMPLETED_STATUSES:
                        continue

                    order_id = (
                        order.get("id")
                        or order.get("order_id")
                        or order.get("norenordno")
                        or order.get("timestamp")
                    )
                    # Skip orders that were already published in a previous poll
                    if order_id is not None and str(order_id) in seen:
                        continue

                    symbol = order.get("symbol") or order.get("tradingSymbol")
                    action = order.get("action") or order.get("transactionType")
                    qty = order.get("qty") or order.get("orderQty")
                    exchange = order.get("exchange") or order.get("exchangeSegment")
                    order_type = order.get("order_type") or order.get("orderType")


                    raw_event = {
                        "master_id": master.client_id,
                        "symbol": symbol,
                        "action": action,
                        "qty": qty,
                        "exchange": exchange,
                        "order_type": order_type,
                    }
                    # Filter out ``None`` values and convert the rest to strings so
                    # the Redis client never receives ``None``.
                    event = {k: str(v) for k, v in raw_event.items() if v is not None}
                    try:
                        redis_client.xadd("trade_events", event)
                    except redis.exceptions.RedisError:
                        log.exception("failed to publish trade event")
                        continue

                    if order_id is not None:
                        seen.add(str(order_id))

            iterations += 1
            if max_iterations is not None and iterations >= max_iterations:
                break
            await asyncio.sleep(poll_interval)

    asyncio.run(_monitor())


def main() -> None:
    """Run the master trade monitor service."""
    session = get_session()
    try:
        monitor_master_trades(session)
    finally:
        session.close()


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    try:
        main()
    except KeyboardInterrupt:  # pragma: no cover - interactive use
        pass


__all__ = ["monitor_master_trades", "main"]
