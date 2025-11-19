import os
from celery import Celery
from services.trade_copier import poll_and_copy_trades, copy_order
from services.db import get_session
from models import db, Account
from prometheus_client import Gauge, Histogram
from services.utils import get_redis_url
from services.webhook_receiver import get_redis_client
from cache import cache_delete
from blueprints.api import (
    _load_snapshot_entry,
    _refresh_snapshot_now,
    snapshot_cache_key_for,
)

_redis_url = get_redis_url()

celery = Celery(
    "worker",
    broker=_redis_url,
    backend=os.environ.get("CELERY_RESULT_BACKEND", _redis_url),
)
celery.conf.timezone = os.environ.get("CELERY_TIMEZONE", "UTC")
celery.conf.broker_connection_retry_on_startup = True
celery.conf.result_expires = 3600  # V-- Add this line here (3600 seconds = 1 hour)

# Prometheus metrics for Celery queue depth and task latency
QUEUE_DEPTH = Gauge("celery_queue_depth", "Number of tasks waiting in the Celery queue")
WORKER_LATENCY = Histogram(
    "poll_trades_task_duration_seconds",
    "Time taken by the poll_trades_task worker to execute",
)


def update_queue_depth():
    """Update the queue depth gauge using Celery inspection."""
    i = celery.control.inspect()
    reserved = i.reserved() or {}
    scheduled = i.scheduled() or {}
    active = i.active() or {}
    total = sum(len(v) for v in reserved.values()) + \
        sum(len(v) for v in scheduled.values()) + \
        sum(len(v) for v in active.values())
    QUEUE_DEPTH.set(total)


@celery.task(name="services.tasks.poll_trades")
def poll_trades() -> None:
    """Poll and copy trades in the background.

    This task no longer depends on the Flask application context and instead
    obtains a standalone SQLAlchemy session via :func:`get_session`.
    """

    update_queue_depth()
    with WORKER_LATENCY.time():
        session = get_session()
        client = get_redis_client()
        try:
            poll_and_copy_trades(session, processor=copy_order, redis_client=client)
        finally:
            session.close()




@celery.task(name="services.tasks.refresh_dashboard_snapshot")
def refresh_dashboard_snapshot(user_id: int, client_id: str | None) -> None:
    """Refresh a cached dashboard snapshot for the given account."""

    update_queue_depth()
    session = get_session()
    key = snapshot_cache_key_for(user_id, client_id)
    refresh_key = f"{key}:refreshing"
    try:
        query = session.query(Account).filter_by(user_id=user_id)
        if client_id:
            query = query.filter_by(client_id=client_id)
        account = query.first()
        if not account or not account.credentials:
            cache_delete(key)
            cache_delete(refresh_key)
            return

        entry = _load_snapshot_entry(key)
        _refresh_snapshot_now(account, key=key, entry=entry)
    except Exception:
        cache_delete(refresh_key)
        raise
    finally:
        session.close()


@celery.task(name="services.tasks.warm_user_cache")
def warm_user_cache(user_id: int) -> None:
    """Warm per-user caches for summary and frequently accessed datasets."""

    update_queue_depth()
    from app import app as flask_app, _prime_user_cache
    from models import User

    with flask_app.app_context():
        user = db.session.get(User, user_id)
        if user:
            _prime_user_cache(user)
