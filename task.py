import os
from celery import Celery
from services.trade_copier import poll_and_copy_trades, copy_order
from services.db import get_session
from models import db
from prometheus_client import Gauge, Histogram
from services.utils import get_redis_url

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
        try:
            poll_and_copy_trades(session, processor=copy_order)
        finally:
            session.close()
