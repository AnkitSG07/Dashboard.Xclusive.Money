"""Celery beat configuration for scheduling background jobs."""
import os
from celery import Celery
from services.utils import get_redis_url

_redis_url = get_redis_url()

celery = Celery(
    "scheduler",
    broker=os.environ.get("SCHEDULER_BROKER_URL", _redis_url),
    backend=os.environ.get(
        "SCHEDULER_RESULT_BACKEND",
        os.environ.get("CELERY_RESULT_BACKEND", _redis_url),
    ),
)
celery.conf.timezone = os.environ.get("SCHEDULER_TIMEZONE", "UTC")

celery.conf.beat_schedule = {
    "poll-trades": {
        "task": os.environ.get("POLL_TRADES_TASK", "services.tasks.poll_trades"),
        "schedule": float(os.environ.get("POLL_TRADES_INTERVAL", 10.0)),
    }
}
