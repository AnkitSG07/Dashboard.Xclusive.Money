"""Celery beat configuration for scheduling background jobs."""
import os
from celery import Celery

celery = Celery(
    "scheduler",
    broker=os.environ.get(
        "SCHEDULER_BROKER_URL",
        os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0"),
    ),
    backend=os.environ.get(
        "SCHEDULER_RESULT_BACKEND",
        os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379/1"),
    ),
)
celery.conf.timezone = os.environ.get("SCHEDULER_TIMEZONE", "UTC")

celery.conf.beat_schedule = {
    "poll-trades": {
        "task": os.environ.get("POLL_TRADES_TASK", "services.tasks.poll_trades"),
        "schedule": float(os.environ.get("POLL_TRADES_INTERVAL", 10.0)),
    }
}
