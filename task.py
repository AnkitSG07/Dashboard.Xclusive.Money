from celery import Celery
from app import app, poll_and_copy_trades
import os
from prometheus_client import Gauge, Histogram

celery = Celery(
    "dhan_trading",
    broker=os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0"),
    backend=os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379/1"),
)
celery.conf.update(app.config)
celery.conf.timezone = "UTC"

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

celery.conf.beat_schedule = {
    "poll-trades": {
        "task": "task.poll_trades_task",
        "schedule": 10.0,
    }
}

@celery.task
def poll_trades_task():
    update_queue_depth()
    with WORKER_LATENCY.time():
        with app.app_context():
            poll_and_copy_trades()
