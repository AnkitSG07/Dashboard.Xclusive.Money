from celery import Celery
from app import app, poll_and_copy_trades
import os

celery = Celery(
    "dhan_trading",
    broker=os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0"),
    backend=os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379/1"),
)
celery.conf.update(app.config)
celery.conf.timezone = "UTC"

celery.conf.beat_schedule = {
    "poll-trades": {
        "task": "tasks.poll_trades_task",
        "schedule": 10.0,
    }
}

@celery.task
def poll_trades_task():
    with app.app_context():
        poll_and_copy_trades()
