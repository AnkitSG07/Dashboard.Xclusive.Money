import os
import logging
from datetime import datetime
import requests

LOGGING_SERVICE_URL = os.environ.get("LOGGING_SERVICE_URL", "http://localhost:9090/logs")
AUTH_TOKEN = os.environ.get("LOGGING_SERVICE_TOKEN")

logger = logging.getLogger(__name__)


def publish_log_event(event: dict) -> None:
    """Send a log event to the external logging service.

    Failures are swallowed to avoid impacting the main application.
    """
    if "timestamp" not in event:
        event["timestamp"] = datetime.utcnow().isoformat()
    headers = {}
    if AUTH_TOKEN:
        headers["Authorization"] = f"Bearer {AUTH_TOKEN}"
    try:
        requests.post(LOGGING_SERVICE_URL, json=event, headers=headers, timeout=2)
    except Exception:  # pragma: no cover - best effort logging
        logger.debug("Failed to publish log event", exc_info=True)
