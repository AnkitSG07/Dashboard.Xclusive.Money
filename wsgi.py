from app import app
from flask_migrate import upgrade as migrate_upgrade
import logging

logger = logging.getLogger(__name__)

with app.app_context():
    try:
        migrate_upgrade()
    except Exception as exc:
        logger.error(f"Failed to apply migrations: {exc}")

# Gunicorn entrypoint
application = app
