"""Standalone SQLAlchemy session factory for worker services.

This module allows background workers (e.g. Celery tasks) to obtain a
SQLAlchemy session without requiring the Flask application context.  The
connection URL is taken from the ``DATABASE_URL`` environment variable and
defaults to ``sqlite:///app.db`` for local development.
"""

from __future__ import annotations

import os
import psycopg
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///app.db")

if DATABASE_URL.startswith("postgresql://"):
    sqlalchemy_url = DATABASE_URL.replace("postgresql://", "postgresql+psycopg://", 1)
else:
    sqlalchemy_url = DATABASE_URL

# The engine is configured with ``pool_pre_ping`` to gracefully handle stale
# connections in long-running worker processes and ``pool_recycle`` to ensure
# connections are refreshed periodically (here every 30 minutes).
_engine = create_engine(
    sqlalchemy_url, pool_pre_ping=True, pool_recycle=1800
)
_Session = sessionmaker(bind=_engine)

# Ensure the core tables exist when using lightweight setups like SQLite.
# The worker processes operate outside the Flask application context so the
# database schema may not have been created yet.  Calling ``create_all`` is
# idempotent and will only create missing tables, avoiding errors such as
# "no such table: account" when starting workers against a fresh database.
try:  # pragma: no cover - defensive
    from models import db as models_db

    models_db.metadata.create_all(bind=_engine)
except Exception:  # pragma: no cover - best effort
    # If the import fails or the database is unavailable we simply defer
    # table creation.  Subsequent database operations will then surface the
    # underlying problem rather than masking it here.
    pass

def get_session() -> Session:
    """Return a new SQLAlchemy session bound to the configured engine."""
    return _Session()

def get_connection() -> psycopg.Connection:
    """Return a raw psycopg connection for PostgreSQL databases."""
    if not DATABASE_URL.startswith("postgresql://"):
        raise RuntimeError("Raw connection is only available for PostgreSQL URLs")
    return psycopg.connect(DATABASE_URL)
