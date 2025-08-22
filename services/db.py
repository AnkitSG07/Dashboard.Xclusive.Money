"""Standalone SQLAlchemy session factory for worker services.

This module allows background workers (e.g. Celery tasks) to obtain a
SQLAlchemy session without requiring the Flask application context.  The
connection URL is taken from the ``DATABASE_URL`` environment variable and
defaults to ``sqlite:///app.db`` for local development.
"""

from __future__ import annotations

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///app.db")

_engine = create_engine(DATABASE_URL)
_Session = sessionmaker(bind=_engine)


def get_session() -> Session:
    """Return a new SQLAlchemy session bound to the configured engine."""
    return _Session()
