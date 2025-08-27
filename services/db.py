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

_engine = create_engine(sqlalchemy_url)
_Session = sessionmaker(bind=_engine)


def get_session() -> Session:
    """Return a new SQLAlchemy session bound to the configured engine."""
    return _Session()

def get_connection() -> psycopg.Connection:
    """Return a raw psycopg connection for PostgreSQL databases."""
    if not DATABASE_URL.startswith("postgresql://"):
        raise RuntimeError("Raw connection is only available for PostgreSQL URLs")
    return psycopg.connect(DATABASE_URL)
