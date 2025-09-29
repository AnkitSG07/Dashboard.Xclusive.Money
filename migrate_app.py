"""Minimal Flask application for running database migrations."""

import os

from flask import Flask
from flask_migrate import Migrate

from models import db  # noqa: F401
import models  # ensure models are registered with metadata


def create_app() -> Flask:
    app = Flask(__name__)

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL must be set to run migrations")

    if db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg://", 1)

    app.config["SQLALCHEMY_DATABASE_URI"] = db_url
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {"pool_pre_ping": True}

    db.init_app(app)
    Migrate(app, db)
    return app


app = create_app()
