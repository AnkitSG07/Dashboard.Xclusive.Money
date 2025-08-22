import os
import json
from datetime import datetime
from flask import Flask, jsonify, request
from sqlalchemy import create_engine, text

LOG_DB_URL = os.environ.get("LOG_DB_URL", "sqlite:///logs.db")
AUTH_TOKEN = os.environ.get("LOGGING_SERVICE_TOKEN")

engine = create_engine(LOG_DB_URL, future=True)

app = Flask(__name__)


def _init_db() -> None:
    """Create the logs table if it does not yet exist."""
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS logs (
                    id SERIAL PRIMARY KEY,
                    timestamp TEXT,
                    level TEXT,
                    message TEXT,
                    user_id TEXT,
                    module TEXT,
                    details TEXT
                )
                """
            )
        )


_init_db()


def _authorize(req) -> bool:
    """Return True if request is authorized."""
    if not AUTH_TOKEN:
        return True
    auth_header = req.headers.get("Authorization", "")
    return auth_header == f"Bearer {AUTH_TOKEN}"


@app.post("/logs")
def ingest_log():
    if not _authorize(request):
        return jsonify({"error": "unauthorized"}), 401
    event = request.get_json(force=True) or {}
    if "timestamp" not in event:
        event["timestamp"] = datetime.utcnow().isoformat()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO logs (timestamp, level, message, user_id, module, details)
                VALUES (:timestamp, :level, :message, :user_id, :module, :details)
                """
            ),
            {
                "timestamp": event.get("timestamp"),
                "level": event.get("level"),
                "message": event.get("message"),
                "user_id": event.get("user_id"),
                "module": event.get("module"),
                "details": json.dumps(event.get("details")),
            },
        )
    return jsonify({"status": "accepted"}), 202


@app.get("/logs")
def query_logs():
    if not _authorize(request):
        return jsonify({"error": "unauthorized"}), 401
    limit = int(request.args.get("limit", 100))
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT timestamp, level, message, user_id, module, details
                FROM logs ORDER BY id DESC LIMIT :limit
                """
            ),
            {"limit": limit},
        )
        rows = []
        for row in result:
            try:
                details = json.loads(row.details) if row.details else {}
            except Exception:
                details = {}
            rows.append(
                {
                    "timestamp": row.timestamp,
                    "level": row.level,
                    "message": row.message,
                    "user_id": row.user_id,
                    "module": row.module,
                    "details": details,
                }
            )
    return jsonify(rows)


def create_app() -> Flask:
    """Factory for creating the logging service app."""
    return app


if __name__ == "__main__":  # pragma: no cover - manual execution
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 9090)))
