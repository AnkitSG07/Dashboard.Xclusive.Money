import os
import json
import sqlite3
import threading
from queue import Queue, Empty
from datetime import datetime
from pathlib import Path
from flask import Blueprint, jsonify, request

class LogService:
    """Background log persistence service using SQLite in WAL mode."""
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.queue: "Queue[dict]" = Queue()
        self._stop = threading.Event()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                level TEXT,
                message TEXT,
                user_id TEXT,
                module TEXT,
                details TEXT
            )
            """
        )
        self._conn.commit()
        self._worker = threading.Thread(target=self._run, daemon=True)
        self._worker.start()

    def _run(self):
        while not self._stop.is_set():
            try:
                event = self.queue.get(timeout=0.5)
            except Empty:
                continue
            try:
                self._conn.execute(
                    "INSERT INTO logs (timestamp, level, message, user_id, module, details) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        event.get("timestamp"),
                        event.get("level"),
                        event.get("message"),
                        event.get("user_id"),
                        event.get("module"),
                        json.dumps(event.get("details")),
                    ),
                )
                self._conn.commit()
            finally:
                self.queue.task_done()

    def publish(self, event: dict) -> None:
        """Queue a log event for asynchronous persistence."""
        if "timestamp" not in event:
            event["timestamp"] = datetime.utcnow().isoformat()
        self.queue.put(event)

    def query(self, limit: int = 100) -> list:
        cur = self._conn.execute(
            "SELECT timestamp, level, message, user_id, module, details FROM logs ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        rows = cur.fetchall()
        result = []
        for ts, level, msg, user_id, module, details in rows:
            try:
                parsed_details = json.loads(details) if details else {}
            except Exception:
                parsed_details = {}
            result.append(
                {
                    "timestamp": ts,
                    "level": level,
                    "message": msg,
                    "user_id": user_id,
                    "module": module,
                    "details": parsed_details,
                }
            )
        return result

# Initialize service with database under data directory
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get("DATA_DIR", BASE_DIR / "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "logs.db"
log_service = LogService(str(DB_PATH))

# Convenience publish function
def publish_log_event(event: dict) -> None:
    log_service.publish(event)

# Flask blueprint for HTTP access
logging_bp = Blueprint("logging_service", __name__)

@logging_bp.route("/", methods=["POST"])
def ingest_log():
    event = request.get_json(force=True) or {}
    log_service.publish(event)
    return jsonify({"status": "accepted"}), 202

@logging_bp.route("/", methods=["GET"])
def query_logs():
    limit = int(request.args.get("limit", 100))
    return jsonify(log_service.query(limit))
