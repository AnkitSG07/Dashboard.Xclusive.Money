from __future__ import annotations

import os
from flask import Flask, jsonify, request

from .webhook_receiver import enqueue_webhook, ValidationError

app = Flask(__name__)


@app.post("/webhook/<int:user_id>")
def webhook(user_id: int):
    """Endpoint that enqueues webhook *payload* for *user_id*."""
    strategy_id = request.args.get("strategy_id", type=int)
    payload = request.get_json(silent=True) or {}
    try:
        event = enqueue_webhook(user_id, strategy_id, payload)
    except ValidationError as exc:  # pragma: no cover - simple pass-through
        return jsonify({"error": str(exc)}), 400
    return jsonify(event), 202


def main() -> None:  # pragma: no cover - CLI helper
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))


if __name__ == "__main__":  # pragma: no cover
    main()


__all__ = ["app", "main"]
