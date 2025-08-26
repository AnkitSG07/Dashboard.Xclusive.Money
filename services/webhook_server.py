from __future__ import annotations

import os
from flask import Flask, jsonify, request

from models import db, User, Strategy
from .webhook_receiver import enqueue_webhook, ValidationError

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URL", "sqlite:///webhook.db")
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db.init_app(app)


@app.post("/webhook/<token>")
def webhook(token: str):
    """Enqueue *payload* for the user identified by *token*.

    ``token`` may be either a numeric user identifier or a webhook token.
    If a ``secret`` is supplied it will be matched against the user's
    strategies and the corresponding ``strategy_id`` will be sent along
    with the event.
    """

    strategy_id = request.args.get("strategy_id", type=int)
    secret = request.args.get("secret") or request.headers.get("X-Webhook-Secret")
    payload = request.get_json(silent=True) or {}

    user = User.query.filter_by(webhook_token=token).first()
    if user is not None:
        user_id = user.id
        if secret:
            strategy = Strategy.query.filter_by(user_id=user_id, webhook_secret=secret).first()
            if not strategy:
                return jsonify({"error": "Invalid webhook secret"}), 403
            # Use strategy.id if caller didn't specify one explicitly
            if strategy_id is None:
                strategy_id = strategy.id
    else:
        # Fallback: allow numeric user IDs to be passed directly
        try:
            user_id = int(token)
        except ValueError:
            return jsonify({"error": "Unknown webhook token"}), 404

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
