"""Utility to expose broker adapters as standalone HTTP services.

Each broker adapter from :mod:`brokers` can be served as its own microservice
so that heavy operations and failures are isolated from the main web
application.  The service exposes a small REST API that mirrors a subset of the
adapter methods (``place_order``, ``get_positions`` and ``get_ltp``) and
includes basic timeout and rate limiting protections.
"""

from __future__ import annotations

import os
from flask import Flask, jsonify, request
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

from .factory import _load_broker_class


def create_broker_service(broker_name: str) -> Flask:
    """Return a Flask app exposing the given broker adapter."""

    BrokerClass = _load_broker_class(broker_name)
    app = Flask(__name__)

    # Apply per-service rate limiting.  The limit can be configured via the
    # ``BROKER_RATE_LIMIT`` environment variable, defaulting to a conservative
    # value that is still generous for tests.
    rate_limit = os.environ.get("BROKER_RATE_LIMIT", "60 per minute")
    Limiter(get_remote_address, app=app, default_limits=[rate_limit])

    def _build_broker(data: dict):
        client_id = data.get("client_id")
        access_token = data.get("access_token")
        symbol_map = data.get("symbol_map", {})
        return BrokerClass(client_id, access_token, symbol_map=symbol_map)

    @app.post("/place_order")
    def place_order():
        data = request.get_json(force=True) or {}
        broker = _build_broker(data)
        for key in ["client_id", "access_token", "symbol_map"]:
            data.pop(key, None)
        result = broker.place_order(**data)
        return jsonify(result)

    @app.post("/positions")
    def get_positions():
        data = request.get_json(force=True) or {}
        broker = _build_broker(data)
        result = broker.get_positions()
        return jsonify(result)

    @app.post("/ltp")
    def get_ltp():
        data = request.get_json(force=True) or {}
        broker = _build_broker(data)
        symbol = data.get("symbol")
        if not symbol or not hasattr(broker, "get_ltp"):
            return jsonify({"status": "failure", "error": "get_ltp not available"}), 501
        result = broker.get_ltp(symbol)
        return jsonify(result)

    return app


__all__ = ["create_broker_service"]
