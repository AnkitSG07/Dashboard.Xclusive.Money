import os

os.environ.setdefault("SECRET_KEY", "test")
os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
os.environ.setdefault("ADMIN_EMAIL", "admin@example.com")
os.environ.setdefault("ADMIN_PASSWORD", "secret")
os.environ.setdefault("RUN_SCHEDULER", "0")

from brokers.symbol_map import SYMBOLS_KEY

import app as app_module
import symbols


def test_symbols_endpoint_includes_broker_tokens(monkeypatch):
    fake_symbol_map = {
        "RELIANCE": {
            "NSE": {
                "dhan": {"security_id": "123"},
                "upstox": {"token": "u1"},
                "kotakneo": {"token": "k1"},
                "iifl": {"token": "i1"},
            },
            "BSE": {
                "dhan": {"security_id": "999"},
                "aliceblue": {"symbol_id": "a1"},
                "angel": {"token": "ang1"},
            },
            SYMBOLS_KEY: {},
        }
    }

    monkeypatch.setattr(symbols, "get_symbol_map", lambda: fake_symbol_map)
    monkeypatch.setattr(symbols, "refresh_symbol_map", lambda force=False: None)

    previous_snapshot = symbols._SYMBOL_SNAPSHOT
    previous_last_good = symbols._LAST_GOOD_SNAPSHOT
    previous_started = symbols._LAST_REFRESH_STARTED_AT
    previous_completed = symbols._LAST_REFRESH_COMPLETED_AT
    previous_duration = symbols._LAST_REFRESH_DURATION
    previous_success = symbols._LAST_REFRESH_SUCCESS
    try:
        assert symbols.refresh_symbol_snapshot(force=True)

        with app_module.app.test_client() as client:
            with client.session_transaction() as session:
                session["user"] = "tester@example.com"

            resp = client.get("/api/symbols")

        assert resp.status_code == 200
        payload = resp.get_json()
        assert isinstance(payload, list)

        entry = next(item for item in payload if item.get("symbol") == "RELIANCE")

        assert entry.get("dhan") == "123"
        assert entry.get("aliceblue") == "a1"
        assert entry.get("angelone") == "ang1"
        assert entry.get("iifl") == "i1"
        assert entry.get("kotak") == "k1"
        assert entry.get("upstox") == "u1"
    finally:
        symbols._SYMBOL_SNAPSHOT = previous_snapshot
        symbols._LAST_GOOD_SNAPSHOT = previous_last_good
        symbols._LAST_REFRESH_STARTED_AT = previous_started
        symbols._LAST_REFRESH_COMPLETED_AT = previous_completed
        symbols._LAST_REFRESH_DURATION = previous_duration
        symbols._LAST_REFRESH_SUCCESS = previous_success
