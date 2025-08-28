import pytest
from marshmallow import ValidationError

from services import alert_guard


class StubRedis:
    def __init__(self):
        self.store = {}

    def set(self, name, value, nx=False, ex=None):
        if nx and name in self.store:
            return False
        self.store[name] = value
        return True

    def get(self, name):
        return self.store.get(name)


def test_duplicate_detection(monkeypatch):
    stub = StubRedis()
    monkeypatch.setattr(alert_guard, "redis_client", stub)
    monkeypatch.setattr(alert_guard, "_USER_SETTINGS_CACHE", {})
    monkeypatch.setattr(alert_guard, "_LOCAL_DEDUP", {})
    event = {
        "user_id": 1,
        "symbol": "AAPL",
        "action": "BUY",
        "qty": 1,
        "alert_id": "abc",
    }
    assert alert_guard.check_duplicate_and_risk(event) is True
    with pytest.raises(ValidationError):
        alert_guard.check_duplicate_and_risk(event)


def test_requires_identifier(monkeypatch):
    stub = StubRedis()
    monkeypatch.setattr(alert_guard, "redis_client", stub)
    monkeypatch.setattr(alert_guard, "_USER_SETTINGS_CACHE", {})
    monkeypatch.setattr(alert_guard, "_LOCAL_DEDUP", {})
    event = {
        "user_id": 1,
        "symbol": "AAPL",
        "action": "BUY",
        "qty": 1,
    }
    with pytest.raises(ValidationError):
        alert_guard.check_duplicate_and_risk(event)


def test_risk_rules(monkeypatch):
    stub = StubRedis()
    monkeypatch.setattr(alert_guard, "redis_client", stub)
    monkeypatch.setattr(alert_guard, "_USER_SETTINGS_CACHE", {})
    monkeypatch.setattr(alert_guard, "_LOCAL_DEDUP", {})
    alert_guard.update_user_settings(
        1, {"max_qty": 5, "allowed_symbols": ["AAPL"]}
    )

    event = {
        "user_id": 1,
        "symbol": "AAPL",
        "action": "BUY",
        "qty": 10,
        "alert_id": "1",
    }
    with pytest.raises(ValidationError):
        alert_guard.check_duplicate_and_risk(event)

    event2 = {
        "user_id": 1,
        "symbol": "GOOG",
        "action": "BUY",
        "qty": 1,
        "alert_id": "2",
    }
    with pytest.raises(ValidationError):
        alert_guard.check_duplicate_and_risk(event2)
