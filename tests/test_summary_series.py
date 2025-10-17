from datetime import datetime
import os
import types

import sqlalchemy


class _FakeInspector:
    def get_table_names(self):
        return []

    def get_columns(self, _table_name):
        return []


sqlalchemy.inspect = lambda *args, **kwargs: _FakeInspector()

os.environ.setdefault("SECRET_KEY", "test-key")
os.environ.setdefault("DATABASE_URL", "postgresql://localhost/test")
os.environ.setdefault("ADMIN_EMAIL", "admin@example.com")
os.environ.setdefault("ADMIN_PASSWORD", "secret")

import app


class _DummyQuery:
    def __init__(self, items):
        self._items = list(items)

    def filter(self, *args, **kwargs):
        return self

    def filter_by(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def limit(self, *_args, **_kwargs):
        return self

    def all(self):
        return list(self._items)


class _DummyColumn:
    def in_(self, _values):
        return self


class _DummyCastResult:
    def desc(self):
        return self


def test_build_summary_series_parses_mixed_timestamps(monkeypatch):
    logs = [
        types.SimpleNamespace(
            timestamp=datetime(2024, 1, 1, 10, 0, 0), performance={"pnl": 100}, strategy_id=1
        ),
        types.SimpleNamespace(
            timestamp="2024-01-01T11:00:00", performance={"pnl": 110}, strategy_id=1
        ),
        types.SimpleNamespace(
            timestamp="2024-01-01 12:00:00", performance={"pnl": 120}, strategy_id=1
        ),
        types.SimpleNamespace(timestamp="not-a-date", performance={"pnl": 999}, strategy_id=1),
    ]

    trades = [
        types.SimpleNamespace(timestamp="0", qty=1, price=10, action="SELL"),
        types.SimpleNamespace(timestamp=1, qty=2, price=5, action="BUY"),
        types.SimpleNamespace(timestamp="bad", qty=1, price=1, action="SELL"),
    ]

    dummy_column = _DummyColumn()
    monkeypatch.setattr(
        app,
        "StrategyLog",
        types.SimpleNamespace(query=_DummyQuery(logs), strategy_id=dummy_column, timestamp=dummy_column),
    )
    monkeypatch.setattr(
        app,
        "Trade",
        types.SimpleNamespace(query=_DummyQuery(trades), timestamp=dummy_column),
    )

    def _fake_cast(*_args, **_kwargs):
        return _DummyCastResult()

    monkeypatch.setattr(app, "cast", _fake_cast)

    user = types.SimpleNamespace(id=123)
    series = app._build_summary_series(user, [1], min_points=5, max_points=10)

    timestamps = [entry["timestamp"] for entry in series]
    assert timestamps == [
        "1970-01-01T00:00:00",
        "1970-01-01T00:00:01",
        "2024-01-01T10:00:00",
        "2024-01-01T11:00:00",
        "2024-01-01T12:00:00",
    ]
