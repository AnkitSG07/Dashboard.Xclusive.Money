from types import SimpleNamespace

import pytest

from blueprints import api as api_module


class DummyAccount(SimpleNamespace):
    pass


def test_prefer_cache_without_entry_returns_placeholder(monkeypatch):
    account = DummyAccount(user_id=1, client_id="demo")

    monkeypatch.setattr(api_module, "_load_snapshot_entry", lambda key: None)

    enqueue_calls = {}

    def fake_enqueue(acc, key):
        enqueue_calls["args"] = (acc, key)
        return True

    monkeypatch.setattr(api_module, "_enqueue_snapshot_refresh", fake_enqueue)

    def fail_refresh(*args, **kwargs):  # pragma: no cover - guard rails
        raise AssertionError("_refresh_snapshot_now should not be invoked when prefer_cache is True")

    monkeypatch.setattr(api_module, "_refresh_snapshot_now", fail_refresh)

    snapshot = api_module.get_cached_dashboard_snapshot(account, prefer_cache=True)

    assert enqueue_calls["args"][0] is account
    assert snapshot["stale"] is True
    assert snapshot.get("portfolio") == []
    assert snapshot.get("orders", {}).get("items") == []
    assert snapshot.get("orders", {}).get("summary") == api_module._default_order_summary()
    assert snapshot.get("account") == {}
    assert snapshot.get("errors") == {}
    assert "cached_at" in snapshot
    assert "age" in snapshot


def test_prefer_cache_placeholder_reports_enqueue_failure(monkeypatch):
    account = DummyAccount(user_id=42, client_id="slow")

    monkeypatch.setattr(api_module, "_load_snapshot_entry", lambda key: None)

    def enqueue_failure(*args, **kwargs):
        return False

    monkeypatch.setattr(api_module, "_enqueue_snapshot_refresh", enqueue_failure)

    def fail_refresh(*args, **kwargs):  # pragma: no cover - guard rails
        raise AssertionError("_refresh_snapshot_now should not be invoked when prefer_cache is True")

    monkeypatch.setattr(api_module, "_refresh_snapshot_now", fail_refresh)

    snapshot = api_module.get_cached_dashboard_snapshot(account, prefer_cache=True)

    assert snapshot["stale"] is True
    assert snapshot.get("errors", {}).get("snapshot") == "Snapshot refresh deferred: background queue unavailable"
