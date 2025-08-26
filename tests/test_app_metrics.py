import os

os.environ.setdefault("SECRET_KEY", "test")
os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
os.environ.setdefault("ADMIN_EMAIL", "admin@example.com")
os.environ.setdefault("ADMIN_PASSWORD", "secret")

import importlib
import app


def test_metrics_route_registered():
    # ensure route exists
    routes = {rule.rule for rule in app.app.url_map.iter_rules()}
    assert "/metrics" in routes

    with app.app.test_client() as client:
        resp = client.get("/metrics")
        assert resp.status_code == 200
        assert resp.mimetype == "text/plain"


def test_scheduler_not_started_on_import():
    """Importing ``app`` should not trigger the optional scheduler."""
    assert app.scheduler_started is False


def test_scheduler_factory_hook(monkeypatch):
    """``start_scheduler`` loads and starts a configured scheduler factory."""
    calls = []

    def factory():
        class Dummy:
            def start(self):
                calls.append(True)

        return Dummy()

    import types, sys

    module = types.ModuleType("fake_sched")
    module.factory = factory
    sys.modules["fake_sched"] = module
    monkeypatch.setenv("SCHEDULER_FACTORY", "fake_sched:factory")

    try:
        app.start_scheduler()
        assert calls == [True]
    finally:
        # Clean up for other tests
        app.scheduler_started = False
        sys.modules.pop("fake_sched", None)
        monkeypatch.delenv("SCHEDULER_FACTORY", raising=False)
