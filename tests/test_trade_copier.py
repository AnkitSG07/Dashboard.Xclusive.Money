import pytest

from services import trade_copier


def test_poll_and_copy_trades_processes_event(monkeypatch):
    processed = []

    class DummyMaster:
        client_id = "m1"

    class DummyQuery:
        def filter_by(self, **kwargs):
            return self
        def first(self):
            return DummyMaster()
        def all(self):
            return [object(), object()]

    # Monkeypatch Account.query to avoid database access
    monkeypatch.setattr(trade_copier, "Account", type("Dummy", (), {"query": DummyQuery()}))

    class DummyApp:
        def app_context(self):
            from contextlib import nullcontext
            return nullcontext()

    monkeypatch.setattr(
        trade_copier,
        "current_app",
        type("CA", (), {"_get_current_object": staticmethod(lambda: DummyApp())}),
    )

    def processor(master, child, order):
        processed.append(order["id"])

    trade_copier.enqueue_master_order("m1", {"id": 1})
    trade_copier.poll_and_copy_trades(processor=processor)

    assert processed == [1, 1]  # two children processed
    assert trade_copier.QUEUE_DEPTH._value.get() == 0
