from types import SimpleNamespace
import logging

from brokers.base import BrokerBase
from services import master_trade_monitor


class RedisStub:
    def xgroup_create(self, *args, **kwargs):
        pass

    def xadd(self, stream, event):
        pass


class SessionStub:
    def __init__(self, master):
        self.master = master
        self.kwargs = None

    def rollback(self):
        pass

    def expire_all(self):
        pass

    def query(self, model):
        return self

    def filter_by(self, **kwargs):
        self.kwargs = kwargs
        return self

    def all(self):
        if self.kwargs.get("role") == "master":
            return [self.master]
        return []


class NoDataBroker(BrokerBase):
    def __init__(self, client_id, access_token="", **kwargs):
        super().__init__(client_id, access_token=access_token)

    def get_order_list(self):
        return {"status": "error", "error": "No data"}

    def place_order(self, *args, **kwargs):
        pass

    def get_positions(self):
        pass

    def cancel_order(self, order_id):
        pass


def test_monitor_skips_no_data_errors(monkeypatch, caplog):
    master = SimpleNamespace(
        client_id="m",
        broker="mock",
        credentials={"access_token": ""},
        role="master",
    )
    session = SessionStub(master)
    redis = RedisStub()

    monkeypatch.setattr(
        master_trade_monitor, "get_broker_client", lambda name: NoDataBroker
    )

    with caplog.at_level(logging.ERROR):
        master_trade_monitor.monitor_master_trades(
            session, redis_client=redis, max_iterations=1, poll_interval=0
        )

    assert not caplog.records
