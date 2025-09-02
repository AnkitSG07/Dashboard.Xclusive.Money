from types import SimpleNamespace

import sys
import types


class _ShoonyaStub:
    pass


sys.modules.setdefault("api_helper", types.SimpleNamespace(ShoonyaApiPy=_ShoonyaStub))

from brokers import finvasia
from services import master_trade_monitor


class DummyAPI:
    def __init__(self, orders):
        self.orders = orders

    def get_order_book(self):
        return self.orders


class FinvasiaStub(finvasia.FinvasiaBroker):
    def __init__(self, client_id, access_token="", **credentials):
        orders = credentials.pop("orders", [])
        imei = credentials.get("imei", "1")
        super().__init__(client_id, imei=imei)
        self.api = DummyAPI(orders)

    def _is_logged_in(self):
        return True


class RedisStub:
    def __init__(self):
        self.stream = []

    def xgroup_create(self, *args, **kwargs):
        pass

    def xadd(self, stream, event):
        self.stream.append((f"{len(self.stream)+1}".encode(), event))


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


def test_finvasia_master_orders_published(monkeypatch):
    order = {
        "tsym": "AAPL-EQ",
        "trantype": "BUY",
        "qty": 1,
        "exch": "NSE",
        "prctyp": "MKT",
        "norenordno": "1",
        "status": "COMPLETE",
    }
    master = SimpleNamespace(
        client_id="m",
        broker="finvasia",
        credentials={"access_token": "", "imei": "1", "orders": [order]},
        role="master",
    )
    session = SessionStub(master)
    redis = RedisStub()

    monkeypatch.setattr(master_trade_monitor, "get_broker_client", lambda name: FinvasiaStub)

    master_trade_monitor.monitor_master_trades(
        session, redis_client=redis, max_iterations=1, poll_interval=0
    )

    assert redis.stream == [
        (
            b"1",
            {
                "master_id": "m",
                "symbol": "AAPL-EQ",
                "action": "BUY",
                "qty": "1",
                "exchange": "NSE",
                "order_type": "MKT",
            },
        )
    ]


def test_finvasia_duplicate_orders_not_republished(monkeypatch):
    order = {
        "tsym": "AAPL-EQ",
        "trantype": "BUY",
        "qty": 1,
        "exch": "NSE",
        "prctyp": "MKT",
        "norenordno": "1",
        "status": "COMPLETE",
    }
    master = SimpleNamespace(
        client_id="m",
        broker="finvasia",
        credentials={"access_token": "", "imei": "1", "orders": [order]},
        role="master",
    )
    session = SessionStub(master)
    redis = RedisStub()

    monkeypatch.setattr(master_trade_monitor, "get_broker_client", lambda name: FinvasiaStub)

    master_trade_monitor.monitor_master_trades(
        session, redis_client=redis, max_iterations=2, poll_interval=0
    )

    assert len(redis.stream) == 1
