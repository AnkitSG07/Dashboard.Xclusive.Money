import asyncio
import time
from types import SimpleNamespace
import json
import pytest
import requests
from services import trade_copier
from services.trade_copier import _replicate_to_children


def test_main_preloads_symbol_map(monkeypatch):
    loaded = {}

    def fake_cache():
        loaded["called"] = True

    def fake_poll(*args, **kwargs):
        raise SystemExit

    monkeypatch.setattr(trade_copier.symbol_map, "SYMBOL_MAP", {})
    monkeypatch.setattr(trade_copier.symbol_map, "ensure_symbol_cache", fake_cache)
    monkeypatch.setattr(trade_copier, "poll_and_copy_trades", fake_poll)

    with pytest.raises(SystemExit):
        trade_copier.main()

    assert loaded.get("called") is True
    assert trade_copier.symbol_map.SYMBOL_MAP == {}


def test_main_exits_when_symbol_map_missing(monkeypatch, caplog):
    def fake_cache():
        raise requests.RequestException("boom")

    def fake_poll(*args, **kwargs):
        pytest.fail("poll_and_copy_trades should not be called")

    monkeypatch.setattr(trade_copier.symbol_map, "ensure_symbol_cache", fake_cache)
    monkeypatch.setattr(trade_copier, "poll_and_copy_trades", fake_poll)

    with caplog.at_level("ERROR"), pytest.raises(SystemExit):
        trade_copier.main()

    assert "failed to load instrument data" in caplog.text.lower()


class StubRedis:
    def __init__(self):
        self.acks = []
        self.calls = 0

    def xgroup_create(self, *args, **kwargs):
        pass

    def xreadgroup(self, group, consumer, streams, count, block):
        if self.calls == 0:
            self.calls += 1
            return [
                (
                    "trade_events",
                    [
                        (b"1", {b"master_id": b"m"}),
                        (b"2", {b"master_id": b"m"}),
                    ],
                )
            ]
        return []

    def xack(self, stream, group, msg_id):
        self.acks.append(msg_id)


class DummySession:
    def __init__(self, master):
        self.master = master

    def query(self, model):
        return self

    def filter_by(self, **kwargs):
        return self

    def first(self):
        return self.master

    def rollback(self):
        pass

    def expire_all(self):
        pass


def test_copy_order_with_extra_credentials(monkeypatch):
    """Ensure client and API keys in credentials don't cause TypeErrors."""

    captured = {}

    class StubBroker:
        def __init__(self, client_id, api_key, access_token):
            captured["client_id"] = client_id
            captured["api_key"] = api_key
            captured["access_token"] = access_token

        def place_order(self, **params):
            return {"status": "ok"}

    monkeypatch.setattr(trade_copier, "get_broker_client", lambda name: StubBroker)

    child = SimpleNamespace(
        broker="stub",
        client_id="c1",
        credentials={"client_id": "dup", "access_token": "t", "api_key": "k"},
        copy_qty=None,
    )
    master = SimpleNamespace(client_id="m", broker="stub")
    order = {"symbol": "AAPL", "action": "BUY", "qty": 1}

    trade_copier.copy_order(master, child, order)

    assert captured == {
        "client_id": "c1",
        "api_key": "k",
        "access_token": "t",
    }


def test_copy_order_normalizes_exchange(monkeypatch):
    """Manual Dhan events use NSE_EQ which should be normalized to NSE."""

    captured_params = {}

    class StubBroker:
        def __init__(self, client_id, access_token, **_):
            self.client_id = client_id

        def place_order(self, **params):
            captured_params.update(params)
            if params.get("exchange") != "NSE":
                raise ValueError("invalid symbol")
            return {"status": "ok"}

    monkeypatch.setattr(trade_copier, "get_broker_client", lambda name: StubBroker)

    master = SimpleNamespace(client_id="m", broker="dhan")
    child = SimpleNamespace(
        broker="stub",
        client_id="c1",
        credentials={"access_token": "token"},
        copy_qty=None,
    )
    order = {
        "symbol": "RELIANCE",
        "action": "BUY",
        "qty": 1,
        "exchange": "NSE_EQ",
    }

    result = trade_copier.copy_order(master, child, order)

    assert result == {"status": "ok"}
    assert captured_params["exchange"] == "NSE"

def test_copy_order_normalises_exchange_codes(monkeypatch):
    captured = {}

    class StubBroker:
        def __init__(self, client_id, access_token, **_):
            self.client_id = client_id

        def place_order(self, **params):
            captured.update(params)
            return {"status": "ok"}

    monkeypatch.setattr(trade_copier, "get_broker_client", lambda name: StubBroker)

    master = SimpleNamespace(broker="dhan", client_id="m")
    child = SimpleNamespace(
        broker="mock", client_id="c1", credentials={"access_token": "t"}, copy_qty=None
    )
    order = {"symbol": "SBIN", "action": "BUY", "qty": 1, "exchange": "BSE_EQ"}

    result = trade_copier.copy_order(master, child, order)

    assert result == {"status": "ok"}
    assert captured["exchange"] == "BSE"



def test_copy_qty_overrides_master_quantity(monkeypatch):
    orders = []

    class StubBroker:
        def __init__(self, client_id, access_token, **_):
            self.client_id = client_id

        def place_order(self, **params):
            orders.append(params)
            return {"status": "ok"}

    monkeypatch.setattr(trade_copier, "get_broker_client", lambda name: StubBroker)

    child = SimpleNamespace(
        broker="stub",
        client_id="c1",
        credentials={"access_token": "t"},
        copy_qty=2,
    )
    master = SimpleNamespace(client_id="m", broker="stub")
    master = {"client_id": "m", "broker": "stub"}
    order = {"symbol": "AAPL", "action": "BUY", "qty": 10}

    trade_copier.copy_order(master, child, order)

    assert orders == [{"symbol": "AAPL", "action": "BUY", "qty": 2}]

def test_copy_from_dhan_to_aliceblue(monkeypatch):
    from brokers.aliceblue import AliceBlueBroker
    captured = {}

    class DummyAliceBlue(AliceBlueBroker):
        def __init__(self, client_id, access_token="", **kwargs):
            self.client_id = client_id
            self.session_id = "sid"
            captured["instance"] = self

        def ensure_session(self):
            pass

        def _request(self, method, url, headers=None, data=None):
            captured["payload"] = json.loads(data)
            class Resp:
                def json(self_inner):
                    return [{"stat": "Ok", "nestOrderNumber": "1"}]
            return Resp()

        monkeypatch.setattr(trade_copier, "get_broker_client", lambda name: DummyAliceBlue)
    import brokers.aliceblue as alice_mod
    monkeypatch.setattr(
        alice_mod,
        "get_symbol_for_broker",
        lambda symbol, broker: {"symbol_id": "1", "trading_symbol": symbol, "exch": "NSE"},
    )
    master = SimpleNamespace(broker="dhan", client_id="m")
    child = SimpleNamespace(
        broker="aliceblue",
        client_id="c1",
        credentials={},
        copy_qty=None,
    )
    order = {"symbol": "AAPL", "action": "BUY", "qty": 1, "product_type": "INTRADAY", "order_type": "MARKET"}

    resp = trade_copier.copy_order(master, child, order)
    assert resp["status"] == "success"
    assert captured["payload"][0]["pCode"] == "MIS"
    assert captured["payload"][0]["prctyp"] == "MKT"


def test_copy_bse_equity_includes_series_symbols(monkeypatch):
    from brokers.aliceblue import AliceBlueBroker
    from brokers.fyers import FyersBroker

    captured = {}

    class DummyAliceBlue(AliceBlueBroker):
        def __init__(self, client_id, access_token="", **kwargs):
            self.client_id = client_id
            self.session_id = "sid"
            captured["alice_instance"] = self

        def ensure_session(self):
            pass

        def _request(self, method, url, headers=None, data=None):
            captured["alice_payload"] = json.loads(data)[0]

            class Resp:
                def json(self_inner):
                    return [{"stat": "Ok", "nestOrderNumber": "1"}]

            return Resp()

    class DummyFyers(FyersBroker):
        def __init__(self, client_id, access_token="", **kwargs):
            super().__init__(client_id, access_token, **kwargs)

        def place_order(self, **params):
            formatted = self._format_symbol_for_fyers(
                params.get("symbol"), params.get("exchange")
            )
            payload = dict(params)
            payload["symbol"] = formatted
            captured["fyers_payload"] = payload
            return {"status": "success"}

    def fake_client(name):
        if name == "aliceblue":
            return DummyAliceBlue
        if name == "fyers":
            return DummyFyers
        raise AssertionError(f"unexpected broker {name}")

    monkeypatch.setattr(trade_copier, "get_broker_client", fake_client)

    import brokers.aliceblue as alice_mod
    monkeypatch.setattr(
        alice_mod,
        "get_symbol_for_broker_lazy",
        lambda symbol, broker, exchange=None: {
            "symbol_id": "321",
            "trading_symbol": "MCL-NS",
            "exch": "BSE",
        },
    )

    import brokers.fyers as fyers_mod

    def fake_fyers_lookup(symbol, broker, exchange=None):
        if symbol in {"MCL", "MCL-EQ"}:
            return {"symbol": "BSE:MCL-NS"}
        return {}

    monkeypatch.setattr(
        fyers_mod.symbol_map,
        "get_symbol_for_broker",
        fake_fyers_lookup,
    )

    master = SimpleNamespace(broker="dhan", client_id="m")

    alice_child = SimpleNamespace(
        broker="aliceblue",
        client_id="alice",
        credentials={},
        copy_qty=None,
    )

    fyers_child = SimpleNamespace(
        broker="fyers",
        client_id="fyers",
        credentials={},
        copy_qty=None,
    )

    order = {
        "symbol": "MCL",
        "action": "BUY",
        "qty": 1,
        "exchange": "BSE_EQ",
        "product_type": "INTRADAY",
        "order_type": "MARKET",
    }

    trade_copier.copy_order(master, alice_child, dict(order))
    assert captured["alice_payload"]["trading_symbol"] == "MCL-NS"

    trade_copier.copy_order(master, fyers_child, dict(order))
    assert captured["fyers_payload"]["symbol"] == "BSE:MCL-NS"
    assert captured["fyers_payload"]["exchange"] == "BSE"


def test_copy_from_dhan_to_zerodha(monkeypatch):
    from brokers.zerodha import ZerodhaBroker
    captured = {}

    class DummyZerodha(ZerodhaBroker):
        def __init__(self, client_id, access_token="", api_key="k", **kwargs):
            self.client_id = client_id
            self.kite = SimpleNamespace(VARIETY_REGULAR="regular")
            self.timeout = 1

            def place_order(**params):
                captured["params"] = params
                return "1"

            self.kite.place_order = lambda variety, **params: place_order(**params)

        def ensure_token(self):
            pass

        def _kite_call(self, func, *args, **kwargs):
            return func(*args, **kwargs)

    monkeypatch.setattr(trade_copier, "get_broker_client", lambda name: DummyZerodha)
    master = SimpleNamespace(broker="dhan", client_id="m")
    child = SimpleNamespace(
        broker="zerodha",
        client_id="c1",
        credentials={"api_key": "k"},
        copy_qty=None,
    )
    order = {"symbol": "AAPL", "action": "BUY", "qty": 1, "product_type": "INTRADAY", "order_type": "MARKET"}

    resp = trade_copier.copy_order(master, child, order)
    assert resp["status"] == "success"
    assert captured["params"]["product"] == "MIS"
    assert captured["params"]["order_type"] == "MARKET"


def test_copy_from_aliceblue_to_dhan(monkeypatch):
    from brokers.dhan import DhanBroker
    captured = {}

    class DummyDhan(DhanBroker):
        def __init__(self, client_id, access_token="", **kwargs):
            super().__init__(client_id, access_token, **kwargs)

        def _request(self, method, url, headers=None, json=None, **kwargs):
            captured["payload"] = json
            class Resp:
                def json(self_inner):
                    return {"orderId": "1"}

            return Resp()

    monkeypatch.setattr(trade_copier, "get_broker_client", lambda name: DummyDhan)
    master = SimpleNamespace(broker="aliceblue", client_id="m")
    child = SimpleNamespace(
        broker="dhan",
        client_id="c1",
        credentials={},
        copy_qty=None,
    )
    order = {
        "symbol": "AAPL",
        "action": "BUY",
        "qty": 1,
        "product_type": "MIS",
        "order_type": "MKT",
        "security_id": "1",
        "exchange_segment": "NSE_EQ",
    }

    resp = trade_copier.copy_order(master, child, order)
    assert resp["status"] == "success"
    assert captured["payload"]["productType"] == "INTRADAY"
    assert captured["payload"]["orderType"] == "MARKET"


def test_copy_from_zerodha_to_dhan(monkeypatch):
    from brokers.dhan import DhanBroker
    captured = {}

    class DummyDhan(DhanBroker):
        def __init__(self, client_id, access_token="", **kwargs):
            super().__init__(client_id, access_token, **kwargs)

        def _request(self, method, url, headers=None, json=None, **kwargs):
            captured["payload"] = json
            class Resp:
                def json(self_inner):
                    return {"orderId": "1"}

            return Resp()

    monkeypatch.setattr(trade_copier, "get_broker_client", lambda name: DummyDhan)
    master = SimpleNamespace(broker="zerodha", client_id="m")
    child = SimpleNamespace(
        broker="dhan",
        client_id="c1",
        credentials={},
        copy_qty=None,
    )
    order = {
        "symbol": "AAPL",
        "action": "BUY",
        "qty": 1,
        "product_type": "MIS",
        "order_type": "MARKET",
        "security_id": "1",
        "exchange_segment": "NSE_EQ",
    }

    resp = trade_copier.copy_order(master, child, order)
    assert resp["status"] == "success"
    assert captured["payload"]["productType"] == "INTRADAY"
    assert captured["payload"]["orderType"] == "MARKET"

def test_copy_from_aliceblue_to_zerodha(monkeypatch):
    from brokers.zerodha import ZerodhaBroker
    captured = {}

    class DummyZerodha(ZerodhaBroker):
        def __init__(self, client_id, access_token="", api_key="k", **kwargs):
            self.client_id = client_id
            self.kite = SimpleNamespace(VARIETY_REGULAR="regular")
            self.timeout = 1

            def place_order(**params):
                captured["params"] = params
                return "1"

            self.kite.place_order = lambda variety, **params: place_order(**params)

        def ensure_token(self):
            pass

        def _kite_call(self, func, *args, **kwargs):
            return func(*args, **kwargs)

    monkeypatch.setattr(trade_copier, "get_broker_client", lambda name: DummyZerodha)
    master = SimpleNamespace(broker="aliceblue", client_id="m")
    child = SimpleNamespace(
        broker="zerodha",
        client_id="c1",
        credentials={"api_key": "k"},
        copy_qty=None,
    )
    order = {"symbol": "AAPL", "action": "BUY", "qty": 1, "product_type": "MIS", "order_type": "MKT"}

    resp = trade_copier.copy_order(master, child, order)
    assert resp["status"] == "success"
    assert captured["params"]["order_type"] == "MARKET"
    assert captured["params"]["product"] == "MIS"


def test_copy_from_zerodha_to_aliceblue(monkeypatch):
    from brokers.aliceblue import AliceBlueBroker
    captured = {}

    class DummyAliceBlue(AliceBlueBroker):
        def __init__(self, client_id, access_token="", **kwargs):
            self.client_id = client_id
            self.session_id = "sid"

        def ensure_session(self):
            pass

        def _request(self, method, url, headers=None, data=None):
            captured["payload"] = json.loads(data)
            class Resp:
                def json(self_inner):
                    return [{"stat": "Ok", "nestOrderNumber": "1"}]
            return Resp()

    monkeypatch.setattr(trade_copier, "get_broker_client", lambda name: DummyAliceBlue)
    import brokers.aliceblue as alice_mod
    monkeypatch.setattr(
        alice_mod,
        "get_symbol_for_broker",
        lambda symbol, broker: {"symbol_id": "1", "trading_symbol": symbol, "exch": "NSE"},
    )
    master = SimpleNamespace(broker="zerodha", client_id="m")
    child = SimpleNamespace(
        broker="aliceblue",
        client_id="c1",
        credentials={},
        copy_qty=None,
    )
    order = {"symbol": "AAPL", "action": "BUY", "qty": 1, "product_type": "MIS", "order_type": "MARKET"}

    resp = trade_copier.copy_order(master, child, order)
    assert resp["status"] == "success"
    assert captured["payload"][0]["prctyp"] == "MKT"
    assert captured["payload"][0]["pCode"] == "MIS"


def test_copy_order_logs_broker_error(monkeypatch, caplog):
    class StubBroker:
        def __init__(self, client_id, access_token):
            pass

        def place_order(self, **params):
            raise ValueError("invalid product type")

    monkeypatch.setattr(trade_copier, "get_broker_client", lambda name: StubBroker)

    master = SimpleNamespace(client_id="m", broker="stub")
    child = SimpleNamespace(broker="stub", client_id="c1", credentials={}, copy_qty=None)
    order = {"symbol": "AAPL", "action": "BUY", "qty": 1}

    monkeypatch.setattr(
        trade_copier, "active_children_for_master", lambda m, s: [child]
    )

    with caplog.at_level("ERROR"):
        asyncio.run(
            _replicate_to_children(None, master, order, trade_copier.copy_order)
        )

    assert any(
        r.levelname == "ERROR"
        and "child c1 (stub) copy failed: invalid product type" in r.message
        and getattr(r, "child", None) == "c1"
        and getattr(r, "broker", None) == "stub"
        and getattr(r, "error", None) == "invalid product type"
        for r in caplog.records
    )

async def slow_replicate(
    db_session,
    master,
    event,
    processor,
    *,
    executor=None,
    max_workers=None,
    timeout=None,
):
    await asyncio.sleep(0.1)


def slow_processor(master, child, order):
    time.sleep(0.1)


def test_poll_and_copy_trades_processes_events_concurrently(monkeypatch):
    master = SimpleNamespace(client_id="m", broker="mock")
    session = DummySession(master)
    redis = StubRedis()

    calls = []
    executors = []

    async def fake_rep(
        db_session,
        master,
        event,
        processor,
        *,
        executor=None,
        max_workers=None,
        timeout=None,
    ):
        calls.append(getattr(executor, "_max_workers", None))
        executors.append(executor)
        await asyncio.sleep(0.1)

    monkeypatch.setattr(trade_copier, "_replicate_to_children", fake_rep)

    start = time.perf_counter()
    processed = trade_copier.poll_and_copy_trades(
        session, max_messages=2, redis_client=redis, max_workers=3
    )
    duration = time.perf_counter() - start

    assert processed == 2
    assert calls == [3, 3]
    assert len({id(e) for e in executors}) == 1
    assert duration < 0.2
    assert redis.acks == [b"1", b"2"]


def test_poll_and_copy_trades_respects_batch_size(monkeypatch):
    master = SimpleNamespace(client_id="m", broker="mock")
    session = DummySession(master)

    class BatchRedis(StubRedis):
        def __init__(self):
            super().__init__()
            self.counts = []
            self.calls = 0

        def xreadgroup(self, group, consumer, streams, count, block):
            self.counts.append(count)
            if self.calls == 0:
                self.calls += 1
                return [("trade_events", [(b"1", {b"master_id": b"m"})])]
            elif self.calls == 1:
                self.calls += 1
                return [("trade_events", [(b"2", {b"master_id": b"m"})])]
            return []

    redis = BatchRedis()
    monkeypatch.setattr(trade_copier, "_replicate_to_children", slow_replicate)

    processed = trade_copier.poll_and_copy_trades(
        session, max_messages=2, redis_client=redis, batch_size=1
    )

    assert processed == 2
    assert redis.counts == [1, 1]


def test_replicate_to_children_respects_max_workers(monkeypatch):
    master = SimpleNamespace(client_id="m", broker="mock")
    children = [
        SimpleNamespace(broker="mock", client_id="c1", credentials={}, copy_qty=None),
        SimpleNamespace(broker="mock", client_id="c2", credentials={}, copy_qty=None),
    ]
    order = {"symbol": "AAPL", "action": "BUY", "qty": 1}

    monkeypatch.setattr(
        trade_copier, "active_children_for_master", lambda m, s: children
    )

    async def measure(limit):
        start = time.perf_counter()
        await _replicate_to_children(None, master, order, slow_processor, max_workers=limit)
        return time.perf_counter() - start

    t_serial = asyncio.run(measure(1))
    t_parallel = asyncio.run(measure(2))

    assert t_serial > t_parallel


def test_replicate_to_children_isolates_child_errors(monkeypatch, caplog):
    master = SimpleNamespace(client_id="m", broker="mock")
    children = [
        SimpleNamespace(broker="mock", client_id="c1", credentials={}, copy_qty=None),
        SimpleNamespace(broker="mock", client_id="c2", credentials={}, copy_qty=None),
    ]
    order = {"symbol": "AAPL", "action": "BUY", "qty": 1}
    
    monkeypatch.setattr(
        trade_copier, "active_children_for_master", lambda m, s: children
    )

    executed = []

    def processor(master, child, order):
        if child.client_id == "c1":
            raise RuntimeError("fail")
        executed.append(child.client_id)

    with caplog.at_level("WARNING"):
        asyncio.run(
            _replicate_to_children(None, master, order, processor, max_workers=2)
        )

    assert executed == ["c2"]
    assert any(
        r.levelname == "ERROR"
        and "child c1 (mock) copy failed: fail" in r.message
        for r in caplog.records
    )
    assert any(
        r.levelname == "ERROR"
        and getattr(r, "child", None) == "c1"
        and getattr(r, "broker", None) == "mock"
        and getattr(r, "error", None) == "fail"
        for r in caplog.records
    )


def test_replicate_to_children_enforces_timeout(monkeypatch, caplog):
    master = SimpleNamespace(client_id="m", broker="mock")
    children = [
        SimpleNamespace(broker="mock", client_id="fast", credentials={}, copy_qty=None),
        SimpleNamespace(broker="mock", client_id="slow", credentials={}, copy_qty=None),
    ]

    order = {"symbol": "AAPL", "action": "BUY", "qty": 1}

    monkeypatch.setattr(
        trade_copier, "active_children_for_master", lambda m, s: children
    )

    def processor(master, child, order):
        if child.client_id == "slow":
            time.sleep(0.2)
        else:
            time.sleep(0.01)

    with caplog.at_level("WARNING"):
        start = time.perf_counter()
        asyncio.run(
            _replicate_to_children(
                None, master, order, processor, max_workers=2, timeout=0.05
            )
        )
        duration = time.perf_counter() - start
    
    assert duration < 0.15
    assert any(
        r.levelname == "WARNING" and "child slow (mock) copy timed out" in r.message
        for r in caplog.records
    )
    assert not any(
        r.levelname == "ERROR" and getattr(r, "child", None) == "slow"
        for r in caplog.records
    )


def test_replicate_to_children_logs_warning_for_timeout(monkeypatch, caplog):
    master = SimpleNamespace(client_id="m", broker="mock")
    children = [
        SimpleNamespace(broker="mock", client_id="to", credentials={}, copy_qty=None),
        SimpleNamespace(broker="mock", client_id="err", credentials={}, copy_qty=None),
    ]
    order = {"symbol": "AAPL", "action": "BUY", "qty": 1}

    monkeypatch.setattr(
        trade_copier, "active_children_for_master", lambda m, s: children
    )

    def processor(master, child, order):
        if child.client_id == "to":
            raise TimeoutError("boom")
        raise RuntimeError("fail")

    with caplog.at_level("WARNING"):
        asyncio.run(
            _replicate_to_children(None, master, order, processor, max_workers=2)
        )

    assert any(
        r.levelname == "WARNING" and "child to (mock) copy timed out" in r.message
        for r in caplog.records
    )
    assert any(
        r.levelname == "ERROR"
        and "child err (mock) copy failed: fail" in r.message
        and getattr(r, "child", None) == "err"
        and getattr(r, "broker", None) == "mock"
        and getattr(r, "error", None) == "fail"
        for r in caplog.records
    )


def test_replicate_to_children_logs_late_completion(monkeypatch, caplog):
    master = SimpleNamespace(client_id="m", broker="mock")
    child = SimpleNamespace(broker="mock", client_id="slow", credentials={}, copy_qty=None)
    order = {"symbol": "AAPL", "action": "BUY", "qty": 1}

    monkeypatch.setattr(
        trade_copier, "active_children_for_master", lambda m, s: [child]
    )

    def processor(master, child, order):
        time.sleep(0.02)
    async def runner():
        await _replicate_to_children(None, master, order, processor, timeout=0.01)
        await asyncio.sleep(0.05)

    with caplog.at_level("WARNING"):
        asyncio.run(runner())

    assert any(
        r.levelname == "WARNING" and "copy completed after timeout" in r.message
        for r in caplog.records
    )


def test_replicate_to_children_handles_case_insensitive_status(monkeypatch):
    master = SimpleNamespace(client_id="m", broker="mock")
    children = [
        SimpleNamespace(broker="mock", client_id="c1", credentials={}, copy_qty=None, copy_status="ON"),
        SimpleNamespace(broker="mock", client_id="c2", credentials={}, copy_qty=None, copy_status="oN"),
        SimpleNamespace(broker="mock", client_id="c3", credentials={}, copy_qty=None, copy_status="Off"),
    ]
    order = {"symbol": "AAPL", "action": "BUY", "qty": 1}

    monkeypatch.setattr(
        trade_copier,
        "active_children_for_master",
        lambda m, s: [c for c in children if c.copy_status.lower() == "on"],
    )

    processed = []

    def processor(master, child, order):
        processed.append(child.client_id)

    asyncio.run(_replicate_to_children(None, master, order, processor))

    assert processed == ["c1", "c2"]


def test_replicate_to_children_passes_session(monkeypatch):
    master = SimpleNamespace(client_id="m", broker="mock")
    order = {"symbol": "AAPL", "action": "BUY", "qty": 1}
    session = object()
    seen = {}

    def fake_active_children(master_arg, session_arg):
        seen["session"] = session_arg
        return []

    monkeypatch.setattr(
        trade_copier, "active_children_for_master", fake_active_children
    )

    asyncio.run(_replicate_to_children(session, master, order, slow_processor))

    assert seen["session"] is session


def test_poll_and_copy_trades_reads_max_workers_env(monkeypatch):
    master = SimpleNamespace(client_id="m", broker="mock")
    session = DummySession(master)
    class OneRedis(StubRedis):
        def xreadgroup(self, group, consumer, streams, count, block):
            if self.calls == 0:
                self.calls += 1
                return [("trade_events", [(b"1", {b"master_id": b"m"})])]
            return []

    redis = OneRedis()

    calls = []

    async def fake_rep(
        db_session,
        master,
        event,
        processor,
        *,
        executor=None,
        max_workers=None,
        timeout=None,
    ):
        calls.append(getattr(executor, "_max_workers", None))

    monkeypatch.setenv("TRADE_COPIER_MAX_WORKERS", "7")
    monkeypatch.setattr(trade_copier, "_replicate_to_children", fake_rep)

    trade_copier.poll_and_copy_trades(session, max_messages=1, redis_client=redis)

    assert calls == [7]


@pytest.mark.parametrize("value", ["0", "-1", "none", "NONE"])
def test_poll_and_copy_trades_disables_child_timeout(monkeypatch, value):
    master = SimpleNamespace(client_id="m", broker="mock")
    session = DummySession(master)

    class OneRedis(StubRedis):
        def xreadgroup(self, group, consumer, streams, count, block):
            if self.calls == 0:
                self.calls += 1
                return [("trade_events", [(b"1", {b"master_id": b"m"})])]
            return []

    redis = OneRedis()
    seen = []

    async def fake_rep(
        db_session,
        master,
        event,
        processor,
        *,
        executor=None,
        max_workers=None,
        timeout=None,
    ):
        seen.append(timeout)

    monkeypatch.setenv("TRADE_COPIER_TIMEOUT", value)
    monkeypatch.setattr(trade_copier, "_replicate_to_children", fake_rep)

    trade_copier.poll_and_copy_trades(session, max_messages=1, redis_client=redis)

    assert seen == [None]


def test_poll_and_copy_trades_enforces_timeout_floor(monkeypatch):
    master = SimpleNamespace(client_id="m", broker="mock")
    session = DummySession(master)

    class OneRedis(StubRedis):
        def xreadgroup(self, group, consumer, streams, count, block):
            if self.calls == 0:
                self.calls += 1
                return [("trade_events", [(b"1", {b"master_id": b"m"})])]
            return []

    redis = OneRedis()
    seen = []

    async def fake_rep(
        db_session,
        master,
        event,
        processor,
        *,
        executor=None,
        max_workers=None,
        timeout=None,
    ):
        seen.append(timeout)

    monkeypatch.setattr(trade_copier, "DEFAULT_CHILD_TIMEOUT", 0.5)
    monkeypatch.setattr(trade_copier, "BROKER_HTTP_TIMEOUT", 0.5)
    monkeypatch.setenv("TRADE_COPIER_TIMEOUT", "0.1")
    monkeypatch.setattr(trade_copier, "_replicate_to_children", fake_rep)

    trade_copier.poll_and_copy_trades(session, max_messages=1, redis_client=redis)

    assert seen == [0.5]


def test_poll_and_copy_trades_handles_slow_broker_without_timeout(monkeypatch, caplog):
    master = SimpleNamespace(client_id="m", broker="mock")
    session = DummySession(master)
    child = SimpleNamespace(broker="mock", client_id="c1", credentials={}, copy_qty=None)

    class OneRedis(StubRedis):
        def xreadgroup(self, group, consumer, streams, count, block):
            if self.calls == 0:
                self.calls += 1
                return [("trade_events", [(b"1", {b"master_id": b"m"})])]
            return []

    redis = OneRedis()

    def processor(master, child, order):
        time.sleep(0.02)

    monkeypatch.setattr(trade_copier, "DEFAULT_CHILD_TIMEOUT", 0.05)
    monkeypatch.setattr(trade_copier, "BROKER_HTTP_TIMEOUT", 0.05)
    monkeypatch.setenv("TRADE_COPIER_TIMEOUT", "0.01")

    def active_children(master_arg, session_arg, logger=None):
        assert logger is not None
        return [child]

    monkeypatch.setattr(trade_copier, "active_children_for_master", active_children)

    with caplog.at_level("WARNING"):
        trade_copier.poll_and_copy_trades(
            session,
            processor=processor,
            max_messages=1,
            redis_client=redis,
        )

    assert redis.acks == [b"1"]
    assert not any(
        "copy completed after timeout" in record.message for record in caplog.records
    )


def test_poll_and_copy_trades_ack_on_child_error(monkeypatch, caplog):
    master = SimpleNamespace(client_id="m", broker="mock")
    children = [
        SimpleNamespace(broker="mock", client_id="good", credentials={}, copy_qty=None),
        SimpleNamespace(broker="mock", client_id="bad", credentials={}, copy_qty=None),
    ]

    class Session:
        def __init__(self, master, children):
            self.master = master
            self.children = children
            self._kwargs = {}

        def query(self, model):
            return self

        def filter_by(self, **kwargs):
            self._kwargs = kwargs
            return self

        def first(self):
            if self._kwargs.get("role") == "master":
                return self.master
            return None

        def all(self):
            if self._kwargs.get("role") == "child":
                return self.children
            return []

        def rollback(self):
            pass

        def expire_all(self):
            pass

    class OneRedis(StubRedis):
        def xreadgroup(self, group, consumer, streams, count, block):
            if self.calls == 0:
                self.calls += 1
                return [("trade_events", [(b"1", {b"master_id": b"m"})])]
            return []

    redis = OneRedis()
    processed = []

    def processor(master, child, order):
        if child.client_id == "bad":
            raise RuntimeError("boom")
        processed.append(child.client_id)

    monkeypatch.setattr(
        trade_copier, "active_children_for_master", lambda m, s: children
    )

    trade_copier.poll_and_copy_trades(
        Session(master, children),
        processor=processor,
        max_messages=1,
        redis_client=redis,
    )

    assert processed == ["good"]
    assert redis.acks == [b"1"]
    assert any(
        r.levelname == "ERROR"
        and "child bad (mock) copy failed: boom" in r.message
        for r in caplog.records
    )
    assert any(
        r.levelname == "ERROR"
        and getattr(r, "child", None) == "bad"
        and getattr(r, "broker", None) == "mock"
        and getattr(r, "error", None) == "boom"
        for r in caplog.records
    )


def test_poll_and_copy_trades_skips_events_without_master(monkeypatch, caplog):
    master = SimpleNamespace(client_id="m", broker="mock")
    session = DummySession(master)

    class MissingMasterRedis(StubRedis):
        def xreadgroup(self, group, consumer, streams, count, block):
            if self.calls == 0:
                self.calls += 1
                return [("trade_events", [(b"1", {b"symbol": b"SBIN"})])]
            return []

    redis = MissingMasterRedis()
    seen = []

    async def fake_rep(
        db_session,
        master,
        event,
        processor,
        *,
        executor=None,
        timeout=None,
    ):
        seen.append(event)

    caplog.set_level("ERROR")
    monkeypatch.setattr(trade_copier, "_replicate_to_children", fake_rep)

    trade_copier.poll_and_copy_trades(session, max_messages=1, redis_client=redis)

    assert redis.acks == [b"1"]
    assert seen == []
    assert "missing master identifier" in caplog.text.lower()


def test_poll_and_copy_trades_supports_legacy_master_key(monkeypatch):
    master = SimpleNamespace(client_id="m", broker="mock")
    session = DummySession(master)

    class LegacyKeyRedis(StubRedis):
        def xreadgroup(self, group, consumer, streams, count, block):
            if self.calls == 0:
                self.calls += 1
                return [("trade_events", [(b"1", {b"master_client_id": b"m"})])]
            return []

    redis = LegacyKeyRedis()
    seen_master_ids = []

    async def fake_rep(
        db_session,
        master,
        event,
        processor,
        *,
        executor=None,
        timeout=None,
    ):
        seen_master_ids.append(event["master_id"])

    monkeypatch.setattr(trade_copier, "_replicate_to_children", fake_rep)

    trade_copier.poll_and_copy_trades(session, max_messages=1, redis_client=redis)

    assert redis.acks == [b"1"]
    assert seen_master_ids == ["m"]


def test_poll_and_copy_trades_logs_task_errors(monkeypatch, caplog):
    master = SimpleNamespace(client_id="m", broker="mock")
    session = DummySession(master)

    class ErrorRedis(StubRedis):
        def xreadgroup(self, group, consumer, streams, count, block):
            if self.calls == 0:
                self.calls += 1
                return [
                    (
                        "trade_events",
                        [
                            (b"1", {b"master_id": b"m", b"fail": b"1"}),
                            (b"2", {b"master_id": b"m"}),
                        ],
                    )
                ]
            return []

    redis = ErrorRedis()

    async def fake_rep(db_session, master, event, processor, *, executor=None, timeout=None):
        if event.get("fail"):
            raise RuntimeError("boom")

    monkeypatch.setattr(trade_copier, "_replicate_to_children", fake_rep)
    with caplog.at_level("ERROR"):
        processed = trade_copier.poll_and_copy_trades(
            session, max_messages=2, redis_client=redis
        )

    assert processed == 2
    assert set(redis.acks) == {b"2"}
    assert any("1" in r.message for r in caplog.records)

def test_poll_and_copy_trades_refreshes_child_credentials(monkeypatch):
    from sqlalchemy import Column, Integer, String, JSON, create_engine
    from sqlalchemy.orm import declarative_base, sessionmaker

    Base = declarative_base()

    class Account(Base):
        __tablename__ = "account"
        id = Column(Integer, primary_key=True)
        client_id = Column(String)
        broker = Column(String)
        role = Column(String)
        linked_master_id = Column(String)
        copy_status = Column(String)
        credentials = Column(JSON)
        user_id = Column(Integer)

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    setup = Session()
    master = Account(
        client_id="m",
        broker="stub",
        role="master",
        copy_status="On",
        user_id=1,
    )
    child = Account(
        client_id="c1",
        broker="stub",
        role="child",
        linked_master_id="m",
        copy_status="On",
        credentials={"access_token": "old"},
        user_id=1,
    )
    setup.add_all([master, child])
    setup.commit()
    setup.close()

    session = Session()
    updater = Session()

    class Redis:
        def __init__(self):
            self.calls = 0
            self.acks = []

        def xgroup_create(self, *args, **kwargs):
            pass

        def xreadgroup(self, group, consumer, streams, count, block):
            self.calls += 1
            if self.calls == 1:
                return [("trade_events", [(b"1", {b"master_id": b"m"})])]
            elif self.calls == 2:
                ch = updater.query(Account).filter_by(client_id="c1").one()
                ch.credentials = {"access_token": "new"}
                updater.commit()
                return [("trade_events", [(b"2", {b"master_id": b"m"})])]
            return []

        def xack(self, stream, group, msg_id):
            self.acks.append(msg_id)

    redis = Redis()

    monkeypatch.setattr(trade_copier, "Account", Account)

    def active_children_for_master(master, session):
        return session.query(Account).filter_by(
            role="child", linked_master_id=master.client_id, copy_status="On"
        ).all()

    monkeypatch.setattr(trade_copier, "active_children_for_master", active_children_for_master)

    tokens = []

    def processor(master, child, order):
        tokens.append(child.credentials.get("access_token"))

    processed = trade_copier.poll_and_copy_trades(
        session, processor=processor, max_messages=2, redis_client=redis
    )

    assert processed == 2
    assert tokens == ["old", "new"]
