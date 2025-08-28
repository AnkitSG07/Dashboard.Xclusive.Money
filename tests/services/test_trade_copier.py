import asyncio
import time
from types import SimpleNamespace

from services import trade_copier
from services.trade_copier import _replicate_to_children


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
    master = SimpleNamespace(client_id="m")
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
    master = SimpleNamespace(client_id="m")
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
    master = SimpleNamespace(client_id="m")
    children = [
        SimpleNamespace(broker="mock", client_id="c1", credentials={}, multiplier=1),
        SimpleNamespace(broker="mock", client_id="c2", credentials={}, multiplier=1),
    ]
    order = {"symbol": "AAPL", "action": "BUY", "qty": 1}

    monkeypatch.setattr(
        trade_copier, "active_children_for_master", lambda m: children
    )

    async def measure(limit):
        start = time.perf_counter()
        await _replicate_to_children(None, master, order, slow_processor, max_workers=limit)
        return time.perf_counter() - start

    t_serial = asyncio.run(measure(1))
    t_parallel = asyncio.run(measure(2))

    assert t_serial > t_parallel


def test_replicate_to_children_isolates_child_errors(monkeypatch, caplog):
    master = SimpleNamespace(client_id="m")
    children = [
        SimpleNamespace(broker="mock", client_id="c1", credentials={}, multiplier=1),
        SimpleNamespace(broker="mock", client_id="c2", credentials={}, multiplier=1),
    ]
    order = {"symbol": "AAPL", "action": "BUY", "qty": 1}
    
    monkeypatch.setattr(
        trade_copier, "active_children_for_master", lambda m: children
    )

    executed = []

    def processor(master, child, order):
        if child.client_id == "c1":
            raise RuntimeError("fail")
        executed.append(child.client_id)

    asyncio.run(
        _replicate_to_children(None, master, order, processor, max_workers=2)
    )

    assert executed == ["c2"]
    assert any("child copy failed" in r.message for r in caplog.records)


def test_replicate_to_children_enforces_timeout(monkeypatch):
    master = SimpleNamespace(client_id="m")
    children = [
        SimpleNamespace(broker="mock", client_id="fast", credentials={}, multiplier=1),
        SimpleNamespace(broker="mock", client_id="slow", credentials={}, multiplier=1),
    ]

    order = {"symbol": "AAPL", "action": "BUY", "qty": 1}

    monkeypatch.setattr(
        trade_copier, "active_children_for_master", lambda m: children
    )
    
    def processor(master, child, order):
        if child.client_id == "slow":
            time.sleep(0.2)
        else:
            time.sleep(0.01)

    start = time.perf_counter()
    asyncio.run(
        _replicate_to_children(
            None, master, order, processor, max_workers=2, timeout=0.05
        )
    )
    duration = time.perf_counter() - start

    assert duration < 0.15


def test_replicate_to_children_handles_case_insensitive_status(monkeypatch):
    master = SimpleNamespace(client_id="m")
    children = [
        SimpleNamespace(broker="mock", client_id="c1", credentials={}, multiplier=1, copy_status="ON"),
        SimpleNamespace(broker="mock", client_id="c2", credentials={}, multiplier=1, copy_status="oN"),
        SimpleNamespace(broker="mock", client_id="c3", credentials={}, multiplier=1, copy_status="Off"),
    ]
    order = {"symbol": "AAPL", "action": "BUY", "qty": 1}

    monkeypatch.setattr(
        trade_copier,
        "active_children_for_master",
        lambda m: [c for c in children if c.copy_status.lower() == "on"],
    )

    processed = []

    def processor(master, child, order):
        processed.append(child.client_id)

    asyncio.run(_replicate_to_children(None, master, order, processor))

    assert processed == ["c1", "c2"]


def test_poll_and_copy_trades_reads_max_workers_env(monkeypatch):
    master = SimpleNamespace(client_id="m")
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


def test_poll_and_copy_trades_ack_on_child_error(monkeypatch, caplog):
    master = SimpleNamespace(client_id="m")
    children = [
        SimpleNamespace(broker="mock", client_id="good", credentials={}, multiplier=1),
        SimpleNamespace(broker="mock", client_id="bad", credentials={}, multiplier=1),
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
        trade_copier, "active_children_for_master", lambda m: children
    )

    trade_copier.poll_and_copy_trades(
        Session(master, children),
        processor=processor,
        max_messages=1,
        redis_client=redis,
    )

    assert processed == ["good"]
    assert redis.acks == [b"1"]
    assert any("child copy failed" in r.message for r in caplog.records)


def test_poll_and_copy_trades_logs_task_errors(monkeypatch, caplog):
    master = SimpleNamespace(client_id="m")
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
    assert set(redis.acks) == {b"1", b"2"}
    assert any("1" in r.message for r in caplog.records)
