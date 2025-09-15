from types import SimpleNamespace

from helpers import active_children_for_master
from models import Account


def test_active_children_for_master_uses_session():
    class DummySession:
        def __init__(self):
            self.model = None
        def query(self, model):
            self.model = model
            return self
        def filter(self, *args, **kwargs):
            return self
        def all(self):
            return []

    session = DummySession()
    master = SimpleNamespace(user_id=1, client_id="M1")

    active_children_for_master(master, session)

    assert session.model is Account

def test_active_children_for_master_logs_exclusions(caplog):
    children = [
        SimpleNamespace(client_id="c1", copy_status="Off", system_errors=[], credentials={"access_token": "t"}),
        SimpleNamespace(client_id="c2", copy_status="On", system_errors=["oops"], credentials={"access_token": "t"}),
        SimpleNamespace(client_id="c3", copy_status="On", system_errors=[], credentials={}),
    ]

    class DummySession:
        def query(self, model):
            return self

        def filter(self, *args, **kwargs):
            return self

        def all(self):
            return children

    master = SimpleNamespace(client_id="M1")

    with caplog.at_level("INFO", logger="helpers"):
        active = active_children_for_master(master, DummySession())

    assert active == []
    messages = [r.getMessage() for r in caplog.records]
    assert any("copy status" in m for m in messages)
    assert any("system errors" in m for m in messages)
    assert any("credentials" in m for m in messages)

def test_active_children_for_master_allows_aliceblue_api_key():
    """Children using Alice Blue should be considered active when they
    provide an API key even if no access token is present."""

    child = SimpleNamespace(
        client_id="c1",
        broker="aliceblue",
        copy_status="On",
        system_errors=[],
        credentials={"api_key": "k"},
    )

    class DummySession:
        def query(self, model):
            return self

        def filter(self, *args, **kwargs):
            return self

        def all(self):
            return [child]

    master = SimpleNamespace(client_id="M1")

    active = active_children_for_master(master, DummySession())
    assert active == [child]
