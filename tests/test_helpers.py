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
