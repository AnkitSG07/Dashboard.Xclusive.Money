import threading
from werkzeug.serving import make_server

from brokers.client import BrokerServiceClient
from brokers.service import create_broker_service
from brokers import service as service_module


class StubBroker:
    """Simple broker implementation for testing."""
    orders = []

    def __init__(self, client_id, access_token, symbol_map=None):
        self.client_id = client_id
        self.access_token = access_token

    def place_order(self, **order):
        StubBroker.orders.append(order)
        return {"status": "ok", **order}

    def get_positions(self):
        return {"positions": [1, 2, 3]}


def test_service_client_roundtrip(monkeypatch):
    # Ensure service uses our stub broker implementation
    monkeypatch.setattr(service_module, "_load_broker_class", lambda name: StubBroker)
    app = create_broker_service("stub")

    server = make_server("localhost", 0, app)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        base_url = f"http://localhost:{server.server_port}"
        client = BrokerServiceClient(base_url, "id", "token")

        result = client.place_order(symbol="AAPL", action="BUY")
        assert result["status"] == "ok"
        assert StubBroker.orders == [{"symbol": "AAPL", "action": "BUY"}]

        pos = client.get_positions()
        assert pos == {"positions": [1, 2, 3]}
    finally:
        server.shutdown()
