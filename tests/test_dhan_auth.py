from brokers.dhan import DhanBroker
from services import dhan_auth


class DummyResponse:
    def __init__(self, payload=None, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if 400 <= self.status_code:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        if self._payload is None:
            raise RuntimeError("No JSON payload available")
        return self._payload


def test_generate_session_token_success(monkeypatch):
    payload = {
        "accessToken": "access-xyz",
        "expiryTime": "2030-01-01T00:00:00Z",
        "dhanClientName": "Test Trader",
    }

    def fake_consume(**kwargs):  # noqa: ANN001
        assert kwargs["token_id"] == "token-abc"
        return payload

    monkeypatch.setattr(dhan_auth, "consume_consent", fake_consume)

    broker = DhanBroker(
        "CID",
        None,
        api_key="app-id",
        api_secret="app-secret",
    )

    token = broker.generate_session_token(token_id="token-abc")

    assert token == "access-xyz"
    assert broker.access_token == "access-xyz"
    assert broker.headers["access-token"] == "access-xyz"
    assert broker.token_expiry is not None and broker.token_expiry.startswith("2030-01-01")
    assert broker.dhan_client_name == "Test Trader"
    assert broker.persist_credentials["access_token"] == "access-xyz"


def test_check_token_valid_refreshes(monkeypatch):
    def fake_renew(**kwargs):  # noqa: ANN001
        assert kwargs["api_key"] == "app-id"
        assert kwargs["api_secret"] == "app-secret"
        return {
            "accessToken": "new-access",
            "expiryTime": "2030-01-01T00:00:00Z",
            "dhanClientName": "Refreshed Trader",
        }

    def fake_request(self, method, url, **kwargs):  # noqa: ANN001
        assert url.endswith("/profile")
        return DummyResponse({"dhanClientId": "CID", "tokenValidity": "2030-01-02T00:00:00Z"})

    monkeypatch.setattr(dhan_auth, "renew_token", fake_renew)
    monkeypatch.setattr(DhanBroker, "_request", fake_request, raising=False)

    broker = DhanBroker(
        "CID",
        "old-token",
        api_key="app-id",
        api_secret="app-secret",
        token_expiry="2000-01-01T00:00:00Z",
    )

    assert broker.check_token_valid() is True
    assert broker.access_token == "new-access"
    assert broker.token_expiry is not None and broker.token_expiry.startswith("2030-01-02")
    assert broker.dhan_client_name == "Refreshed Trader"
    assert broker.persist_credentials["token_expiry"].startswith("2030-01-02")



def test_renew_token_sends_json_body(monkeypatch):
    captured = {}

    def fake_post(url, **kwargs):  # noqa: ANN001
        captured["url"] = url
        captured["headers"] = kwargs.get("headers")
        captured["json"] = kwargs.get("json")
        captured["timeout"] = kwargs.get("timeout")
        return DummyResponse({"accessToken": "renewed", "expiryTime": "2030-01-01T00:00:00Z"})

    monkeypatch.setattr(dhan_auth.requests, "post", fake_post)

    payload = dhan_auth.renew_token(
        access_token="  token-123  ",
        client_id="  CID  ",
        api_base="https://example.com/v2",
        timeout=5.0,
    )

    assert payload["accessToken"] == "renewed"
    assert captured["url"] == "https://example.com/v2/RenewToken"
    assert captured["timeout"] == 5.0
    assert captured["json"] == {"clientId": "CID", "dhanClientId": "CID"}
    assert captured["headers"] == {
        "access-token": "token-123",
        "dhanClientId": "CID",
        "clientId": "CID",
        "Content-Type": "application/json",
    }




def test_renew_token_includes_optional_auth_headers(monkeypatch):
    captured = {}

    def fake_post(url, **kwargs):  # noqa: ANN001
        captured["headers"] = kwargs.get("headers")
        return DummyResponse({"accessToken": "renewed", "expiryTime": "2030-01-01T00:00:00Z"})

    monkeypatch.setattr(dhan_auth.requests, "post", fake_post)

    dhan_auth.renew_token(
        access_token="token-123",
        client_id="CID",
        api_key="app-id",
        api_secret="app-secret",
    )

    assert captured["headers"]["app_id"] == "app-id"
    assert captured["headers"]["app_secret"] == "app-secret"
    assert captured["headers"]["clientId"] == "CID"



def test_broker_trims_credentials_and_headers(monkeypatch):
    captured = {}

    def fake_renew(**kwargs):  # noqa: ANN001
        captured.update(kwargs)
        return {
            "accessToken": "  refreshed-token  ",
            "expiryTime": "2031-01-01T00:00:00Z",
            "dhanClientName": " Trader One ",
        }

    monkeypatch.setattr(dhan_auth, "renew_token", fake_renew)

    broker = DhanBroker(
        "  CID123  ",
        "  token-abc  ",
        api_key="  app-id  ",
        api_secret="  app-secret  ",
    )

    assert broker.client_id == "CID123"
    assert broker.access_token == "token-abc"
    assert broker.headers["access-token"] == "token-abc"
    assert broker.api_key == "app-id"
    assert broker.api_secret == "app-secret"

    token = broker.generate_session_token(force_refresh=True)

    assert token == "refreshed-token"
    assert broker.access_token == "refreshed-token"
    assert broker.headers["access-token"] == "refreshed-token"
    assert broker.persist_credentials["access_token"] == "refreshed-token"
    assert broker.token_expiry is not None and broker.token_expiry.startswith("2031-01-01")
    assert broker.dhan_client_name == "Trader One"

    assert captured["access_token"] == "token-abc"
    assert captured["client_id"] == "CID123"
    assert captured["api_key"] == "app-id"
    assert captured["api_secret"] == "app-secret"
