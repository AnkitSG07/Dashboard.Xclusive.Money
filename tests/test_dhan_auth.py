import json
import urllib.parse

from brokers.dhan import DhanBroker


class DummyResponse:
    def __init__(self, payload=None, status_code=200, text=None):
        self._payload = payload
        self.status_code = status_code
        self._text = text

    def raise_for_status(self):
        if 400 <= self.status_code:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        if self._payload is None:
            raise RuntimeError("No JSON payload available")
        return self._payload

    @property
    def text(self):
        if self._text is not None:
            return self._text
        return json.dumps(self._payload)


def test_generate_session_token_success(monkeypatch):
    sequence = []

    def fake_request(self, method, url, **kwargs):
        sequence.append(url)
        if "generate-consent" in url:
            return DummyResponse({"consentAppId": "consent-123", "status": "success"})
        if "jwt/token" in url:
            body = kwargs.get("data")
            assert body is not None
            decoded = json.loads(urllib.parse.unquote(body))
            decrypted = json.loads(self._decrypt_payload(decoded))
            assert decrypted["user_id"] == "user@example.com"
            assert decrypted["app_id"] == DhanBroker._DHAN_APP_ID
            return DummyResponse({}, text="{}")
        if "loginV2/login" in url:
            body = kwargs.get("data")
            assert body is not None
            decoded = json.loads(urllib.parse.unquote(body))
            decrypted = json.loads(self._decrypt_payload(decoded))
            assert decrypted["consent_app_id"] == "consent-123"
            response_payload = {
                "status": "success",
                "data": [
                    {
                        "token_id": "token-abc",
                        "salt": "salt-value",
                        "pass_type": "OT",
                    }
                ],
            }
            encrypted_response = self._encrypt_payload(response_payload)
            return DummyResponse(text=json.dumps({"data": encrypted_response}))
        if "consumeApp-consent" in url:
            return DummyResponse(
                {
                    "accessToken": "access-xyz",
                    "refreshToken": "refresh-xyz",
                    "expiryTime": "2030-01-01T00:00:00Z",
                }
            )
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr(DhanBroker, "_request", fake_request, raising=False)

    broker = DhanBroker(
        "CID",
        None,
        login_id="user@example.com",
        login_password="supersecret",
        api_key="app-id",
        api_secret="app-secret",
        totp_secret="JBSWY3DPEHPK3PXP",
    )

    assert broker.access_token == "access-xyz"
    assert broker.headers["access-token"] == "access-xyz"
    assert broker.refresh_token == "refresh-xyz"
    assert broker.token_expiry is not None and broker.token_expiry.startswith("2030-01-01")
    assert broker.persist_credentials["access_token"] == "access-xyz"
    assert any("generate-consent" in url for url in sequence)
    assert any("jwt/token" in url for url in sequence)
    assert any("loginV2/login" in url for url in sequence)


def test_check_token_valid_refreshes(monkeypatch):
    refresh_calls = {"count": 0}

    def fake_generate(self, force_refresh=False):
        refresh_calls["count"] += 1
        self._update_headers("new-access")
        self.token_time = "2025-01-01T00:00:00Z"
        self.refresh_token = "refresh-new"
        self._set_token_expiry("2030-01-01T00:00:00Z")
        self._record_persist_credentials()
        return self.access_token

    def fake_request(self, method, url, **kwargs):
        class _Resp(DummyResponse):
            def __init__(self):
                super().__init__({"clientId": "CID"})

        return _Resp()

    monkeypatch.setattr(DhanBroker, "generate_session_token", fake_generate)
    monkeypatch.setattr(DhanBroker, "_request", fake_request, raising=False)

    broker = DhanBroker(
        "CID",
        "old-token",
        login_id="login",
        login_password="password",
        api_key="app-id",
        api_secret="app-secret",
        totp_secret="JBSWY3DPEHPK3PXP",
        token_expiry="2000-01-01T00:00:00Z",
    )

    assert broker.check_token_valid() is True
    assert refresh_calls["count"] >= 1
