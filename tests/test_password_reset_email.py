import os
import sys
import tempfile
import pytest
from itsdangerous import URLSafeTimedSerializer
from flask import url_for

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("SECRET_KEY", "test")
os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
os.environ.setdefault("RUN_SCHEDULER", "0")
os.environ.setdefault("MAIL_SUPPRESS_SEND", "1")

import app as app_module


@pytest.fixture
def client():
    db_fd, db_path = tempfile.mkstemp()
    os.environ["DATABASE_URL"] = "sqlite:///" + db_path
    from importlib import reload
    reload(app_module)
    local_app = app_module.app
    local_db = app_module.db
    User = app_module.User
    local_app.config["TESTING"] = True
    with local_app.app_context():
        local_db.create_all()
        if not local_db.session.query(User).filter_by(email="test@example.com").first():
            user = User(email="test@example.com")
            user.set_password("secret")
            local_db.session.add(user)
            local_db.session.commit()
        with local_app.test_client() as client:
            yield client
        local_db.session.remove()
        local_db.drop_all()
    os.close(db_fd)
    os.unlink(db_path)


def test_password_reset_sends_email(client):
    app = app_module.app
    mail = app_module.mail
    with app.app_context():
        serializer = URLSafeTimedSerializer(app.secret_key)
        token = serializer.dumps("test@example.com", salt="password-reset")
        with app.test_request_context():
            expected_url = url_for("auth.reset_password", token=token, _external=True)
        with mail.record_messages() as outbox:
            resp = client.post("/request-password-reset", data={"email": "test@example.com"})
            assert resp.status_code == 302
            assert len(outbox) == 1
            assert outbox[0].recipients == ["test@example.com"]
            assert expected_url in outbox[0].body


def test_password_reset_handles_send_failure(client, monkeypatch):
    """A failure to dispatch email should not raise an error."""
    app = app_module.app
    mail = app_module.mail

    def fail_send(msg):
        raise ConnectionRefusedError

    monkeypatch.setattr(mail, "send", fail_send)
    resp = client.post("/request-password-reset", data={"email": "test@example.com"})
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/login")
