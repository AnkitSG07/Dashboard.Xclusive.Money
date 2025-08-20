import os
import sys
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import app as app_module
from .test_api import client, login


def test_dhan_order_parsing_handles_quantity_and_price(client, monkeypatch):
    login(client)
    app = app_module.app
    db = app_module.db
    User = app_module.User
    Account = app_module.Account

    with app.app_context():
        user = User.query.filter_by(email="test@example.com").first()
        acc = Account(
            user_id=user.id,
            client_id="DH1",
            broker="dhan",
            credentials={"access_token": "token"},
        )
        db.session.add(acc)
        db.session.commit()

    sample_order = {
        "orderId": "1",
        "status": "COMPLETE",
        "orderQty": 1,
        "quantity": 999,  # price-related field that should not be used as qty
        "price": 123.45,
        "tradedPrice": 123.45,
        "tradingSymbol": "IDEA",
        "transactionType": "BUY",
    }

    class DummyBroker:
        def get_order_list(self):
            return [sample_order]

    monkeypatch.setattr(app_module, "broker_api", lambda acc: DummyBroker())

    resp = client.get("/api/order-book/DH1")
    assert resp.status_code == 200
    data = resp.get_json()
    assert isinstance(data, list) and len(data) == 1
    order = data[0]
    assert order["placed_qty"] == 1
    assert order["avg_price"] == sample_order["price"]
