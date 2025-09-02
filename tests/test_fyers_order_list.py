from brokers.fyers import FyersBroker


class DummyAPI:
    def __init__(self):
        self.orderbook_called = 0
        self.tradebook_called = 0

    def orderbook(self):
        self.orderbook_called += 1
        return {"s": "ok", "orderBook": []}

    def tradebook(self):
        self.tradebook_called += 1
        return {
            "s": "ok",
            "tradeBook": [
                {"id": "1", "side": 1, "type": 2, "symbol": "NSE:SBIN-EQ", "qty": 1}
            ],
        }


def test_manual_trades_produce_complete_events():
    br = FyersBroker("C1", "token")
    br.api = DummyAPI()

    result = br.get_order_list()

    assert result["status"] == "success"
    data = result["data"]
    assert len(data) == 1
    order = data[0]
    assert order["action"] == "BUY"
    assert order["order_type"] == "MARKET"
    assert order["exchange"] == "NSE"
    assert order["symbol"] == "SBIN-EQ"
    assert br.api.orderbook_called == 1
    assert br.api.tradebook_called == 1
