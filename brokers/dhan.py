# brokers/dhan.py
import requests
from .base import BrokerBase

class DhanBroker(BrokerBase):
    NSE = "NSE_EQ"
    INTRA = "INTRADAY"
    BUY = "BUY"
    SELL = "SELL"
    MARKET = "MARKET"
    LIMIT = "LIMIT"

    def __init__(self, client_id, access_token, **kwargs):
        super().__init__(client_id, access_token, **kwargs)
        self.api_base = "https://api.dhan.co/v2"
        self.headers = {
            "access-token": access_token,
            "Content-Type": "application/json"
        }

    def place_order(self, security_id, exchange_segment, transaction_type, quantity, order_type, product_type, price=0, **extra):
        payload = {
            "dhanClientId": self.client_id,
            "securityId": security_id,
            "exchangeSegment": exchange_segment,
            "transactionType": transaction_type,
            "productType": product_type,
            "orderType": order_type,
            "quantity": int(quantity),
            "validity": "DAY",
            "price": float(price) if price else 0,
            "triggerPrice": "",
            "afterMarketOrder": False,
        }
        # Optionally handle disclosedQuantity, boProfitValue, boStopLossValue, etc.
        r = requests.post(f"{self.api_base}/orders", json=payload, headers=self.headers, timeout=10)
        try:
            resp = r.json()
        except Exception:
            resp = {"status": "failure", "error": r.text}
        if "orderId" in resp:
            return {"status": "success", **resp}
        return {"status": "failure", **resp}

    def get_order_list(self):
        r = requests.get(f"{self.api_base}/orders", headers=self.headers, timeout=10)
        try:
            return {"status": "success", "data": r.json()}
        except Exception:
            return {"status": "failure", "error": r.text}

    def cancel_order(self, order_id):
        r = requests.delete(f"{self.api_base}/orders/{order_id}", headers=self.headers, timeout=10)
        try:
            return {"status": "success", "data": r.json()}
        except Exception:
            return {"status": "failure", "error": r.text}

    def get_positions(self):
        # Implement as per Dhan docs (usually /positions endpoint)
        return {"status": "failure", "error": "Not Implemented"}

    # ... Implement other methods as required ...
