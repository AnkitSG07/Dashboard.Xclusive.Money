# brokers/dhan.py
import requests
from .base import BrokerBase
from .symbol_map import get_symbol_for_broker

class DhanBroker(BrokerBase):
    BROKER = "dhan"
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
        # self.symbol_map is already initialized by base class

    def place_order(
        self,
        tradingsymbol=None,
        security_id=None,
        exchange_segment=None,
        transaction_type=None,
        quantity=None,
        order_type="MARKET",
        product_type="INTRADAY",
        price=0,
        **extra
    ):
        # --- Symbol mapping ---
        mapping = get_symbol_for_broker(tradingsymbol or "", self.BROKER)
        if not security_id:
            if tradingsymbol and self.symbol_map:
                security_id = self.symbol_map.get(tradingsymbol.upper())
            if not security_id:
                security_id = mapping.get("security_id")
            if not security_id:
                raise Exception(f"DhanBroker: 'security_id' required (tradingsymbol={tradingsymbol})")

        if not exchange_segment:
            exchange_segment = mapping.get("exchange_segment", self.NSE)

        if not product_type:
            product_type = self.INTRA

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
        r = requests.get(f"{self.api_base}/positions", headers=self.headers, timeout=10)
        try:
            return {"status": "success", "data": r.json()}
        except Exception:
            return {"status": "failure", "error": r.text}

    
    def get_profile(self):
        """Return profile or fund data to confirm account id."""
        r = requests.get(f"{self.api_base}/fundlimit", headers=self.headers, timeout=5)
        try:
            return {"status": "success", "data": r.json()}
        except Exception:
            return {"status": "failure", "error": r.text}

    def check_token_valid(self):
        """Validate access token and ensure it belongs to this client_id."""
        try:
            r = requests.get(f"{self.api_base}/fundlimit", headers=self.headers, timeout=5)
            r.raise_for_status()
            data = r.json()
            cid = str(data.get("clientId") or data.get("dhanClientId") or "").strip()
            if cid and cid != str(self.client_id):
                return False
            return True
        except Exception:
            return False

    def get_opening_balance(self):
        """Fetch available cash balance from fundlimit API."""
        try:
            r = requests.get(f"{self.api_base}/fundlimit", headers=self.headers, timeout=5)
            data = r.json()
            for key in [
                "openingBalance",
                "netCashAvailable",
                "availableBalance",
                "availabelBalance",
                "withdrawableBalance",
                "availableAmount",
                "netCash",
            ]:
                if key in data:
                    return float(data[key])
            return float(data.get("cash", 0))
        except Exception:
            return None
