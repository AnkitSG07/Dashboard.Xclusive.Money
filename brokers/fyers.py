from .base import BrokerBase

try:
    from fyers_apiv3 import fyersModel
except ImportError:
    fyersModel = None

class FyersBroker(BrokerBase):
    def __init__(self, client_id, access_token, **kwargs):
        super().__init__(client_id, access_token, **kwargs)
        if fyersModel is None:
            raise ImportError("fyers-apiv3 not installed")
        self.api = fyersModel.FyersModel(token=access_token, client_id=client_id)

    def place_order(self, tradingsymbol, exchange, transaction_type, quantity, order_type="MARKET", product="INTRADAY", price=None):
        try:
            data = {
                "symbol": f"NSE:{tradingsymbol.upper()}-EQ",
                "qty": int(quantity),
                "type": 2 if order_type.upper() == "MARKET" else 1,
                "side": 1 if transaction_type.upper() == "BUY" else -1,
                "productType": product,
                "limitPrice": float(price) if price else 0,
                "disclosedQty": 0,
                "validity": "DAY",
                "offlineOrder": False,
                "stopPrice": 0
            }
            result = self.api.place_order(data=data)
            return {"status": "success" if result["s"] == "ok" else "failure", "order_id": result.get("id", None), "error": result.get("message")}
        except Exception as e:
            return {"status": "failure", "error": str(e)}

    def get_order_list(self):
        try:
            orders = self.api.get_orders()
            return {"status": "success", "data": orders["data"] if "data" in orders else []}
        except Exception as e:
            return {"status": "failure", "error": str(e), "data": []}

    def get_positions(self):
        try:
            positions = self.api.positions()
            return {"status": "success", "data": positions.get("netPositions", [])}
        except Exception as e:
            return {"status": "failure", "error": str(e), "data": []}

    def cancel_order(self, order_id):
        try:
            result = self.api.cancel_order({"id": order_id})
            return {"status": "success", "order_id": order_id}
        except Exception as e:
            return {"status": "failure", "error": str(e)}

    def get_profile(self):
        try:
            profile = self.api.get_profile()
            return {"status": "success", "data": profile}
        except Exception as e:
            return {"status": "failure", "error": str(e), "data": None}

    def check_token_valid(self):
        try:
            self.api.get_profile()
            return True
        except Exception:
            return False

