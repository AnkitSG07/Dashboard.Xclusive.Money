from .base import BrokerBase
import hashlib
import requests
from urllib.parse import urlencode

try:
    from fyers_apiv3 import fyersModel
except ImportError:
    fyersModel = None

class FyersBroker(BrokerBase):
    def __init__(self, client_id, access_token, **kwargs):
        super().__init__(client_id, access_token, **kwargs)
        if fyersModel is not None:
            self.api = fyersModel.FyersModel(token=access_token, client_id=client_id)
        else:
            # Library not installed; minimal HTTP fallback
            self.api = None
            self.session = requests.Session()
            self.session.headers.update({"Authorization": f"{client_id}:{access_token}"})


    def place_order(self, tradingsymbol, exchange, transaction_type, quantity, order_type="MARKET", product="INTRADAY", price=None):
        if self.api is None:
            raise RuntimeError("fyers-apiv3 not installed")
        try:
            product_map = {
                "MIS": "INTRADAY",
                "INTRA": "INTRADAY",
                "NRML": "MARGIN",
                "DELIVERY": "CNC",
            }
            prod = product_map.get(str(product).upper(), str(product).upper())
            fy_type = 2 if order_type.upper() == "MARKET" else 1

            if fy_type == 1:
                if price is None:
                    raise ValueError("price is required for LIMIT orders")
                limit_price = float(price)
            else:
                limit_price = 0

            symbol = tradingsymbol
            if ":" in symbol:
                symbol = symbol.upper()
            else:
                exch = str(exchange or "NSE").upper()
                symbol = f"{exch}:{symbol.upper()}"
                if exch in {"NSE", "BSE"} and not symbol.endswith("-EQ"):
                    symbol = f"{symbol}-EQ"


            data = {
                "symbol": symbol,
                "qty": int(quantity),
                "type": fy_type,
                "side": 1 if transaction_type.upper() == "BUY" else -1,
                "productType": prod,
                "limitPrice": limit_price,
                "disclosedQty": 0,
                "validity": "DAY",
                "offlineOrder": False,
                "stopPrice": 0,
            }
            result = self.api.place_order(data=data)
            return {"status": "success" if result["s"] == "ok" else "failure", "order_id": result.get("id", None), "error": result.get("message")}
        except Exception as e:
            return {"status": "failure", "error": str(e)}

    def get_order_list(self):
        if self.api is None:
            raise RuntimeError("fyers-apiv3 not installed")

        try:
            orders = self.api.get_orders()
            return {"status": "success", "data": orders["data"] if "data" in orders else []}
        except Exception as e:
            return {"status": "failure", "error": str(e), "data": []}

    def get_positions(self):
        if self.api is None:
            raise RuntimeError("fyers-apiv3 not installed")
        try:
            positions = self.api.positions()
            return {"status": "success", "data": positions.get("netPositions", [])}
        except Exception as e:
            return {"status": "failure", "error": str(e), "data": []}

    def cancel_order(self, order_id):
        if self.api is None:
            raise RuntimeError("fyers-apiv3 not installed")
        try:
            result = self.api.cancel_order({"id": order_id})
            return {"status": "success", "order_id": order_id}
        except Exception as e:
            return {"status": "failure", "error": str(e)}

    def get_profile(self):
        if self.api is None:
            raise RuntimeError("fyers-apiv3 not installed")
        try:
            profile = self.api.get_profile()
            return {"status": "success", "data": profile}
        except Exception as e:
            return {"status": "failure", "error": str(e), "data": None}

    def check_token_valid(self):
        if self.api is None:
            raise RuntimeError("fyers-apiv3 not installed")
        try:
            self.api.get_profile()
            return True
        except Exception:
            return False

    def get_opening_balance(self):
        if self.api is None:
            raise RuntimeError("fyers-apiv3 not installed")
        try:
            funds = self.api.funds()
            data = funds.get("fund_limit", funds.get("data", funds))
            for key in ["equityAmount", "cash", "available_balance", "availableCash"]:
                if key in data:
                    return float(data[key])

            items = funds.get("fund_limit") or funds.get("data") or []
            if isinstance(items, dict):
                items = [items]
            for item in items:
                title = str(item.get("title", "")).lower()
                if title in {"available balance", "clear balance"}:
                    for key in ["equityAmount", "cash"]:
                        if key in item:
                            return float(item[key])
            return None
        except Exception:
            return None
            
    # ----- Authentication Helpers -----
    @classmethod
    def login_url(cls, client_id, redirect_uri, state="state123"):
        """Return the Fyers OAuth login URL."""
        params = {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "state": state,
        }
        return f"https://api-t1.fyers.in/api/v3/generate-authcode?{urlencode(params)}"

    @classmethod
    def exchange_code_for_token(cls, client_id, secret_key, auth_code):
        """Exchange auth code for access and refresh tokens."""
        app_hash = hashlib.sha256(f"{client_id}:{secret_key}".encode()).hexdigest()
        payload = {
            "grant_type": "authorization_code",
            "appIdHash": app_hash,
            "code": auth_code,
        }
        resp = requests.post(
            "https://api-t1.fyers.in/api/v3/validate-authcode",
            json=payload,
            timeout=10,
        )
        return resp.json()

    @classmethod
    def refresh_access_token(cls, client_id, secret_key, refresh_token, pin):
        """Refresh the access token using refresh token and pin."""
        app_hash = hashlib.sha256(f"{client_id}{secret_key}".encode()).hexdigest()
        payload = {
            "grant_type": "refresh_token",
            "appIdHash": app_hash,
            "refresh_token": refresh_token,
            "pin": str(pin),
        }
        resp = requests.post(
            "https://api-t1.fyers.in/api/v3/validate-refresh-token",
            json=payload,
            timeout=10,
        )
        return resp.json()
