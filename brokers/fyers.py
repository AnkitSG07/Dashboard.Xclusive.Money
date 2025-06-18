from .base import BrokerBase
import hashlib
import requests

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

    def get_opening_balance(self):
        try:
            funds = self.api.funds()
            data = funds.get("fund_limit", funds.get("data", funds))
            for key in ["equityAmount", "cash", "available_balance", "availableCash"]:
                if key in data:
                    return float(data[key])
            return None
        except Exception:
            return None
        # ----- Authentication Helpers -----
    @classmethod
    def login_url(cls, client_id, secret_key, redirect_uri, state="state123"):
        """Return the Fyers OAuth login URL."""
        if fyersModel is None:
            raise ImportError("fyers-apiv3 not installed")
        session = fyersModel.SessionModel(
            client_id=client_id,
            secret_key=secret_key,
            redirect_uri=redirect_uri,
            response_type="code",
            state=state,
        )
        return session.generate_authcode()

    @classmethod
    def exchange_code_for_token(cls, client_id, secret_key, auth_code):
        """Exchange auth code for access and refresh tokens."""
        if fyersModel is None:
            raise ImportError("fyers-apiv3 not installed")
        session = fyersModel.SessionModel(
            client_id=client_id,
            secret_key=secret_key,
            grant_type="authorization_code",
        )
        session.set_token(auth_code)
        return session.generate_token()

    @classmethod
    def refresh_access_token(cls, client_id, secret_key, refresh_token, pin):
        """Refresh the access token using refresh token and pin."""
        app_hash = hashlib.sha256(f"{client_id}:{secret_key}".encode()).hexdigest()
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

