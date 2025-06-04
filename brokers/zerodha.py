# brokers/zerodha.py
"""Zerodha broker adapter using KiteConnect."""

import requests
import pyotp

from .base import BrokerBase

try:
    from kiteconnect import KiteConnect
except ImportError:  # pragma: no cover - kiteconnect might not be installed during tests
    KiteConnect = None


class ZerodhaBroker(BrokerBase):
    """Adapter for Zerodha KiteConnect API."""
    BASE_URL = "https://kite.zerodha.com/api"

    def __init__(self, client_id, access_token=None, api_key=None,
                 api_secret=None, password=None, totp_secret=None, **kwargs):
        super().__init__(client_id, access_token or "", **kwargs)
        if KiteConnect is None:
            raise ImportError("kiteconnect not installed")
        if not api_key:
            raise ValueError("api_key is required for Zerodha.")

        self.api_key = api_key
        self.api_secret = api_secret
        self.password = password
        self.totp_secret = totp_secret

        self.kite = KiteConnect(api_key=api_key)
        if access_token:
            self.kite.set_access_token(access_token)
            self.access_token = access_token
        elif all([password, totp_secret, api_secret]):
            self.access_token = self.create_session()
            self.kite.set_access_token(self.access_token)
        else:
            raise ValueError("access_token or login credentials required for Zerodha.")

    def get_totp(self):
        """Return current TOTP using the secret."""
        return pyotp.TOTP(self.totp_secret).now()

    def create_session(self):
        """Login using password and TOTP to obtain an access token."""
        sess = requests.Session()
        r = sess.post(f"{self.BASE_URL}/login", data={
            "user_id": self.client_id,
            "password": self.password
        }, timeout=10)
        resp = r.json()
        if resp.get("status") != "success":
            raise Exception(resp.get("message", "Login failed"))

        request_id = resp["data"]["request_id"]
        twofa = self.get_totp()
        r = sess.post(f"{self.BASE_URL}/twofa", data={
            "user_id": self.client_id,
            "request_id": request_id,
            "twofa_value": twofa,
            "twofa_type": "totp"
        }, timeout=10)
        resp = r.json()
        if resp.get("status") != "success":
            raise Exception(resp.get("message", "TwoFA failed"))

        request_token = resp["data"]["request_token"]
        session_data = self.kite.generate_session(
            request_token,
            api_secret=self.api_secret,
        )
        return session_data["access_token"]

    # ================= Standard BrokerBase methods ==================
    def place_order(
        self,
        tradingsymbol,
        exchange,
        transaction_type,
        quantity,
        order_type="MARKET",
        product="MIS",
        price=None,
        **extra,
    ):
        params = {
            "tradingsymbol": tradingsymbol,
            "exchange": exchange,
            "transaction_type": transaction_type.upper(),
            "quantity": int(quantity),
            "order_type": order_type.upper(),
            "product": product.upper(),
        }
        if order_type.upper() == "LIMIT" and price is not None:
            params["price"] = float(price)
        try:
            order_id = self.kite.place_order(
                variety=self.kite.VARIETY_REGULAR,
                **params
            )
            return {"status": "success", "order_id": order_id}
        except Exception as e:  # pragma: no cover - network call
            return {"status": "failure", "error": str(e)}

    def get_order_list(self):
        try:
            orders = self.kite.orders()
            return {"status": "success", "data": orders}
        except Exception as e:  # pragma: no cover - network call
            return {"status": "failure", "error": str(e), "data": []}

    def cancel_order(self, order_id):
        try:
            self.kite.cancel_order(
                variety=self.kite.VARIETY_REGULAR,
                order_id=order_id
            )
            return {"status": "success", "order_id": order_id}
        except Exception as e:  # pragma: no cover - network call
            return {"status": "failure", "error": str(e)}

    def get_positions(self):
        try:
            positions = self.kite.positions()
            return {"status": "success", "data": positions.get("net", [])}
        except Exception as e:  # pragma: no cover - network call
            return {"status": "failure", "error": str(e), "data": []}

    def get_profile(self):
        try:
            profile = self.kite.profile()
            return {"status": "success", "data": profile}
        except Exception as e:  # pragma: no cover - network call
            return {"status": "failure", "error": str(e), "data": None}

    def check_token_valid(self):
        try:
            self.kite.profile()
            return True
        except Exception:  # pragma: no cover - network call
            return False
