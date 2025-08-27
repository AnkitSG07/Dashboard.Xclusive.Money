# brokers/zerodha.py
"""Zerodha broker adapter using KiteConnect."""

import re
import requests
import pyotp
import time
from urllib.parse import urlparse, parse_qs

from .base import BrokerBase
from .symbol_map import get_symbol_for_broker

try:
    from kiteconnect import KiteConnect
except ImportError:  # pragma: no cover - kiteconnect might not be installed during tests
    KiteConnect = None


class ZerodhaBroker(BrokerBase):
    BROKER = "zerodha"
    """Adapter for Zerodha KiteConnect API."""
    BASE_URL = "https://kite.zerodha.com/api"

    def __init__(self, client_id, access_token=None, api_key=None,
                 api_secret=None, request_token=None, password=None,
                 totp_secret=None, token_time=None, **kwargs):
        super().__init__(client_id, access_token or "", **kwargs)
        if KiteConnect is None:
            raise ImportError("kiteconnect not installed")
        if not api_key:
            raise ValueError("api_key is required for Zerodha.")

        self.api_key = api_key
        self.api_secret = api_secret
        self.request_token = request_token
        self.password = password
        self.totp_secret = totp_secret
        self.kite = KiteConnect(api_key=api_key)
        self.token_time = token_time
        if access_token:
            self.kite.set_access_token(access_token)
            self.access_token = access_token
            if self.token_time is None:
                self.token_time = time.time()
        elif request_token and api_secret:
            self.access_token = self.create_session(request_token)
            self.kite.set_access_token(self.access_token)
            self.token_time = time.time()
        elif password and totp_secret and api_secret:
            req_token = self._login_with_totp(password, totp_secret)
            self.access_token = self.create_session(req_token)
            self.kite.set_access_token(self.access_token)
            self.token_time = time.time()
        
        else:
            raise ValueError("access_token or login credentials required for Zerodha.")

    def login_url(self):
        """Return the official login URL for generating request token."""
        return self.kite.login_url()

    def create_session(self, request_token):
        """Generate access token using request token from official login."""
        session_data = self.kite.generate_session(
            request_token,
            api_secret=self.api_secret,
        )
        return session_data["access_token"]

    def _login_with_totp(self, password, totp_secret):
        """Return request token by performing Zerodha login with TOTP."""
        session = requests.Session()
        # Step 1: Submit user_id and password
        resp = session.post(
            f"{self.BASE_URL}/login",
            data={"user_id": self.client_id, "password": password},
            timeout=10,
        )
        data = resp.json()
        if data.get("status") != "success":
            raise Exception(data.get("message", "Login failed"))

        request_id = data["data"].get("request_id")
        twofa_type = data["data"].get("twofa_type", "totp")
        totp = pyotp.TOTP(totp_secret).now()

        # Step 2: Submit TOTP
        resp = session.post(
            f"{self.BASE_URL}/twofa",
            data={
                "user_id": self.client_id,
                "request_id": request_id,
                "twofa_value": totp,
                "twofa_type": twofa_type,
            },
            allow_redirects=True,
            timeout=10,
        )
        # After successful 2FA we get redirected to the connect login URL with request_token
        # Extract request_token from the final URL
        match = re.search(r"request_token=([^&]+)", resp.url)
        if not match:
            message = None
            try:
                data = resp.json()
                if isinstance(data, dict):
                    message = data.get("message") or data.get("error")
            except Exception:  # pragma: no cover - non-json response
                pass

            if not message:
                parsed = urlparse(resp.url)
                qs = parse_qs(parsed.query)
                message = (
                    (qs.get("error") or qs.get("message") or [None])[0]
                )

            if not message:
                message = resp.text

            raise Exception(message or "TOTP login failed")
        return match.group(1)

    def ensure_token(self):
        """Refresh token if expired or invalid."""
        if self.access_token:
            # If token_time is unknown or older than 7 hours, verify by calling profile
            if not self.token_time or time.time() - self.token_time > 7 * 3600:
                try:
                    self.kite.profile()
                    if not self.token_time:
                        self.token_time = time.time()
                    return
                except Exception:
                    self.access_token = None
            else:
                return

        # If we reach here, no valid token is present - attempt to create one
        if self.request_token:
            self.access_token = self.create_session(self.request_token)
            self.kite.set_access_token(self.access_token)
            self.token_time = time.time()
            return

        if self.password and self.totp_secret:
            req = self._login_with_totp(self.password, self.totp_secret)
            self.access_token = self.create_session(req)
            self.kite.set_access_token(self.access_token)
            self.token_time = time.time()
            return

    # ================= Standard BrokerBase methods ==================
    def place_order(
        self,
        tradingsymbol=None,
        exchange=None,
        transaction_type=None,
        quantity=None,
        order_type="MARKET",
        product="MIS",
        price=None,
        **extra,
    ):
        self.ensure_token()
        # Map generic field aliases when explicit params are not supplied
        tradingsymbol = tradingsymbol or extra.pop("symbol", None)
        transaction_type = transaction_type or extra.pop("action", None)
        quantity = quantity or extra.pop("qty", None)
        product = extra.pop("product_type", product)
        exchange = exchange or extra.pop("exchange", None)

        mapping = get_symbol_for_broker(tradingsymbol or "", self.BROKER)
        tradingsymbol = mapping.get("trading_symbol", tradingsymbol)
        exchange = exchange or mapping.get("exchange", "NSE")

        # Normalise string parameters
        if isinstance(transaction_type, str):
            transaction_type = transaction_type.upper)
        if isinstance(order_type, str):
            order_type = order_type.upper()
        if isinstance(product, str):
            product = product.upper()
        if isinstance(exchange, str):
            exchange = exchange.upper()

        params = {
            "tradingsymbol": tradingsymbol,
            "exchange": exchange,
            "transaction_type": transaction_type,
            "quantity": int(quantity),
            "order_type": order_type,
            "product": product,
        }
        if order_type == "LIMIT" and price is not None:
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
        self.ensure_token()
        try:
            orders = self.kite.orders()
            # In some cases an expired session may return a profile dict
            if isinstance(orders, dict) and "profile" in orders:
                # Force token refresh and retry once
                self.access_token = None
                self.ensure_token()
                orders = self.kite.orders()
            return {"status": "success", "data": orders}
        except Exception as e:  # pragma: no cover - network call
            return {"status": "failure", "error": str(e), "data": []}

    def cancel_order(self, order_id):
        self.ensure_token()
        try:
            self.kite.cancel_order(
                variety=self.kite.VARIETY_REGULAR,
                order_id=order_id
            )
            return {"status": "success", "order_id": order_id}
        except Exception as e:  # pragma: no cover - network call
            return {"status": "failure", "error": str(e)}

    def get_positions(self):
        self.ensure_token()
        try:
            positions = self.kite.positions()
            return {"status": "success", "data": positions.get("net", [])}
        except Exception as e:  # pragma: no cover - network call
            return {"status": "failure", "error": str(e), "data": []}

    def get_profile(self):
        self.ensure_token()
        try:
            profile = self.kite.profile()
            return {"status": "success", "data": profile}
        except Exception as e:  # pragma: no cover - network call
            return {"status": "failure", "error": str(e), "data": None}

    def check_token_valid(self):
        try:
            self.ensure_token()
            self.kite.profile()
            return True
        except Exception:  # pragma: no cover - network call
            return False

    def get_opening_balance(self):
        """Return available cash balance using Kite margins API."""
        self.ensure_token()
        try:
            margins = self.kite.margins(segment="equity")
            data = margins.get("data", margins)
            if isinstance(data, dict):
                eq = data.get("equity", data)
                avail = eq.get("available", eq)
                for key in ["cash", "liveBalance", "openingBalance"]:
                    if key in avail:
                        return float(avail[key])
            return None
        except Exception:
            return None
