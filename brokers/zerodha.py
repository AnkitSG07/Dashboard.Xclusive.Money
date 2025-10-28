# brokers/zerodha.py
"""Zerodha broker adapter using KiteConnect."""

import inspect
import os
import re
import requests
import pyotp
import time
from urllib.parse import urlparse, parse_qs, urljoin

from .base import BrokerBase
from .symbol_map import get_symbol_for_broker_lazy

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
            api_key = os.environ.get("ZERODHA_API_KEY")
        if not api_key:
            raise ValueError("api_key is required for Zerodha.")

        self.api_key = api_key
        self.api_secret = api_secret
        self.request_token = request_token
        self.password = password
        self.totp_secret = totp_secret
        self.kite = KiteConnect(api_key=api_key)
        # Configure KiteConnect to use the broker's HTTP session so that all
        # network calls honour the same retry and timeout configuration as
        # ``BrokerBase``.  ``KiteConnect`` doesn't expose a timeout parameter on
        # its public methods, so we monkeypatch the session's ``request`` method
        # to supply a default timeout for every call.
        session = self.session
        orig_request = session.request

        def request_with_timeout(method, url, **kwargs):
            kwargs.setdefault("timeout", self.timeout)
            return orig_request(method, url, **kwargs)

        session.request = request_with_timeout
        # ``KiteConnect`` (v4+) exposes a ``reqsession`` attribute to allow
        # overriding the underlying ``requests`` session used for API calls.
        # Older versions provided a ``set_session`` method which no longer
        # exists, hence we assign the session directly to maintain
        # compatibility with the updated API.
        self.kite.reqsession = session

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

    def create_session(self, request_token, *, timeout=None):
        """Generate access token using request token from official login."""
        session_data = self._kite_call(
            self.kite.generate_session,
            request_token,
            api_secret=self.api_secret,
            timeout=timeout,
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
            allow_redirects=False,
            timeout=10,
        )
        # After successful 2FA we get redirected to the connect login URL with request_token
        # Extract request_token from either the redirect chain, the final URL or the JSON payload

        def _extract_request_token(response):
            if response is None:
                return None
            match = re.search(r"request_token=([^&]+)", getattr(response, "url", ""))
            if match:
                return match.group(1)
            try:
                data = response.json()
            except Exception:  # pragma: no cover - non-json response
                return None
            if isinstance(data, dict):
                token = data.get("data", {}).get("request_token")
                if not token:
                    token = data.get("request_token")
                return token
            return None

        redirect_status_codes = {301, 302, 303, 307, 308}
        allowed_redirect_hosts = {"kite.zerodha.com", "kite.trade"}

        def _follow_redirects_for_token(initial_response):
            responses = [initial_response]
            current = initial_response
            for _ in range(10):
                token = _extract_request_token(current)
                if token:
                    return token, responses

                if current.status_code not in redirect_status_codes:
                    break

                location = current.headers.get("Location")
                if not location:
                    break

                next_url = urljoin(current.url, location)
                parsed = urlparse(next_url)
                host = parsed.hostname
                if host and host not in allowed_redirect_hosts:
                    qs = parse_qs(parsed.query)
                    token = (qs.get("request_token") or [None])[0]
                    if token:
                        return token, responses
                    break

                current = session.get(
                    next_url,
                    allow_redirects=False,
                    timeout=10,
                )
                responses.append(current)

            return None, responses

        request_token, responses = _follow_redirects_for_token(resp)
        if request_token:
            return request_token

        resp = responses[-1]

        response_data = None
        try:
            response_data = resp.json()
        except Exception:  # pragma: no cover - non-json response
            response_data = None

        if (
            isinstance(response_data, dict)
            and response_data.get("status") == "success"
            and isinstance(response_data.get("data"), dict)
        ):
            follow_resp = session.get(
                self.kite.login_url(),
                allow_redirects=False,
                timeout=10,
            )
            request_token, responses = _follow_redirects_for_token(follow_resp)
            if request_token:
                return request_token
            resp = responses[-1]
            try:
                response_data = resp.json()
            except Exception:  # pragma: no cover - non-json response
                response_data = None

        message = None
        if isinstance(response_data, dict):
            message = response_data.get("message") or response_data.get("error")

        if not message:
            parsed = urlparse(resp.url)
            qs = parse_qs(parsed.query)
            message = (
                (qs.get("error") or qs.get("message") or [None])[0]
            )

        if not message:
            message = resp.text

        raise Exception(message or "TOTP login failed")

    def _kite_call(self, func, *args, timeout=None, **kwargs):
        """Call a KiteConnect API method ensuring a timeout is supplied."""
        timeout = timeout or self.timeout
        if "timeout" in inspect.signature(func).parameters:
            kwargs.setdefault("timeout", timeout)
        return func(*args, **kwargs)

    def ensure_token(self):
        """Refresh token if expired or invalid."""
        if self.access_token:
            # If token_time is unknown or older than 7 hours, verify by calling profile
            if not self.token_time or time.time() - self.token_time > 7 * 3600:
                try:
                    self._kite_call(self.kite.profile)
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

    def _normalize_product_type(self, product_type):
        if not product_type:
            return product_type
        pt = product_type.upper()
        if pt == "INTRADAY":
            return "MIS"
        return pt

    def _normalize_order_type(self, order_type):
        if not order_type:
            return order_type
        ot = order_type.upper()
        mapping = {
            "MKT": "MARKET",
            "L": "LIMIT",
        }
        return mapping.get(ot, ot)

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
        timeout=None,
        **extra,
    ):
        self.ensure_token()
        timeout = timeout or self.timeout
        # Map generic field aliases when explicit params are not supplied
        tradingsymbol = tradingsymbol or extra.pop("symbol", None)
        transaction_type = transaction_type or extra.pop("action", None)
        quantity = quantity or extra.pop("qty", None)
        product = extra.pop("product_type", product)
        product = self._normalize_product_type(product)
        order_type = self._normalize_order_type(order_type)
        exchange = exchange or extra.pop("exchange", None)

        mapping = get_symbol_for_broker_lazy(
            tradingsymbol or "", self.BROKER, exchange
        )
        tradingsymbol = mapping.get("trading_symbol", tradingsymbol)
        exchange = exchange or mapping.get("exchange", "NSE")

        # Normalise string parameters
        if isinstance(transaction_type, str):
            transaction_type = transaction_type.upper()
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
            order_id = self._kite_call(
                self.kite.place_order,
                variety=self.kite.VARIETY_REGULAR,
                timeout=timeout,
                **params,
            )
            return {"status": "success", "order_id": order_id}
        except Exception as e:  # pragma: no cover - network call
            return {"status": "failure", "error": str(e)}

    def get_order_list(self, *, timeout=None):
        self.ensure_token()
        timeout = timeout or self.timeout
        try:
            orders = self._kite_call(self.kite.orders, timeout=timeout)
            # In some cases an expired session may return a profile dict
            if isinstance(orders, dict) and "profile" in orders:
                # Force token refresh and retry once
                self.access_token = None
                self.ensure_token()
                orders = self._kite_call(self.kite.orders, timeout=timeout)
            if isinstance(orders, list):
                for o in orders:
                    o["symbol"] = o.pop("tradingsymbol", "")
                    o["action"] = o.pop("transaction_type", "")
                    o["qty"] = o.pop("quantity", 0)
            return {"status": "success", "data": orders}
        except Exception as e:  # pragma: no cover - network call
            return {"status": "failure", "error": str(e), "data": []}

    def list_orders(self, **kwargs):
        """Return a list of orders with canonical field names.

        Zerodha orders use ``tradingsymbol``/``transaction_type``/``quantity`` and
        other broker-specific keys.  This helper maps each order to the common
        structure used throughout the project and upper-cases status values.
        """

        resp = self.get_order_list(**kwargs)
        if isinstance(resp, dict):
            if resp.get("status") != "success":
                raise RuntimeError(resp.get("error") or "failed to fetch order list")
            orders = resp.get("data", [])
        else:
            orders = resp

        normalized = []
        for o in orders:
            if not isinstance(o, dict):
                continue
            order = dict(o)

            symbol = order.get("symbol") or order.get("tradingsymbol")
            if symbol is not None:
                order["symbol"] = str(symbol)

            action = order.get("action") or order.get("transaction_type")
            if action is not None:
                order["action"] = str(action).upper()

            qty = order.get("qty") or order.get("quantity")
            try:
                order["qty"] = int(qty)
            except (TypeError, ValueError):
                pass

            exchange = order.get("exchange")
            if exchange is not None:
                order["exchange"] = str(exchange).upper()

            order_type = order.get("order_type")
            if order_type is not None:
                order["order_type"] = str(order_type).upper()

            status = order.get("status")
            if status is not None:
                order["status"] = str(status).upper()

            order_id = order.get("order_id") or order.get("id")
            if order_id is not None:
                order["order_id"] = str(order_id)

            normalized.append(order)

        return normalized

    def cancel_order(self, order_id, *, timeout=None):
        self.ensure_token()
        timeout = timeout or self.timeout
        try:
            self._kite_call(
                self.kite.cancel_order,
                variety=self.kite.VARIETY_REGULAR,
                order_id=order_id,
                timeout=timeout,
            )
            return {"status": "success", "order_id": order_id}
        except Exception as e:  # pragma: no cover - network call
            return {"status": "failure", "error": str(e)}

    def get_positions(self, *, timeout=None):
        self.ensure_token()
        timeout = timeout or self.timeout
        try:
            positions = self._kite_call(self.kite.positions, timeout=timeout)
            return {"status": "success", "data": positions.get("net", [])}
        except Exception as e:  # pragma: no cover - network call
            return {"status": "failure", "error": str(e), "data": []}

    def get_profile(self, *, timeout=None):
        self.ensure_token()
        timeout = timeout or self.timeout
        try:
            profile = self._kite_call(self.kite.profile, timeout=timeout)
            return {"status": "success", "data": profile}
        except Exception as e:  # pragma: no cover - network call
            return {"status": "failure", "error": str(e), "data": None}

    def get_fund_limits(self, *, timeout=None):
        """Return Zerodha equity fund limits with available/used margin totals."""
        self.ensure_token()
        timeout = timeout or self.timeout

        def _sum_numeric(value):
            if isinstance(value, dict):
                total = 0.0
                found = False
                for item in value.values():
                    subtotal, has_value = _sum_numeric(item)
                    if has_value:
                        total += subtotal
                        found = True
                return total, found
            if isinstance(value, (list, tuple, set)):
                total = 0.0
                found = False
                for item in value:
                    subtotal, has_value = _sum_numeric(item)
                    if has_value:
                        total += subtotal
                        found = True
                return total, found
            try:
                return float(value), True
            except (TypeError, ValueError):
                return 0.0, False

        try:
            margins = self._kite_call(
                self.kite.margins, segment="equity", timeout=timeout
            )
            data = margins.get("data", margins) if isinstance(margins, dict) else {}
            equity = data.get("equity", data) if isinstance(data, dict) else {}
            available_section = (
                equity.get("available")
                if isinstance(equity, dict)
                else {}
            )
            utilised_section = None
            if isinstance(equity, dict):
                utilised_section = (
                    equity.get("utilised")
                    or equity.get("utilized")
                    or equity.get("utilisedMargin")
                )

            available_total, available_found = _sum_numeric(available_section or 0.0)
            utilised_total, utilised_found = _sum_numeric(utilised_section or 0.0)

            net_total = 0.0
            net_found = False
            if isinstance(equity, dict) and "net" in equity:
                net_total, net_found = _sum_numeric(equity.get("net"))

            if not available_found and isinstance(equity, dict):
                fallback_available = (
                    equity.get("availableMargin")
                    or equity.get("available")
                )
                available_total, available_found = _sum_numeric(
                    fallback_available or 0.0
                )

            if not utilised_found and isinstance(equity, dict):
                fallback_utilised = (
                    equity.get("usedMargin")
                    or equity.get("utilised")
                    or equity.get("utilized")
                )
                utilised_total, utilised_found = _sum_numeric(
                    fallback_utilised or 0.0
                )

            total_funds = net_total if net_found else available_total + utilised_total

            payload = {
                "availableMargin": available_total if available_found else 0.0,
                "usedMargin": utilised_total if utilised_found else 0.0,
                "totalFunds": total_funds if (available_found or utilised_found or net_found) else 0.0,
                "netCash": total_funds if (available_found or utilised_found or net_found) else 0.0,
            }

            return {"status": "success", "data": payload}
        except Exception as e:  # pragma: no cover - network call
            return {"status": "failure", "error": str(e), "data": None}

    def check_token_valid(self, *, timeout=None):
        self.ensure_token()
        try:
            self._kite_call(self.kite.profile, timeout=timeout)
            return True
        except Exception:  # pragma: no cover - network call
            self.access_token = None
            self.token_time = None
            try:
                self.ensure_token()
                self._kite_call(self.kite.profile, timeout=timeout)
                return True
            except Exception:  # pragma: no cover - network call
                return False

    def get_opening_balance(self, *, timeout=None):
        """Return available cash balance using Kite margins API."""
        self.ensure_token()
        timeout = timeout or self.timeout
        try:
            margins = self._kite_call(
                self.kite.margins, timeout=timeout, segment="equity"
            )
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
