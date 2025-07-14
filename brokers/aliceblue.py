import requests
import hashlib
import json
import logging
from .base import BrokerBase
from .symbol_map import get_symbol_for_broker

logger = logging.getLogger(__name__)

class AliceBlueBroker(BrokerBase):
    BROKER = "aliceblue"
    BASE_URL = "https://ant.aliceblueonline.com/rest/AliceBlueAPIService/api/"

    def __init__(self, client_id, api_key, device_number=None, **kwargs):
        super().__init__(client_id, api_key, **kwargs)
        self.client_id = str(client_id).strip()
        self.api_key = str(api_key).strip()
        self.device_number = device_number
        self.session_id = None
        self.headers = None
        self._last_auth_error = None
        self.authenticate()

    def authenticate(self):
        # Step 1: Get Encryption Key
        url = self.BASE_URL + "customer/getAPIEncpkey"
        payload = {"userId": self.client_id}
        headers = {'Content-Type': 'application/json'}
        resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=10)
        try:
            data = resp.json()
        except Exception:
            raise Exception(f"AliceBlue getAPIEncpkey HTTP error: {resp.text}")
        if data.get("stat") != "Ok" or not data.get("encKey"):
            msg = data.get("emsg") or data.get("stat") or data
            self._last_auth_error = f"getAPIEncpkey failed: {msg}"
            raise Exception(f"AliceBlue getAPIEncpkey failed: {msg}")
        enc_key = data["encKey"]

        # Step 2: Generate SHA256(userId + apiKey + encKey)
        to_hash = f"{self.client_id}{self.api_key}{enc_key}"
        user_data = hashlib.sha256(to_hash.encode()).hexdigest()

        client_id = str(self.client_id).strip()
        api_key = str(self.api_key).strip()
        enc_key = str(enc_key).strip()  # from previous step

        logger.debug("client_id: %r", client_id)
        logger.debug("api_key: %r", api_key)
        logger.debug("enc_key: %r", enc_key)

        to_hash = f"{client_id}{api_key}{enc_key}"
        logger.debug("concat string: %r", to_hash)

        user_data = hashlib.sha256(to_hash.encode()).hexdigest()
        logger.debug("sha256: %s", user_data)

        payload = {
            "userId": client_id,
            "userData": user_data,
        }
        logger.debug("payload: %s", json.dumps(payload))

        url = "https://ant.aliceblueonline.com/rest/AliceBlueAPIService/api/customer/getUserSID"
        headers = {"Content-Type": "application/json"}
        resp = requests.post(url, headers=headers, data=json.dumps(payload))
        logger.debug("getUserSID raw response: %s", resp.text)

        # Step 3: Get Session ID
        url = self.BASE_URL + "customer/getUserSID"
        payload = {
            "userId": self.client_id,
            "userData": user_data
        }
        headers = {'Content-Type': 'application/json'}
        resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=10)
        try:
            data = resp.json()
        except Exception:
            raise Exception(f"AliceBlue getUserSID HTTP error: {resp.text}")
        if data.get("stat") != "Ok" or not data.get("sessionID"):
            msg = data.get("emsg") or data.get("stat") or data
            self._last_auth_error = f"getUserSID failed: {msg}"
            raise Exception(f"AliceBlue getUserSID failed: {msg}")
        self.session_id = data["sessionID"]
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.client_id} {self.session_id}"
        }
        self._last_auth_error = None

    def ensure_session(self):
        if not self.session_id or not self.headers:
            self.authenticate()

    def place_order(
        self,
        tradingsymbol=None,
        exchange="NSE",
        transaction_type="BUY",
        quantity=1,
        order_type="MKT",
        product="MIS",
        price=0,
        symbol_id="",
        deviceNumber=None,
        orderTag="order1",
        complexty="regular",
        disclosed_qty=0,
        retention="DAY",
        trigger_price=""
    ):
        """Place an order on Alice Blue using the official API contract."""
        mapping = get_symbol_for_broker(tradingsymbol or "", self.BROKER)
        if not symbol_id:
            symbol_id = mapping.get("symbol_id")
        tradingsymbol = mapping.get("trading_symbol", tradingsymbol)
        exchange = exchange or mapping.get("exch", exchange)
        if not symbol_id:
            raise ValueError("symbol_id is required")
        # Map order_type to prctyp as per Alice Blue docs
        ORDER_TYPE_MAP = {
            "MARKET": "MKT",
            "MKT": "MKT",
            "LIMIT": "L",
            "L": "L",
            "SL": "SL",
            "SL-M": "SL-M"
        }
        prctyp = ORDER_TYPE_MAP.get(order_type.upper(), "MKT")

        if not deviceNumber and hasattr(self, "device_number"):
            deviceNumber = self.device_number
        elif not deviceNumber:
            deviceNumber = "device123"

        url = self.BASE_URL + "placeOrder/executePlaceOrder"
        payload = [{
            "complexty": complexty.upper(),
            "discqty": str(disclosed_qty),
            "exch": exchange.upper(),
            "pCode": product.upper(),
            "prctyp": prctyp,
            "price": str(price) if price else "0",
            "qty": int(quantity),
            "ret": retention.upper(),
            "symbol_id": str(symbol_id),
            "trading_symbol": tradingsymbol,
            "transtype": transaction_type.upper(),
            "trigPrice": str(trigger_price) if trigger_price else "",
            "orderTag": orderTag,
            "deviceNumber": deviceNumber
        }]
        headers = {
            'Authorization': f'Bearer {self.client_id} {self.session_id}',
            'Content-Type': 'application/json'
        }
        try:
            r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=10)
            try:
                resp = r.json()
            except Exception:
                return {"status": "failure", "error": r.text}
            if isinstance(resp, list):
                resp = resp[0] if resp else {}
            if resp.get("stat", "").lower() == "ok" and "nestOrderNumber" in resp:
                return {"status": "success", "order_id": resp["nestOrderNumber"], **resp}
            else:
                # Try to extract the most meaningful error message available
                error_msg = (
                    resp.get("emsg")
                    or resp.get("remarks")
                    or resp.get("stat")
                    or resp.get("message")
                    or (str(resp) if resp else "Unknown error: Empty response")
                )
                return {"status": "failure", "error": error_msg, "raw": resp}
        except Exception as e:
            return {"status": "failure", "error": str(e)}

    def get_order_list(self):
        self.ensure_session()
        url = self.BASE_URL + "placeOrder/fetchOrderBook"
        r = requests.get(url, headers=self.headers, timeout=10)
        try:
            resp = r.json()
        except Exception:
            return {"status": "failure", "error": r.text}
        if isinstance(resp, list):
            orders = resp
        elif isinstance(resp, dict):
            if resp.get("stat") and resp.get("stat") != "Ok":
                return {"status": "failure", "error": resp.get("emsg", "Failed to retrieve order book."), "raw": resp}
            orders = (
                resp.get("data")
                or resp.get("OrderBookDetail")
                or resp.get("orderBook")
                or resp.get("OrderBook")
                or resp.get("orders")
                or []
            )
        else:
            orders = []
        return {"status": "success", "orders": orders}

    def get_trade_book(self):
        """Fetch the trade book which includes filled quantity information."""
        self.ensure_session()
        url = self.BASE_URL + "placeOrder/fetchTradeBook"
        r = requests.get(url, headers=self.headers, timeout=10)
        try:
            resp = r.json()
        except Exception:
            return {"status": "failure", "error": r.text}
        if isinstance(resp, list):
            trades = resp
        elif isinstance(resp, dict):
            if resp.get("stat") and resp.get("stat") != "Ok":
                return {"status": "failure", "error": resp.get("emsg", "Failed to retrieve trade book."), "raw": resp}
            trades = (
                resp.get("data")
                or resp.get("tradeBook")
                or resp.get("TradeBook")
                or resp.get("trades")
                or []
            )
        else:
            trades = []
        return {"status": "success", "trades": trades}

    def cancel_order(self, order_id):
        self.ensure_session()
        url = self.BASE_URL + "cancelOrder"
        payload = {"NOrdNo": order_id}
        r = requests.post(url, json=payload, headers=self.headers, timeout=10)
        try:
            resp = r.json()
        except Exception:
            return {"status": "failure", "error": r.text}
        if resp.get("stat") == "Ok":
            return {"status": "success", **resp}
        return {"status": "failure", **resp}

    def get_positions(self):
        self.ensure_session()
        url = self.BASE_URL + "positionAndHoldings/positionBook"
        r = requests.get(url, headers=self.headers, timeout=10)
        try:
            resp = r.json()
        except Exception:
            return {"status": "failure", "error": r.text}
        if isinstance(resp, list):
            positions = resp
        elif isinstance(resp, dict):
            if resp.get("stat") and resp.get("stat") != "Ok":
                return {
                    "status": "failure",
                    "error": resp.get("emsg", "Failed to retrieve positions."),
                    "raw": resp,
                }
            positions = (
                resp.get("data")
                or resp.get("positions")
                or resp.get("positionBook")
                or resp.get("PositionBook")
                or []
            )
        else:
            positions = []
        return {"status": "success", "positions": positions}

def get_opening_balance(self):
    self.ensure_session()
    url = self.BASE_URL + "limits/getRmsLimits"

    def _find_balance(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if str(k).lower() in [
                    "balance",
                    "cash",
                    "netbalance",
                    "openingbalance",
                    "availablebalance",
                    "available_cash",
                    "availablecash",
                    "availabelbalance",  # Note: Possible typo, should it be "availablebalance"?
                    "withdrawablebalance",
                    "equityamount",
                    "netcash",
                ]:
                    try:
                        return float(str(v).replace(",", ""))
                    except (TypeError, ValueError):
                        pass
                val = _find_balance(v)
                if val is not None:
                    return val
        elif isinstance(obj, list):
            for item in obj:
                val = _find_balance(item)
                if val is not None:
                    return val
        return None

    try:
        # This block was incorrectly indented
        r = requests.get(url, headers=self.headers, timeout=10)
        data = r.json()
        return _find_balance(data)
    except Exception:
        return None

    def check_token_valid(self):
        try:
            self.ensure_session()
            url = self.BASE_URL + "customer/accountDetails"
            # According to Alice Blue docs, GET is supported, but POST is safer if you ever add a body.
            # We'll use GET since the docs specify it and it works for most users.
            r = requests.get(url, headers=self.headers, timeout=10)
            content_type = r.headers.get("Content-Type", "").lower()
            data = None
            if "json" in content_type:
                try:
                    data = r.json()
                except Exception:
                    snippet = r.text[:100]
                    self._last_auth_error = f"HTTP {r.status_code}: {snippet}"
                    return False
            else:
                snippet = r.text[:100]
                self._last_auth_error = f"HTTP {r.status_code}: {snippet}"
                return False

            # Success: stat == "Ok" and accountStatus == "Activated"
            if (
                r.status_code == 200
                and data.get("accountStatus", "").lower() == "activated"
            ):
                return True

            # Error: stat == "Not_Ok" or emsg present
            self._last_auth_error = (
                data.get("emsg")
                or data.get("stat")
                or data.get("accountStatus")
                or str(data)
            )
            return False

        except Exception as e:
            self._last_auth_error = str(e)
            return False

    def last_auth_error(self):
        return self._last_auth_error
