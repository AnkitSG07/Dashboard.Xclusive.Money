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
        timeout = kwargs.pop("timeout", None)
        super().__init__(client_id, access_token, timeout=timeout, **kwargs)
        self.api_base = "https://api.dhan.co/v2"
        requests.packages.urllib3.util.connection.HAS_IPV6 = False
        self.headers = {
            "access-token": access_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

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
        """
        Place a new order on Dhan.
        """
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

        try:
            r = self.session.post(
                f"{self.api_base}/orders", json=payload, headers=self.headers, timeout=self.timeout
            )
            resp = r.json()
        except Exception as e:
            resp = {"status": "failure", "error": str(e)}
        if "orderId" in resp:
            return {"status": "success", **resp}
        return {"status": "failure", **resp}

    def get_order_list(self, use_pagination=True, batch_size=100, max_batches=50):
        """
        Fetch order list. For large accounts, use pagination to avoid timeouts.
        If the Dhan API doesn't support offset/limit, set use_pagination=False.
        """
        if not use_pagination:
            try:
                r = self.session.get(
                    f"{self.api_base}/orders", headers=self.headers, timeout=self.timeout
                )
                return {"status": "success", "data": r.json()}
            except Exception as e:
                return {"status": "failure", "error": str(e)}

        # Paginated fetch for large accounts
        all_orders = []
        offset = 0
        for _ in range(max_batches):
            try:
                url = f"{self.api_base}/orders?offset={offset}&limit={batch_size}"
                r = self.session.get(url, headers=self.headers, timeout=self.timeout)
                batch = r.json()
                batch_orders = batch.get("data", batch) if isinstance(batch, dict) else batch
                if not batch_orders or len(batch_orders) == 0:
                    break
                all_orders.extend(batch_orders)
                if len(batch_orders) < batch_size:
                    break
                offset += batch_size
            except Exception as e:
                # Return partial results if error occurs
                return {
                    "status": "partial_failure" if all_orders else "failure",
                    "error": str(e),
                    "data": all_orders,
                }
        return {"status": "success", "data": all_orders}

    def cancel_order(self, order_id):
        """
        Cancel an order by order_id.
        """
        try:
            r = self.session.delete(
                f"{self.api_base}/orders/{order_id}", headers=self.headers, timeout=self.timeout
            )
            return {"status": "success", "data": r.json()}
        except Exception as e:
            return {"status": "failure", "error": str(e)}

    def get_positions(self):
        """
        Fetch current positions.
        """
        try:
            r = self.session.get(
                f"{self.api_base}/positions", headers=self.headers, timeout=self.timeout
            )
            return {"status": "success", "data": r.json()}
        except Exception as e:
            return {"status": "failure", "error": str(e)}

    def get_profile(self):
        """
        Return profile or fund data to confirm account id.
        """
        try:
            r = self.session.get(
                f"{self.api_base}/fundlimit", headers=self.headers, timeout=self.timeout
            )
            return {"status": "success", "data": r.json()}
        except Exception as e:
            return {"status": "failure", "error": str(e)}

    def check_token_valid(self):
        """
        Validate access token and ensure it belongs to this client_id.
        """
        try:
            r = self.session.get(
                f"{self.api_base}/fundlimit", headers=self.headers, timeout=5
            )
            r.raise_for_status()
            data = r.json()
            cid = str(data.get("clientId") or data.get("dhanClientId") or "").strip()
            if cid and cid != str(self.client_id):
                return False
            return True
        except Exception:
            return False

    def get_opening_balance(self):
        """
        Fetch available cash balance from fundlimit API.
        """
        try:
            r = self.session.get(
                f"{self.api_base}/fundlimit", headers=self.headers, timeout=self.timeout
            )
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
