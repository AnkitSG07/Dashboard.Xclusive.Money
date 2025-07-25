import requests
from .base import BrokerBase
from .symbol_map import get_symbol_for_broker
import json

class DhanBroker(BrokerBase):
    BROKER = "dhan"
    NSE = "NSE_EQ"
    INTRA = "INTRADAY"
    BUY = "BUY"
    SELL = "SELL"
    MARKET = "MARKET"
    LIMIT = "LIMIT"

    def __init__(self, client_id, access_token, **kwargs):
        timeout = kwargs.pop("timeout", 10)
        super(DhanBroker, self).__init__(client_id, access_token, timeout=timeout, **kwargs)
        self.api_base = "https://api.dhan.co/v2"
        requests.packages.urllib3.util.connection.HAS_IPV6 = False
        self.headers = {
            "access-token": access_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        self.symbol_map = kwargs.pop("symbol_map", {})

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
                raise ValueError("DhanBroker: 'security_id' required (tradingsymbol={})".format(tradingsymbol))

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

        if order_type == self.LIMIT and not price:
            raise ValueError("Limit orders require a 'price'.")
        if order_type == self.MARKET:
            payload.pop("price", None)
            payload.pop("triggerPrice", None)
        try:
            r = self._request(
                "post",
                "{}/orders".format(self.api_base),
                json=payload,
                headers=self.headers,
                timeout=self.timeout,
            )
            resp = r.json()
        except requests.exceptions.Timeout:
            return {"status": "failure", "error": "Request to Dhan API timed out while placing order."}
        except requests.exceptions.RequestException as e:
            return {"status": "failure", "error": "Failed to place order with Dhan API: {}".format(str(e))}
        except json.JSONDecodeError:
            return {"status": "failure", "error": "Invalid JSON response from Dhan API: {}".format(r.text)}
        except Exception as e:
            return {"status": "failure", "error": "An unexpected error occurred while placing order: {}".format(str(e))}

        if "orderId" in resp:
            result = {"status": "success"}
            if isinstance(resp, dict):
                result.update(resp)
            return result
        result = {"status": "failure"}
        if isinstance(resp, dict):
            result.update(resp)
        return result

    def get_order_list(self, use_pagination=True, batch_size=100, max_batches=50):
        if not use_pagination:
            try:
                r = self._request(
                    "get",
                    "{}/orders".format(self.api_base),
                    headers=self.headers,
                    timeout=self.timeout,
                )
                return {"status": "success", "data": r.json()}
            except requests.exceptions.Timeout:
                return {"status": "failure", "error": "Request to Dhan API timed out while fetching order list."}
            except requests.exceptions.RequestException as e:
                return {"status": "failure", "error": "Failed to fetch order list from Dhan API: {}".format(str(e))}
            except json.JSONDecodeError:
                return {"status": "failure", "error": "Invalid JSON response from Dhan API: {}".format(r.text)}
            except Exception as e:
                return {"status": "failure", "error": "An unexpected error occurred while fetching order list: {}".format(str(e))}

        all_orders = []
        offset = 0
        for i in range(max_batches):
            try:
                url = "{}/orders?offset={}&limit={}".format(self.api_base, offset, batch_size)
                r = self._request("get", url, headers=self.headers, timeout=self.timeout)
                batch = r.json()
                batch_orders = batch.get("data", batch) if isinstance(batch, dict) else batch
                if not batch_orders:
                    break
                all_orders.extend(batch_orders)
                if len(batch_orders) < batch_size:
                    break
                offset += batch_size
            except requests.exceptions.Timeout:
                return {
                    "status": "partial_failure" if all_orders else "failure",
                    "error": "Request to Dhan API timed out during pagination.",
                    "data": all_orders,
                }
            except requests.exceptions.RequestException as e:
                return {
                    "status": "partial_failure" if all_orders else "failure",
                    "error": "Failed to fetch order list from Dhan API during pagination: {}".format(str(e)),
                    "data": all_orders,
                }
            except json.JSONDecodeError:
                return {
                    "status": "partial_failure" if all_orders else "failure",
                    "error": "Invalid JSON response from Dhan API during pagination: {}".format(r.text),
                    "data": all_orders,
                }
            except Exception as e:
                return {
                    "status": "partial_failure" if all_orders else "failure",
                    "error": "An unexpected error occurred during pagination: {}".format(str(e)),
                    "data": all_orders,
                }
        return {"status": "success", "data": all_orders}

    def cancel_order(self, order_id):
        try:
            r = self._request(
                "delete",
                "{}/orders/{}".format(self.api_base, order_id),
                headers=self.headers,
                timeout=self.timeout,
            )
            return {"status": "success", "data": r.json()}
        except requests.exceptions.Timeout:
            return {"status": "failure", "error": "Request to Dhan API timed out while canceling order."}
        except requests.exceptions.RequestException as e:
            return {"status": "failure", "error": "Failed to cancel order with Dhan API: {}".format(str(e))}
        except json.JSONDecodeError:
            return {"status": "failure", "error": "Invalid JSON response from Dhan API: {}".format(r.text)}
        except Exception as e:
            return {"status": "failure", "error": "An unexpected error occurred while canceling order: {}".format(str(e))}

    def get_positions(self):
        try:
            r = self._request(
                "get",
                "{}/positions".format(self.api_base),
                headers=self.headers,
                timeout=self.timeout,
            )
            return {"status": "success", "data": r.json()}
        except requests.exceptions.Timeout:
            return {"status": "failure", "error": "Request to Dhan API timed out while fetching positions."}
        except requests.exceptions.RequestException as e:
            return {"status": "failure", "error": "Failed to fetch positions from Dhan API: {}".format(str(e))}
        except json.JSONDecodeError:
            return {"status": "failure", "error": "Invalid JSON response from Dhan API: {}".format(r.text)}
        except Exception as e:
            return {"status": "failure", "error": "An unexpected error occurred while fetching positions: {}".format(str(e))}

    def get_profile(self):
        try:
            r = self._request(
                "get",
                "{}/fundlimit".format(self.api_base),
                headers=self.headers,
                timeout=self.timeout,
            )
            return {"status": "success", "data": r.json()}
        except requests.exceptions.Timeout:
            return {"status": "failure", "error": "Request to Dhan API timed out while fetching profile."}
        except requests.exceptions.RequestException as e:
            return {"status": "failure", "error": "Failed to fetch profile from Dhan API: {}".format(str(e))}
        except json.JSONDecodeError:
            return {"status": "failure", "error": "Invalid JSON response from Dhan API: {}".format(r.text)}
        except Exception as e:
            return {"status": "failure", "error": "An unexpected error occurred while fetching profile: {}".format(str(e))}

    def check_token_valid(self):
        try:
            r = self._request(
                "get",
                "{}/fundlimit".format(self.api_base),
                headers=self.headers,
                timeout=5,
            )
            r.raise_for_status()
            data = r.json()
            cid = str(data.get("clientId") or data.get("dhanClientId") or "").strip()
            if cid and cid != str(self.client_id):
                return False
            return True
        except (requests.exceptions.Timeout, requests.exceptions.RequestException, json.JSONDecodeError, Exception):
            return False

    def get_opening_balance(self):
        try:
            r = self._request(
                "get",
                "{}/fundlimit".format(self.api_base),
                headers=self.headers,
                timeout=self.timeout,
            )
            data = r.json()
            for key in [
                "openingBalance",
                "netCashAvailable",
                "availableBalance",
                "withdrawableBalance",
                "availableAmount",
                "netCash",
            ]:
                if key in data:
                    return float(data[key])
            return float(data.get("cash", 0))
        except (requests.exceptions.Timeout, requests.exceptions.RequestException, json.JSONDecodeError, Exception):
            return None
