# brokers/dhan.py
import requests
from .base import BrokerBase

SYMBOL_MAP = {
    "RELIANCE": "2885", "TCS": "11536", "INFY": "10999", "ADANIPORTS": "15083", "IDEA": "532822", "HDFCBANK": "1333",
    "SBIN": "3045", "ICICIBANK": "4963", "AXISBANK": "1343", "ITC": "1660", "HINDUNILVR": "1394",
    "KOTAKBANK": "1922", "LT": "11483", "BAJFINANCE": "317", "HCLTECH": "7229", "ASIANPAINT": "236",
    "MARUTI": "1095", "M&M": "2031", "SUNPHARMA": "3046", "TATAMOTORS": "3432", "WIPRO": "3787",
    "ULTRACEMCO": "11532", "TITAN": "3506", "NESTLEIND": "11262", "BAJAJFINSV": "317",
    "POWERGRID": "14977", "NTPC": "2886", "JSWSTEEL": "11723", "HDFCLIFE": "11915",
    "DRREDDY": "881", "TECHM": "11534", "BRITANNIA": "293", "TATASTEEL": "3505", "CIPLA": "694",
    "SBILIFE": "11916", "BAJAJ-AUTO": "317", "HINDALCO": "1393", "DIVISLAB": "881",
    "GRASIM": "1147", "ADANIENT": "15083", "COALINDIA": "694", "INDUSINDBK": "1393",
    "TATACONSUM": "3505", "EICHERMOT": "881", "SHREECEM": "1147", "HEROMOTOCO": "15083",
    "BAJAJHLDNG": "694", "SBICARD": "1393", "DLF": "3505", "DMART": "881", "UPL": "1147",
    "ICICIPRULI": "15083", "HDFCAMC": "694", "HDFC": "1393", "GAIL": "3505", "HAL": "881",
    "TATAPOWER": "1147", "VEDL": "15083", "BPCL": "694", "IOC": "1393", "ONGC": "3505",
    "LICHSGFIN": "881", "BANKBARODA": "1147", "PNB": "15083", "CANBK": "694", "UNIONBANK": "1393",
    "IDFCFIRSTB": "3505", "BANDHANBNK": "881", "FEDERALBNK": "1147", "RBLBANK": "15083",
    "YESBANK": "694", "IGL": "1393", "PETRONET": "3505", "GUJGASLTD": "881", "MGL": "1147",
    "TORNTPHARM": "15083", "LUPIN": "694", "AUROPHARMA": "1393", "BIOCON": "3505",
    "GLENMARK": "881", "CADILAHC": "1147", "ALKEM": "15083", "APOLLOHOSP": "694",
    "MAXHEALTH": "1393", "FORTIS": "3505", "JUBLFOOD": "881", "UBL": "1147", "MCDOWELL-N": "15083",
    "COLPAL": "694", "DABUR": "1393", "GODREJCP": "3505", "MARICO": "881", "EMAMILTD": "1147",
    "PGHH": "15083", "GILLETTE": "694", "TATACHEM": "1393", "PIDILITIND": "3505",
    "BERGEPAINT": "881", "KANSAINER": "1147", "JSWENERGY": "15083", "ADANIGREEN": "694",
    "ADANITRANS": "1393", "NHPC": "3505", "SJVN": "881", "RECLTD": "1147", "PFC": "15083"
}


class DhanBroker(BrokerBase):
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
        if not security_id:
            if tradingsymbol and self.symbol_map:
                security_id = self.symbol_map.get(tradingsymbol.upper())
            if not security_id:
                raise Exception(f"DhanBroker: 'security_id' required (tradingsymbol={tradingsymbol})")

        if not exchange_segment:
            exchange_segment = self.NSE

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
        # Not implemented yet
        return {"status": "failure", "error": "Not Implemented"}
