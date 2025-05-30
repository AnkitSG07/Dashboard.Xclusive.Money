# brokers/zerodha.py

from .base import BrokerBase
from kiteconnect import KiteConnect, KiteTicker

class ZerodhaBroker(BrokerBase):
    def __init__(self, api_key, api_secret, access_token):
        self.api_key = api_key
        self.api_secret = api_secret
        self.access_token = access_token
        self.kite = KiteConnect(api_key=api_key)
        self.kite.set_access_token(access_token)

    @classmethod
    def login_url(cls, api_key, redirect_url):
        kite = KiteConnect(api_key=api_key)
        return kite.login_url(redirect_url=redirect_url)

    @classmethod
    def get_access_token(cls, api_key, api_secret, request_token):
        kite = KiteConnect(api_key=api_key)
        data = kite.generate_session(request_token, api_secret=api_secret)
        return data['access_token']

    def get_positions(self):
        return self.kite.positions()

    def get_order_list(self):
        return self.kite.orders()

    def place_order(self, symbol, quantity, transaction_type, order_type, product, price=0):
        # transaction_type: "BUY"/"SELL"
        # order_type: "MARKET"/"LIMIT"
        # product: "CNC"/"MIS"/"NRML"
        # exchange and variety can be improved to be dynamic
        return self.kite.place_order(
            variety='regular',
            exchange='NSE',
            tradingsymbol=symbol,
            transaction_type=transaction_type,
            quantity=quantity,
            order_type=order_type,
            product=product,
            price=price
        )


