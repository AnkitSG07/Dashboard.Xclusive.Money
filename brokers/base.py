# brokers/base.py

class BrokerBase:
    def __init__(self, **kwargs):
        pass

    @classmethod
    def login_url(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def get_access_token(cls, *args, **kwargs):
        raise NotImplementedError

    def get_positions(self):
        raise NotImplementedError

    def get_order_list(self):
        raise NotImplementedError

    def place_order(self, *args, **kwargs):
        raise NotImplementedError

