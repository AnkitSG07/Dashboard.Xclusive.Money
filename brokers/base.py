# brokers/base.py

class BrokerBase:
    def __init__(self, client_id, access_token, **kwargs):
        self.client_id = client_id
        self.access_token = access_token
        self.extra = kwargs  # Other credential fields

    def place_order(self, *args, **kwargs):
        raise NotImplementedError("place_order not implemented for this broker.")

    def get_order_list(self):
        raise NotImplementedError("get_order_list not implemented for this broker.")

    def get_positions(self):
        raise NotImplementedError("get_positions not implemented for this broker.")

    def cancel_order(self, order_id):
        raise NotImplementedError("cancel_order not implemented for this broker.")

    def get_profile(self):
        raise NotImplementedError("get_profile not implemented for this broker.")

    def check_token_valid(self):
        """Optional: Returns True if access token is valid, else False."""
        return False
