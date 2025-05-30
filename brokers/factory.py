# brokers/factory.py

from .dhan import DhanBroker
from .zerodha import ZerodhaBroker
from .aliceblue import AliceBlueBroker
from .fyers import FyersBroker
# add other imports as needed

def get_broker_class(broker_name):
    name = broker_name.lower()
    if name == "dhan":
        return DhanBroker
    elif name == "zerodha":
        return ZerodhaBroker
    elif name == "aliceblue":
        return AliceBlueBroker
    elif name == "fyers":
        return FyersBroker
    # add more as you add brokers
    else:
        raise Exception(f"Unknown broker: {broker_name}")
