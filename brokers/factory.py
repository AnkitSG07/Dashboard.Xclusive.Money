# brokers/factory.py

from .dhan import DhanBroker
from .zerodha import ZerodhaBroker
# import other brokers...

def get_broker_class(broker_name):
    if broker_name.lower() == "dhan":
        return DhanBroker
    if broker_name.lower() == "zerodha":
        return ZerodhaBroker
    # add more...
    raise Exception("Unknown broker")

