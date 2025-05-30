# brokers/factory.py

from .base import BrokerBase

from .zerodha import ZerodhaBroker
from .aliceblue import AliceBlueBroker
from .fyers import FyersBroker
from .finvasia import FinvasiaBroker
from .dhan import DhanBroker
from .flattrade import FlattradeBroker
from .acagarwal import ACAgarwalBroker
from .motilaloswal import MotilalOswalBroker
from .kotakneo import KotakNeoBroker
from .tradejini import TradejiniBroker
from .zebu import ZebuBroker
from .enrichmoney import EnrichMoneyBroker
from .broker1 import Broker1

BROKER_CLASS_MAP = {
    "zerodha": ZerodhaBroker,
    "aliceblue": AliceBlueBroker,
    "fyers": FyersBroker,
    "finvasia": FinvasiaBroker,
    "dhan": DhanBroker,
    "flattrade": FlattradeBroker,
    "acagarwal": ACAgarwalBroker,
    "motilaloswal": MotilalOswalBroker,
    "kotakneo": KotakNeoBroker,
    "tradejini": TradejiniBroker,
    "zebu": ZebuBroker,
    "enrichmoney": EnrichMoneyBroker,
    "broker1": Broker1,  # Custom/unlisted broker logic
}

def get_broker_instance(broker_name, client_id, access_token, **kwargs):
    """
    Factory to instantiate broker objects dynamically.
    Example usage:
        broker = get_broker_instance("zerodha", client_id, access_token)
    """
    name = str(broker_name or "").lower()
    broker_class = BROKER_CLASS_MAP.get(name)
    if not broker_class:
        raise ValueError(f"Broker '{broker_name}' is not supported in this system.")
    return broker_class(client_id, access_token, **kwargs)
