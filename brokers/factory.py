# brokers/factory.py

import importlib

def get_broker_class(broker_name):
    """
    Dynamically import and return the appropriate broker adapter class.
    Example: get_broker_class('zerodha') -> ZerodhaBroker class
    """
    name = broker_name.lower().replace(" ", "").replace("_", "")
    mapping = {
        "zerodha": "ZerodhaBroker",
        "aliceblue": "AliceBlueBroker",
        "fyers": "FyersBroker",
        "finvasia": "FinvasiaBroker",
        "dhan": "DhanAPI",
        "flattrade": "FlattradeBroker",
        "acagarwal": "ACAgarwalBroker",
        "motilaloswal": "MotilalOswalBroker",
        "kotakneo": "KotakNeoBroker",
        "tradejini": "TradejiniBroker",
        "zebu": "ZebuBroker",
        "enrichmoney": "EnrichMoneyBroker",
        "broker1": "Broker1Broker"
    }
    if name not in mapping:
        raise ValueError(f"Unknown broker: {broker_name}")
    try:
        module = importlib.import_module(f".{name}", __package__ or "brokers")
        return getattr(module, mapping[name])
    except Exception as e:
        raise ImportError(f"Failed to load broker '{broker_name}': {e}")
