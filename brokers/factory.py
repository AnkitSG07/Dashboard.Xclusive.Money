"""Factory helpers for broker adapters and their service clients."""

import importlib
import os

from .client import BrokerServiceClient


def _load_broker_class(broker_name):
    """Return the in-process adapter class for ``broker_name``.

    This is the original behaviour of :func:`get_broker_class`.  It is kept
    as a private helper so that broker microservices can still import and
    instantiate the concrete adapter implementation.
    """
    
    name = broker_name.lower().replace(" ", "").replace("_", "")
    mapping = {
        "zerodha": "ZerodhaBroker",
        "aliceblue": "AliceBlueBroker",
        "fyers": "FyersBroker",
        "finvasia": "FinvasiaBroker",
        "dhan": "DhanBroker",
        "flattrade": "FlattradeBroker",
        "groww": "GrowwBroker",
        "acagarwal": "ACAgarwalBroker",
        "motilaloswal": "MotilalOswalBroker",
        "kotakneo": "KotakNeoBroker",
        "tradejini": "TradejiniBroker",
        "zebu": "ZebuBroker",
        "enrichmoney": "EnrichMoneyBroker",
        "broker1": "Broker1Broker",
    }
    if name not in mapping:
        raise ValueError(f"Unknown broker: {broker_name}")
    try:
        module = importlib.import_module(f".{name}", __package__ or "brokers")
        return getattr(module, mapping[name])
    except Exception as e:  # pragma: no cover - defensive error path
        raise ImportError(f"Failed to load broker '{broker_name}': {e}")

def get_broker_client(broker_name):
    """Return a callable that instantiates a broker service client.

    If an environment variable of the form ``BROKER_<NAME>_URL`` is present
    (for example ``BROKER_ZERODHA_URL``) a lightweight HTTP client is returned
    which will forward calls to that service.  Otherwise the local in-process
    adapter class is returned for backwards compatibility.
    """

    env_var = f"BROKER_{broker_name.upper()}_URL"
    service_url = os.environ.get(env_var)
    if service_url:
        # Create a small factory that mirrors the original adapter API but
        # delegates the heavy lifting to a remote service.
        def _factory(client_id, access_token, **kwargs):
            return BrokerServiceClient(service_url, client_id, access_token, **kwargs)

        return _factory

    # Fall back to the legacy in-process adapter
    return _load_broker_class(broker_name)


# ``get_broker_class`` used to return the adapter class.  To ease the
# transition to remote broker services we keep the old name as an alias of the
# new ``get_broker_client`` function so existing imports continue to work.
get_broker_class = get_broker_client


__all__ = ["get_broker_client", "get_broker_class", "_load_broker_class"]
