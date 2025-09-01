# Trade Copier Service

The trade copier reads master order events from the `trade_events` Redis
stream and dispatches matching orders to all active child accounts.

## Symbol Data

The copier requires a complete symbol mapping for brokers such as AliceBlue
and Zerodha.  On startup the service attempts to download the latest
instrument dumps and build this mapping.  If the dumps cannot be fetched and
no cached copy exists the service exits with an error.

Before deploying, ensure the host has network access to the Zerodha
instrument API or pre-populate the cache by running:

```bash
python -c "from brokers import symbol_map; symbol_map.build_symbol_map()"
```

This will download the necessary data and store it in the cache directory so
subsequent service starts do not require network access.

## Configuration

- `TRADE_COPIER_MAX_WORKERS`: maximum number of threads used when
  submitting orders to child brokers. Defaults to `10` if unset.
- `TRADE_COPIER_TIMEOUT`: maximum number of seconds to wait for each child
  broker API call. Defaults to `5` seconds.

Tune these settings based on your host's capacity and the expected
latency of broker APIs.
