# Trade Copier Service

The trade copier reads master order events from the `trade_events` Redis
stream and dispatches matching orders to all active child accounts.

## Configuration

- `TRADE_COPIER_MAX_WORKERS`: maximum number of threads used when
  submitting orders to child brokers. Defaults to `10` if unset.
- `TRADE_COPIER_TIMEOUT`: maximum number of seconds to wait for each child
  broker API call. Defaults to `5` seconds.

Tune these settings based on your host's capacity and the expected
latency of broker APIs.
