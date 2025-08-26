# Master Trade Monitor

`services.master_trade_monitor` polls each master broker account for orders that
were placed manually (outside the webhook pipeline) and publishes them to the
`trade_events` Redis stream. The `trade_copier` service then copies these
orders to linked child accounts.

## Running the service

```bash
python -m services.master_trade_monitor
```

The monitor queries all accounts with role `master` from the database and sends
any newly executed orders to Redis. Deploy it as a standalone worker and run
multiple instances if required.

Set the `ORDER_MONITOR_INTERVAL` environment variable or pass the
`poll_interval` argument to `monitor_master_trades` to control the polling
interval in seconds (default: 5).

## Scaling

The service is stateless and can be scaled horizontally. Each instance polls all
masters and publishes any trades found. Because events are written to the
`trade_events` stream, the existing `trade_copier` workers will replicate the
orders to child accounts immediately.

## Avoiding polling

Some brokers expose webhooks or streaming APIs that emit trade events as soon
as they execute. Integrating such push notifications can eliminate the polling
delay entirely. If your broker offers this, publish those events directly to
the `trade_events` stream instead of relying on the polling monitor.
