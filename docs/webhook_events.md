# Webhook Event Schema

Events published to the low-latency queue from the `/webhook/<user_id>`
endpoint follow the structure below. Downstream consumers should expect
the event fields listed in the table and treat any additional fields as
optional extensions.

| Field        | Type    | Description                                                  |
| ------------ | ------- | ------------------------------------------------------------ |
| `user_id`    | integer | ID of the user associated with the webhook token.           |
| `strategy_id` | integer or null | Identifier for the strategy verified via the webhook secret. May be `null` if no strategy is associated. |
| `symbol`     | string  | Trading symbol for the order (e.g. `NSE:SBIN`).             |
| `action`     | string  | Trading action, upper‑cased (e.g. `BUY`, `SELL`).           |
| `qty`        | integer | Quantity to trade.                                          |

The events are published to the Redis Stream `webhook_events` and are
validated using Marshmallow in `services/webhook_receiver.py`.
