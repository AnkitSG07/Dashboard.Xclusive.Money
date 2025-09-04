#!/usr/bin/env python3
"""Fetch the Alice Blue trade book and print it as JSON."""

import argparse
import json
from brokers.aliceblue import AliceBlueBroker


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Alice Blue trade book")
    parser.add_argument("--client-id", required=True, help="Alice Blue client ID")
    parser.add_argument("--api-key", required=True, help="Alice Blue API key")
    parser.add_argument(
        "--device-number",
        help="Optional device number registered with Alice Blue",
        default=None,
    )
    args = parser.parse_args()

    broker = AliceBlueBroker(
        client_id=args.client_id,
        api_key=args.api_key,
        device_number=args.device_number,
    )
    trade_book = broker.get_trade_book()
    print(json.dumps(trade_book, indent=2))


if __name__ == "__main__":
    main()
