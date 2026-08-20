import argparse
import json
import random
import time
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional


PRODUCTS = [
    "P501",
    "P502",
    "P503",
    "P504",
    "P505",
    "P506",
    "P507",
    "P508",
    "P509",
    "P510"
]

PAYMENT_METHODS = [
    "Credit Card",
    "Debit Card",
    "UPI",
    "Net Banking"
]


def generate_transaction(order_id: int) -> Dict[str, Any]:
    """
    Generates a single realistic e-commerce transaction dictionary.

    Args:
        order_id: Unique integer identifier for the transaction order.

    Returns:
        Dict representing transaction details.
    """
    quantity = random.randint(1, 5)
    price = round(random.uniform(100.0, 5000.0), 2)
    tax_amount = round(price * quantity * 0.18, 2)

    return {
        "order_id": order_id,
        "customer_id": f"C{random.randint(101, 999)}",
        "product_id": random.choice(PRODUCTS),
        "quantity": quantity,
        "price": price,
        "tax_amount": tax_amount,
        "payment_method": random.choice(PAYMENT_METHODS),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def generate_transactions(
    count: int,
    rate: Optional[float] = None,
    start_order_id: int = 10001
) -> Generator[Dict[str, Any], None, None]:
    """
    Generates a stream of `count` transactions.
    Optionally paces generation to match target `rate` (transactions per second).

    Args:
        count: Total number of transactions to generate.
        rate: Target transactions per second rate (None or 0 for unlimited).
        start_order_id: Starting integer for unique sequential order IDs.

    Yields:
        Transaction dictionaries sequentially.
    """
    if count <= 0:
        raise ValueError("Count must be greater than 0.")
    if rate is not None and rate <= 0:
        raise ValueError("Rate must be greater than 0.")

    start_time = time.perf_counter()
    for i in range(count):
        order_id = start_order_id + i
        tx = generate_transaction(order_id)
        yield tx

        if rate and rate > 0:
            expected_elapsed = (i + 1) / rate
            actual_elapsed = time.perf_counter() - start_time
            sleep_time = expected_elapsed - actual_elapsed
            if sleep_time > 0.001:
                time.sleep(sleep_time)
            elif sleep_time > 0:
                while time.perf_counter() - start_time < expected_elapsed:
                    pass


def main() -> None:
    parser = argparse.ArgumentParser(
        description="High-Volume E-Commerce Transaction Generator"
    )
    parser.add_argument(
        "-c", "--count",
        type=int,
        default=1000,
        help="Number of transactions to generate (default: 1000)"
    )
    parser.add_argument(
        "-r", "--rate",
        type=float,
        default=None,
        help="Target rate in transactions per second (e.g. 1000)"
    )
    parser.add_argument(
        "-s", "--start-order-id",
        type=int,
        default=10001,
        help="Starting Order ID (default: 10001)"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Print all generated transactions as JSON"
    )

    args = parser.parse_args()

    if args.count <= 0:
        parser.error("Count must be greater than 0.")
    if args.rate is not None and args.rate <= 0:
        parser.error("Rate must be greater than 0.")

    # Small count or verbose mode: print full JSON output
    if args.count <= 10 or args.verbose:
        transactions = list(
            generate_transactions(
                count=args.count,
                rate=args.rate,
                start_order_id=args.start_order_id
            )
        )
        print(json.dumps(transactions, indent=2))
        return

    # High-volume execution: efficient output without flooding terminal
    rate_desc = f"{args.rate:.1f} tx/sec" if args.rate else "unlimited"
    print(f"Generating {args.count} transactions (Target rate: {rate_desc})...\n")

    start_time = time.perf_counter()
    tx_gen = generate_transactions(
        count=args.count,
        rate=args.rate,
        start_order_id=args.start_order_id
    )

    first_tx = next(tx_gen)
    print("Sample generated transaction:")
    print(json.dumps(first_tx, indent=2))
    print()

    generated_count = 1

    for _ in tx_gen:
        generated_count += 1

    total_time = time.perf_counter() - start_time
    achieved_tps = generated_count / total_time if total_time > 0 else float("inf")

    print(f"Successfully generated {generated_count} transactions in {total_time:.3f} seconds.")
    print(f"Achieved Rate: {achieved_tps:.2f} tx/sec")
    print(f"Order ID range: {args.start_order_id} to {args.start_order_id + generated_count - 1}")


if __name__ == "__main__":
    main()