import argparse
import json
import random
import time
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Optional


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
    Generates a finite stream of `count` transactions.
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


def generate_transaction_batches(
    count: int,
    batch_size: int = 1000,
    rate: Optional[float] = None,
    start_order_id: int = 10001
) -> Generator[List[Dict[str, Any]], None, None]:
    """
    Generates transactions in configurable batches without storing all transactions in memory.

    Args:
        count: Total number of transactions to generate.
        batch_size: Number of transactions per batch.
        rate: Target rate in transactions per second.
        start_order_id: Starting Order ID.

    Yields:
        Lists of transaction dictionaries (batches).
    """
    if count <= 0:
        raise ValueError("Count must be greater than 0.")
    if batch_size <= 0:
        raise ValueError("Batch size must be greater than 0.")
    if rate is not None and rate <= 0:
        raise ValueError("Rate must be greater than 0.")

    tx_generator = generate_transactions(count=count, rate=rate, start_order_id=start_order_id)
    batch: List[Dict[str, Any]] = []

    for tx in tx_generator:
        batch.append(tx)
        if len(batch) >= batch_size:
            yield batch
            batch = []

    if batch:
        yield batch


def generate_continuous_transactions(
    rate: Optional[float] = None,
    start_order_id: int = 10001
) -> Generator[Dict[str, Any], None, None]:
    """
    Generates an infinite stream of transactions continuously.
    Paces generation to match target `rate` if specified.

    Args:
        rate: Target transactions per second rate (None for unlimited).
        start_order_id: Starting integer for unique sequential order IDs.

    Yields:
        Transaction dictionaries sequentially.
    """
    if rate is not None and rate <= 0:
        raise ValueError("Rate must be greater than 0.")

    order_id = start_order_id
    start_time = time.perf_counter()
    i = 0

    while True:
        tx = generate_transaction(order_id)
        yield tx

        i += 1
        order_id += 1

        if rate and rate > 0:
            expected_elapsed = i / rate
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
        "-m", "--mode",
        type=str,
        choices=["finite", "batch", "continuous"],
        default="finite",
        help="Generation mode: finite, batch, or continuous (default: finite)"
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
        "-b", "--batch-size",
        type=int,
        default=1000,
        help="Batch size for batch generation mode (default: 1000)"
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

    # Input validation
    if args.count <= 0:
        parser.error("Count must be greater than 0.")
    if args.rate is not None and args.rate <= 0:
        parser.error("Rate must be greater than 0.")
    if args.batch_size <= 0:
        parser.error("Batch size must be greater than 0.")

    rate_desc = f"{args.rate:.1f} tx/sec" if args.rate else "unlimited"

    # -------------------------------------------------------------
    # Continuous Mode
    # -------------------------------------------------------------
    if args.mode == "continuous":
        print(f"Continuous mode started at {rate_desc}.")
        print("Press Ctrl+C to stop.\n")

        generated_count = 0
        start_time = time.perf_counter()
        progress_interval = 5000

        try:
            for tx in generate_continuous_transactions(
                rate=args.rate,
                start_order_id=args.start_order_id
            ):
                generated_count += 1
                if generated_count % progress_interval == 0:
                    print(f"Generated: {generated_count:,} transactions")
        except KeyboardInterrupt:
            print("\nContinuous mode stopped by user.")

        total_time = time.perf_counter() - start_time
        achieved_tps = generated_count / total_time if total_time > 0 else 0.0

        print(f"\nGenerated: {generated_count:,}")
        print(f"Elapsed: {total_time:.2f} seconds")
        print(f"Achieved rate: {achieved_tps:.0f} tx/sec")
        return

    # -------------------------------------------------------------
    # Batch Mode
    # -------------------------------------------------------------
    if args.mode == "batch":
        print(f"Generating {args.count:,} transactions in batches of {args.batch_size:,} (Target rate: {rate_desc})...\n")
        start_time = time.perf_counter()

        batch_gen = generate_transaction_batches(
            count=args.count,
            batch_size=args.batch_size,
            rate=args.rate,
            start_order_id=args.start_order_id
        )

        total_generated = 0
        batch_count = 0

        for batch in batch_gen:
            batch_count += 1
            total_generated += len(batch)
            if args.verbose or args.count <= 10:
                print(f"Batch {batch_count} ({len(batch)} txs):")
                print(json.dumps(batch, indent=2))

        total_time = time.perf_counter() - start_time
        achieved_tps = total_generated / total_time if total_time > 0 else 0.0

        if not (args.verbose or args.count <= 10):
            print(f"Processed {batch_count:,} batch(es) containing {total_generated:,} total transactions.")

        print(f"\nGenerated: {total_generated:,}")
        print(f"Elapsed: {total_time:.2f} seconds")
        print(f"Achieved rate: {achieved_tps:.0f} tx/sec")
        return

    # -------------------------------------------------------------
    # Finite Mode (Default)
    # -------------------------------------------------------------
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

    print(f"Generating {args.count:,} transactions (Target rate: {rate_desc})...\n")

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
    achieved_tps = generated_count / total_time if total_time > 0 else 0.0

    print(f"Generated: {generated_count:,}")
    print(f"Elapsed: {total_time:.2f} seconds")
    print(f"Achieved rate: {achieved_tps:.0f} tx/sec")


if __name__ == "__main__":
    main()