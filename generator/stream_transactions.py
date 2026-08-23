"""
Continuous E-Commerce Transaction Streaming Pipeline.

Orchestrates the generation of realistic e-commerce transactions
and streams them continuously to Apache Kafka using KafkaProducerService.
"""

import argparse
import os
import sys
import time
from typing import Any, Dict, Optional

# Ensure repository root is on sys.path when run directly
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from generator.kafka_producer import (
    KafkaProducerError,
    KafkaProducerService,
)
from generator.transaction_generator import generate_transaction


def stream_transactions(
    rate: float = 1000.0,
    start_order_id: int = 10001,
    bootstrap_servers: Optional[str] = None,
    topic: Optional[str] = None,
    producer_service: Optional[KafkaProducerService] = None,
    max_transactions: Optional[int] = None,
    progress_interval: int = 1000,
) -> Dict[str, Any]:
    """
    Orchestrates continuous streaming of generated transactions to Kafka.

    Args:
        rate: Target transactions per second rate (> 0).
        start_order_id: Starting integer for unique sequential order IDs.
        bootstrap_servers: Optional Kafka broker address override.
        topic: Optional Kafka topic override.
        producer_service: Optional pre-configured KafkaProducerService instance (for mocking/testing).
        max_transactions: Optional limit on total transactions to send (None for infinite).
        progress_interval: Progress print interval in transaction count (0 to disable).

    Returns:
        Dict containing total count, total elapsed time, and average rate.

    Raises:
        ValueError: If rate <= 0 or start_order_id < 0 or max_transactions <= 0.
        KafkaProducerError: On Kafka configuration or publishing failures.
    """
    if rate <= 0:
        raise ValueError("Rate must be greater than 0.")
    if start_order_id < 0:
        raise ValueError("Starting order ID must be non-negative.")
    if max_transactions is not None and max_transactions <= 0:
        raise ValueError("Max transactions must be greater than 0.")

    created_service = False
    if producer_service is None:
        producer_service = KafkaProducerService(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
        )
        created_service = True

    order_id = start_order_id
    generated_count = 0
    start_time = time.perf_counter()

    try:
        while True:
            if max_transactions is not None and generated_count >= max_transactions:
                break

            tx = generate_transaction(order_id)
            producer_service.send_transaction(tx)

            generated_count += 1
            order_id += 1

            if progress_interval > 0 and generated_count % progress_interval == 0:
                print(f"Generated and published: {generated_count:,}")

            if rate > 0:
                expected_elapsed = generated_count / rate
                actual_elapsed = time.perf_counter() - start_time
                sleep_time = expected_elapsed - actual_elapsed
                if sleep_time > 0.001:
                    time.sleep(sleep_time)
                elif sleep_time > 0:
                    while time.perf_counter() - start_time < expected_elapsed:
                        pass
    except KeyboardInterrupt:
        pass
    finally:
        if created_service or producer_service is not None:
            try:
                producer_service.flush()
            except Exception:
                pass
            try:
                producer_service.close()
            except Exception:
                pass

    elapsed_time = time.perf_counter() - start_time
    achieved_rate = generated_count / elapsed_time if elapsed_time > 0 else 0.0

    return {
        "count": generated_count,
        "elapsed_time": elapsed_time,
        "achieved_rate": achieved_rate,
    }


def main() -> None:
    """CLI entry point for transaction streaming."""
    parser = argparse.ArgumentParser(
        description="Continuous E-Commerce Transaction Streaming to Kafka"
    )
    parser.add_argument(
        "-r",
        "--rate",
        type=float,
        default=1000.0,
        help="Target transactions per second rate (default: 1000)",
    )
    parser.add_argument(
        "-s",
        "--start-order-id",
        type=int,
        default=10001,
        help="Starting Order ID (default: 10001)",
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=None,
        help="Kafka bootstrap servers (overrides KAFKA_BOOTSTRAP_SERVERS env var)",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=None,
        help="Kafka topic (overrides KAFKA_TOPIC env var)",
    )
    parser.add_argument(
        "-m",
        "--max-transactions",
        type=int,
        default=None,
        help="Optional maximum number of transactions to stream before stopping",
    )

    args = parser.parse_args()

    if args.rate <= 0:
        parser.error("Rate must be greater than 0.")
    if args.start_order_id < 0:
        parser.error("Start order ID must be non-negative.")
    if args.max_transactions is not None and args.max_transactions <= 0:
        parser.error("Max transactions must be greater than 0.")

    try:
        summary = stream_transactions(
            rate=args.rate,
            start_order_id=args.start_order_id,
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            max_transactions=args.max_transactions,
        )
        print("\nStreaming stopped.")
        print(f"Transactions generated: {summary['count']:,}")
        print(f"Elapsed time: {summary['elapsed_time']:.2f} seconds")
        print(f"Average rate: {summary['achieved_rate']:.0f} tx/sec")
    except KafkaProducerError as exc:
        print(f"\nKafka Error: {exc}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nStreaming stopped.")
        sys.exit(0)
    except Exception as exc:
        print(f"\nUnexpected Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
