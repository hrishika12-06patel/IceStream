import argparse
import json
import logging
import os
import sys
from typing import Any, Dict, Optional, Tuple

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class KafkaProducerError(Exception):
    """Base exception for Kafka Producer module errors."""

    pass


class KafkaConfigError(KafkaProducerError):
    """Raised when Kafka configuration (broker, topic) is missing or invalid."""

    pass


class TransactionValidationError(KafkaProducerError):
    """Raised when the input transaction is not a valid dictionary."""

    pass


class SerializationError(KafkaProducerError):
    """Raised when transaction cannot be serialized to JSON."""

    pass


class KafkaPublishError(KafkaProducerError):
    """Raised when publishing a message to Kafka fails."""

    pass


def load_kafka_config(
    bootstrap_servers: Optional[str] = None,
    topic: Optional[str] = None
) -> Tuple[str, str]:
    """
    Loads and validates Kafka configuration from arguments or environment variables.

    Args:
        bootstrap_servers: Kafka broker address(es).
        topic: Kafka topic name.

    Returns:
        Tuple of (bootstrap_servers, topic).

    Raises:
        KafkaConfigError: If bootstrap_servers or topic is missing/empty.
    """
    servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if not servers or not str(servers).strip():
        raise KafkaConfigError(
            "Missing Kafka broker configuration. Set KAFKA_BOOTSTRAP_SERVERS environment variable."
        )

    kafka_topic = topic or os.getenv("KAFKA_TOPIC")
    if not kafka_topic or not str(kafka_topic).strip():
        raise KafkaConfigError(
            "Missing Kafka topic configuration. Set KAFKA_TOPIC environment variable."
        )

    return str(servers).strip(), str(kafka_topic).strip()


def serialize_transaction(transaction: Dict[str, Any]) -> bytes:
    """
    Validates and serializes a transaction dictionary to UTF-8 encoded JSON bytes.

    Args:
        transaction: Transaction dictionary to serialize.

    Returns:
        bytes: UTF-8 encoded JSON string bytes.

    Raises:
        TransactionValidationError: If transaction is not a dict.
        SerializationError: If JSON serialization fails.
    """
    if not isinstance(transaction, dict):
        raise TransactionValidationError(
            f"Transaction must be a dictionary, got '{type(transaction).__name__}'."
        )

    try:
        json_str = json.dumps(transaction, ensure_ascii=False)
        return json_str.encode("utf-8")
    except (TypeError, ValueError, OverflowError) as exc:
        raise SerializationError(f"Failed to serialize transaction to JSON: {str(exc)}") from exc


class KafkaProducerService:
    """
    Service for sending serialized transaction messages to Apache Kafka.
    """

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        producer: Optional[Any] = None,
        **kafka_kwargs: Any
    ):
        """
        Initialize KafkaProducerService.

        Args:
            bootstrap_servers: Kafka broker(s) endpoint(s).
            topic: Kafka topic to publish to.
            producer: Optional existing producer instance (used for testing/mocking).
            **kafka_kwargs: Extra keyword args passed to underlying KafkaProducer.
        """
        self.bootstrap_servers, self.topic = load_kafka_config(bootstrap_servers, topic)
        self._producer = producer
        self._kafka_kwargs = kafka_kwargs

    @property
    def producer(self) -> Any:
        """
        Lazily initializes the underlying KafkaProducer if not provided.
        """
        if self._producer is None:
            try:
                from kafka import KafkaProducer
            except ImportError as exc:
                raise KafkaPublishError(
                    "Kafka client library is not installed. Ensure kafka-python-ng is installed."
                ) from exc

            try:
                self._producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    **self._kafka_kwargs
                )
            except Exception as exc:
                err_msg = str(exc)
                raise KafkaPublishError(
                    f"Failed to initialize Kafka producer for broker '{self.bootstrap_servers}': {err_msg}"
                ) from exc

        return self._producer

    def send_transaction(
        self,
        transaction: Dict[str, Any],
        key: Optional[str] = None
    ) -> Any:
        """
        Sends a single transaction dictionary to the configured Kafka topic.

        Args:
            transaction: Transaction data dictionary.
            key: Optional record key string.

        Returns:
            Producer future / metadata object.

        Raises:
            TransactionValidationError: If transaction is not a dict.
            SerializationError: If transaction cannot be JSON-serialized.
            KafkaPublishError: If Kafka fails to send the message.
        """
        serialized_bytes = serialize_transaction(transaction)
        key_bytes = key.encode("utf-8") if key is not None else None

        try:
            future = self.producer.send(
                self.topic,
                value=serialized_bytes,
                key=key_bytes
            )
            return future
        except KafkaProducerError:
            raise
        except Exception as exc:
            raise KafkaPublishError(
                f"Error publishing message to Kafka topic '{self.topic}': {str(exc)}"
            ) from exc

    def flush(self, timeout: Optional[float] = None) -> None:
        """Flushes pending buffered messages."""
        if self._producer:
            try:
                self._producer.flush(timeout=timeout)
            except Exception as exc:
                raise KafkaPublishError(f"Error flushing Kafka producer: {str(exc)}") from exc

    def close(self, timeout: Optional[float] = None) -> None:
        """Closes the Kafka producer connection."""
        if self._producer:
            try:
                self._producer.close(timeout=timeout)
            except Exception as exc:
                raise KafkaPublishError(f"Error closing Kafka producer: {str(exc)}") from exc

    def __enter__(self) -> "KafkaProducerService":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Kafka Producer module for sending transactions to Apache Kafka."
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        help="Kafka bootstrap servers (defaults to KAFKA_BOOTSTRAP_SERVERS env var)"
    )
    parser.add_argument(
        "--topic",
        type=str,
        help="Kafka topic (defaults to KAFKA_TOPIC env var)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration and test JSON serialization without connecting to Kafka"
    )

    args = parser.parse_args()

    sample_tx = {
        "order_id": 10001,
        "customer_id": "C123",
        "product_id": "P501",
        "quantity": 2,
        "price": 1200.50,
        "tax_amount": 216.09,
        "payment_method": "UPI",
        "timestamp": "2026-08-22T12:00:00+00:00"
    }

    try:
        servers, topic = load_kafka_config(args.bootstrap_servers, args.topic)
        logger.info(f"Kafka configuration loaded: broker={servers}, topic={topic}")

        serialized = serialize_transaction(sample_tx)
        logger.info(f"Sample transaction serialized successfully ({len(serialized)} bytes):")
        logger.info(serialized.decode("utf-8"))

        if args.dry_run:
            logger.info("Dry-run mode completed successfully.")
            return

        service = KafkaProducerService(bootstrap_servers=servers, topic=topic)
        service.send_transaction(sample_tx)
        service.flush()
        logger.info("Sample transaction published successfully.")
        service.close()

    except KafkaProducerError as e:
        logger.error(f"Kafka Producer Error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
