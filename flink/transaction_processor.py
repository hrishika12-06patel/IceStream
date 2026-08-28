"""
IceStream Flink Transaction Processor.

Consumes transaction messages from Apache Kafka, parses JSON payloads,
validates transaction data, calculates total_amount, tracks processing metrics,
and outputs processed transactions.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

REQUIRED_FIELDS = [
    "order_id",
    "customer_id",
    "product_id",
    "quantity",
    "price",
    "tax_amount",
    "payment_method",
    "timestamp",
]


class ProcessingMetrics:
    """
    Tracks basic stream processing metrics for the Flink job.

    Metrics:
    - total_records_received: Total input messages received from Kafka source.
    - valid_records_processed: Records that successfully passed JSON parsing and schema validation.
    - invalid_records: Records that failed JSON parsing or schema validation.
    - processing_errors: Errors encountered during payload computation/processing.
    - records_per_second: Rate of record processing per second since initialization.
    """

    def __init__(self) -> None:
        self.total_records_received: int = 0
        self.valid_records_processed: int = 0
        self.invalid_records: int = 0
        self.processing_errors: int = 0
        self.start_time: float = time.time()

    def record_received(self) -> None:
        self.total_records_received += 1

    def record_valid(self) -> None:
        self.valid_records_processed += 1

    def record_invalid(self) -> None:
        self.invalid_records += 1

    def record_error(self) -> None:
        self.processing_errors += 1

    @property
    def records_per_second(self) -> float:
        elapsed = time.time() - self.start_time
        if elapsed <= 0:
            return 0.0
        return round(self.total_records_received / elapsed, 2)

    def reset(self) -> None:
        self.total_records_received = 0
        self.valid_records_processed = 0
        self.invalid_records = 0
        self.processing_errors = 0
        self.start_time = time.time()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_records_received": self.total_records_received,
            "valid_records_processed": self.valid_records_processed,
            "invalid_records": self.invalid_records,
            "processing_errors": self.processing_errors,
            "records_per_second": self.records_per_second,
        }


try:
    from pyflink.datastream.functions import RichMapFunction
    _BASE_MAP_CLASS = RichMapFunction
except ImportError:
    _BASE_MAP_CLASS = object


class TransactionProcessMapFunction(_BASE_MAP_CLASS):
    """
    PyFlink MapFunction that parses JSON, validates schema, calculates total_amount,
    and updates processing metrics.
    """

    def __init__(self, metrics: Optional[ProcessingMetrics] = None) -> None:
        if _BASE_MAP_CLASS is not object:
            super().__init__()
        self.metrics = metrics if metrics is not None else ProcessingMetrics()
        self._flink_counter_received: Any = None
        self._flink_counter_valid: Any = None
        self._flink_counter_invalid: Any = None
        self._flink_counter_errors: Any = None

    def open(self, runtime_context: Any) -> None:
        """Initializes Flink metrics counters when running within PyFlink runtime."""
        try:
            metric_group = runtime_context.get_metrics_group()
            self._flink_counter_received = metric_group.counter("total_records_received")
            self._flink_counter_valid = metric_group.counter("valid_records_processed")
            self._flink_counter_invalid = metric_group.counter("invalid_records")
            self._flink_counter_errors = metric_group.counter("processing_errors")
        except Exception:
            pass  # Standalone/test fallback

    def map(self, raw_record: Any) -> Optional[str]:
        """
        Processes a raw input record string/bytes/dict.

        Returns:
            JSON string of processed transaction if valid, None if invalid or error.
        """
        self.metrics.record_received()
        if self._flink_counter_received:
            self._flink_counter_received.inc()

        try:
            processed, errors = parse_and_process_record(raw_record)
            if processed is not None:
                self.metrics.record_valid()
                if self._flink_counter_valid:
                    self._flink_counter_valid.inc()
                return json.dumps(processed)
            else:
                self.metrics.record_invalid()
                if self._flink_counter_invalid:
                    self._flink_counter_invalid.inc()
                return None
        except Exception as exc:
            self.metrics.record_error()
            if self._flink_counter_errors:
                self._flink_counter_errors.inc()
            logger.error(f"Unexpected error processing record: {exc}")
            return None


def load_kafka_config(
    bootstrap_servers: Optional[str] = None,
    topic: Optional[str] = None,
) -> Tuple[str, str]:
    """
    Loads Kafka configuration from environment variables or parameters.

    Args:
        bootstrap_servers: Optional Kafka broker address override.
        topic: Optional Kafka topic override.

    Returns:
        Tuple of (bootstrap_servers, topic).
    """
    servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic = topic or os.getenv("KAFKA_TOPIC", "transactions")
    return str(servers).strip(), str(kafka_topic).strip()


def parse_json(raw_msg: Union[str, bytes, Dict[str, Any]]) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """
    Safely parses raw JSON payload into a dictionary without crashing.

    Args:
        raw_msg: Raw message as string, bytes, or pre-parsed dictionary.

    Returns:
        Tuple of (is_success, parsed_dict_or_none, error_message_or_none).
    """
    if isinstance(raw_msg, dict):
        return True, raw_msg, None

    if isinstance(raw_msg, (bytes, bytearray)):
        try:
            raw_msg = raw_msg.decode("utf-8")
        except UnicodeDecodeError:
            return False, None, "Invalid UTF-8 encoding in message."

    if not isinstance(raw_msg, str):
        return False, None, f"Unsupported message type: '{type(raw_msg).__name__}'."

    try:
        parsed = json.loads(raw_msg)
        if not isinstance(parsed, dict):
            return False, None, f"JSON payload must be a dict, got '{type(parsed).__name__}'."
        return True, parsed, None
    except json.JSONDecodeError as exc:
        return False, None, f"Malformed JSON: {exc.msg}"
    except Exception as exc:
        return False, None, f"JSON parse error: {str(exc)}"


def validate_transaction(transaction: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validates a transaction dictionary against required schema and business rules.

    Args:
        transaction: Transaction dictionary to validate.

    Returns:
        Tuple of (is_valid, list_of_error_messages).
    """
    errors: List[str] = []

    # 1. Missing required fields
    for field in REQUIRED_FIELDS:
        if field not in transaction:
            errors.append(f"Missing required field: {field}")

    if errors:
        return False, errors

    # 2. NULL checks
    for field in REQUIRED_FIELDS:
        if transaction[field] is None:
            errors.append(f"NULL value is not allowed: {field}")

    if errors:
        return False, errors

    # 3. Field specific validations

    # order_id: integer or non-empty string
    order_id = transaction["order_id"]
    if isinstance(order_id, bool):
        errors.append("order_id cannot be a boolean")
    elif isinstance(order_id, int):
        if order_id < 0:
            errors.append("order_id must be non-negative")
    elif isinstance(order_id, str):
        if not order_id.strip():
            errors.append("order_id cannot be empty")
    else:
        errors.append("order_id must be an integer or a string")

    # customer_id: non-empty string
    customer_id = transaction["customer_id"]
    if not isinstance(customer_id, str):
        errors.append("customer_id must be a string")
    elif not customer_id.strip():
        errors.append("customer_id cannot be empty")

    # product_id: non-empty string
    product_id = transaction["product_id"]
    if not isinstance(product_id, str):
        errors.append("product_id must be a string")
    elif not product_id.strip():
        errors.append("product_id cannot be empty")

    # quantity: integer > 0
    quantity = transaction["quantity"]
    if isinstance(quantity, bool) or not isinstance(quantity, int):
        errors.append("quantity must be an integer")
    elif quantity <= 0:
        errors.append("quantity must be greater than 0")

    # price: numeric >= 0
    price = transaction["price"]
    if isinstance(price, bool) or not isinstance(price, (int, float)):
        errors.append("price must be a number")
    elif price < 0:
        errors.append("price cannot be negative")

    # tax_amount: numeric >= 0
    tax_amount = transaction["tax_amount"]
    if isinstance(tax_amount, bool) or not isinstance(tax_amount, (int, float)):
        errors.append("tax_amount must be a number")
    elif tax_amount < 0:
        errors.append("tax_amount cannot be negative")

    # payment_method: non-empty string
    payment_method = transaction["payment_method"]
    if not isinstance(payment_method, str):
        errors.append("payment_method must be a string")
    elif not payment_method.strip():
        errors.append("payment_method cannot be empty")

    # timestamp: ISO 8601 string
    timestamp = transaction["timestamp"]
    if not isinstance(timestamp, str):
        errors.append("timestamp must be a string")
    else:
        try:
            ts_str = timestamp.replace("Z", "+00:00") if timestamp.endswith("Z") else timestamp
            datetime.fromisoformat(ts_str)
        except ValueError:
            errors.append("timestamp must be a valid ISO 8601 datetime")

    if errors:
        return False, errors

    return True, []


def process_transaction(transaction: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calculates total_amount for a valid transaction.
    total_amount = (price * quantity) + tax_amount

    Args:
        transaction: Validated transaction dictionary.

    Returns:
        Dict containing original transaction fields plus total_amount.
    """
    price = float(transaction["price"])
    quantity = int(transaction["quantity"])
    tax_amount = float(transaction["tax_amount"])

    total_amount = round((price * quantity) + tax_amount, 2)

    processed = dict(transaction)
    processed["total_amount"] = total_amount
    return processed


def parse_and_process_record(
    raw_msg: Any,
    metrics: Optional[ProcessingMetrics] = None,
) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    """
    End-to-end single record processing pipeline:
    Parse JSON -> Validate -> Calculate total_amount -> Processed record.

    Args:
        raw_msg: Raw input message string/bytes or dict.
        metrics: Optional ProcessingMetrics instance to update.

    Returns:
        Tuple of (processed_transaction_or_none, list_of_errors).
    """
    if metrics:
        metrics.record_received()

    parsed_ok, tx_dict, parse_err = parse_json(raw_msg)
    if not parsed_ok or tx_dict is None:
        err_msg = parse_err or "JSON parsing failed."
        logger.warning(f"Rejected malformed transaction: {err_msg}")
        if metrics:
            metrics.record_invalid()
        return None, [err_msg]

    valid, val_errors = validate_transaction(tx_dict)
    if not valid:
        logger.warning(f"Rejected invalid transaction order_id={tx_dict.get('order_id')}: {val_errors}")
        if metrics:
            metrics.record_invalid()
        return None, val_errors

    try:
        processed = process_transaction(tx_dict)
        if metrics:
            metrics.record_valid()
        return processed, []
    except Exception as exc:
        err_msg = f"Calculation error: {str(exc)}"
        logger.error(f"Processing error for order_id={tx_dict.get('order_id')}: {err_msg}")
        if metrics:
            metrics.record_error()
        return None, [err_msg]


def create_execution_environment():
    """
    Creates and returns PyFlink StreamExecutionEnvironment.
    """
    try:
        from pyflink.datastream import StreamExecutionEnvironment
        env = StreamExecutionEnvironment.get_execution_environment()
        return env
    except ImportError as exc:
        raise RuntimeError(
            "PyFlink is not installed in the current environment. "
            "Ensure PyFlink is installed or run within the Flink container environment."
        ) from exc


def create_kafka_source(bootstrap_servers: str, topic: str):
    """
    Creates Kafka source for PyFlink environment.
    """
    try:
        from pyflink.common.serialization import SimpleStringSchema
        from pyflink.datastream.connectors.kafka import KafkaSource
        from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer

        source = (
            KafkaSource.builder()
            .set_bootstrap_servers(bootstrap_servers)
            .set_topics(topic)
            .set_group_id("icestream-flink-group")
            .set_starting_offsets(KafkaOffsetsInitializer.earliest())
            .set_value_only_deserializer(SimpleStringSchema())
            .build()
        )
        return source
    except ImportError as exc:
        raise RuntimeError("PyFlink Kafka connector dependencies not available.") from exc


def load_iceberg_config() -> Any:
    """
    Loads Iceberg configuration for downstream lakehouse storage.
    """
    try:
        from iceberg.iceberg_config import IcebergConfig
        return IcebergConfig.from_env()
    except Exception as exc:
        logger.warning(f"Could not load Iceberg configuration: {exc}")
        return None


def create_iceberg_sink_config() -> Dict[str, Any]:
    """
    Returns Iceberg sink and catalog configuration dictionary for Flink integration.
    """
    config = load_iceberg_config()
    if config:
        return {
            "catalog_name": config.catalog,
            "catalog_properties": config.get_catalog_properties(),
            "full_table_name": config.full_table_name,
        }
    return {}


def create_output_sink():
    """
    Returns output sink specification or handler description for output stream.
    Output is logged/printed to standard output/log stream.
    """
    return "stdout_print_sink"


def main() -> None:
    """
    Main entry point for running the Flink Streaming Job.
    """
    bootstrap_servers, topic = load_kafka_config()
    logger.info(f"Starting Flink Transaction Processor for broker={bootstrap_servers}, topic={topic}")

    try:
        env = create_execution_environment()
        kafka_source = create_kafka_source(bootstrap_servers, topic)

        from pyflink.common.watermark_strategy import WatermarkStrategy

        ds = env.from_source(
            source=kafka_source,
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="Kafka Transactions Source"
        )

        processor = TransactionProcessMapFunction()
        processed_stream = ds.map(processor).filter(lambda x: x is not None)
        processed_stream.print()

        env.execute("IceStream Flink Transaction Processor")
    except RuntimeError as exc:
        logger.error(f"Flink Runtime Error: {exc}")
        sys.exit(1)
    except Exception as exc:
        logger.error(f"Unexpected Flink Job Failure: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
