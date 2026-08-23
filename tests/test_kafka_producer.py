import json
import os
import sys
from unittest.mock import MagicMock, patch
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from generator.kafka_producer import (

    KafkaProducerService,
    KafkaConfigError,
    TransactionValidationError,
    SerializationError,
    KafkaPublishError,
    load_kafka_config,
    serialize_transaction,
)


def get_sample_transaction():
    return {
        "order_id": 10001,
        "customer_id": "C123",
        "product_id": "P501",
        "quantity": 2,
        "price": 1200.50,
        "tax_amount": 216.09,
        "payment_method": "UPI",
        "timestamp": "2026-08-22T12:00:00+00:00",
    }


def test_valid_transaction_serialization():
    """Test 1 & 2: Valid transaction serializes correctly to JSON UTF-8 bytes."""
    tx = get_sample_transaction()
    serialized = serialize_transaction(tx)

    assert isinstance(serialized, bytes)
    decoded = json.loads(serialized.decode("utf-8"))
    assert decoded["order_id"] == 10001
    assert decoded["customer_id"] == "C123"
    assert decoded["product_id"] == "P501"
    assert decoded["price"] == 1200.50
    assert decoded["timestamp"] == "2026-08-22T12:00:00+00:00"


def test_configured_topic_and_broker():
    """Test 3 & 4: Configured topic and broker are correctly loaded and used by producer service."""
    mock_producer = MagicMock()
    service = KafkaProducerService(
        bootstrap_servers="localhost:9092",
        topic="test-transactions",
        producer=mock_producer,
    )

    assert service.bootstrap_servers == "localhost:9092"
    assert service.topic == "test-transactions"

    tx = get_sample_transaction()
    service.send_transaction(tx)

    mock_producer.send.assert_called_once()
    args, kwargs = mock_producer.send.call_args
    assert args[0] == "test-transactions"
    assert json.loads(kwargs["value"].decode("utf-8")) == tx


def test_missing_broker_configuration():
    """Test 5: Missing broker configuration is detected."""
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(KafkaConfigError) as exc_info:
            load_kafka_config(bootstrap_servers=None, topic="transactions")
        assert "Missing Kafka broker configuration" in str(exc_info.value)


def test_missing_topic_configuration():
    """Test 6: Missing topic configuration is detected."""
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(KafkaConfigError) as exc_info:
            load_kafka_config(bootstrap_servers="localhost:9092", topic=None)
        assert "Missing Kafka topic configuration" in str(exc_info.value)


def test_invalid_transaction_input_rejected():
    """Test 7: Non-dictionary transaction input raises TransactionValidationError."""
    invalid_inputs = ["not a dict", 12345, [1, 2, 3], None, True]
    for invalid_input in invalid_inputs:
        with pytest.raises(TransactionValidationError) as exc_info:
            serialize_transaction(invalid_input)
        assert "must be a dictionary" in str(exc_info.value)


def test_json_serialization_error_handling():
    """Test 8: Unserializable data in transaction raises SerializationError."""
    unserializable_tx = {"order_id": 101, "unserializable_field": object()}
    with pytest.raises(SerializationError) as exc_info:
        serialize_transaction(unserializable_tx)
    assert "Failed to serialize transaction to JSON" in str(exc_info.value)


def test_producer_error_handling():
    """Test 9: Producer send errors are caught and raised cleanly as KafkaPublishError."""
    mock_producer = MagicMock()
    mock_producer.send.side_effect = Exception("Kafka connection timeout")

    service = KafkaProducerService(
        bootstrap_servers="localhost:9092",
        topic="transactions",
        producer=mock_producer,
    )

    with pytest.raises(KafkaPublishError) as exc_info:
        service.send_transaction(get_sample_transaction())
    assert "Error publishing message to Kafka topic" in str(exc_info.value)


def test_no_secrets_exposed_in_error_messages():
    """Test 10: Error messages do not leak secrets or credentials."""
    sensitive_token = "secret_auth_token_xyz_9999"
    with patch.dict(os.environ, {"KAFKA_BOOTSTRAP_SERVERS": "broker:9092", "KAFKA_SECRET": sensitive_token}):
        try:
            serialize_transaction("invalid_tx")
        except TransactionValidationError as err:
            err_msg = str(err)
            assert sensitive_token not in err_msg
            assert "KAFKA_SECRET" not in err_msg


def test_service_context_manager():
    """Test that KafkaProducerService works cleanly as a context manager."""
    mock_producer = MagicMock()
    with KafkaProducerService(
        bootstrap_servers="localhost:9092",
        topic="transactions",
        producer=mock_producer,
    ) as service:
        service.send_transaction(get_sample_transaction())

    mock_producer.close.assert_called_once()
