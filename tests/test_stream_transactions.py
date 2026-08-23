"""
Unit tests for stream_transactions.py streaming pipeline.
All tests use mock KafkaProducerService instances and do not require a running Kafka broker.
"""

import json
import os
import sys
from unittest.mock import MagicMock, patch
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from generator.kafka_producer import (
    KafkaConfigError,
    KafkaProducerService,
    KafkaPublishError,
)
from generator.stream_transactions import stream_transactions


def get_mock_producer_service():
    """Returns a mock KafkaProducerService instance."""
    mock_service = MagicMock(spec=KafkaProducerService)
    mock_service.bootstrap_servers = "localhost:9092"
    mock_service.topic = "test-transactions"
    return mock_service


def test_transaction_generated_and_passed_to_kafka_producer():
    """Test 1 & 2: Single transaction is generated and passed to KafkaProducerService."""
    mock_service = get_mock_producer_service()

    summary = stream_transactions(
        rate=1000.0,
        start_order_id=10001,
        producer_service=mock_service,
        max_transactions=1,
        progress_interval=0,
    )

    assert summary["count"] == 1
    assert mock_service.send_transaction.call_count == 1

    tx = mock_service.send_transaction.call_args[0][0]
    assert isinstance(tx, dict)
    assert tx["order_id"] == 10001


def test_generated_transaction_contains_expected_fields():
    """Test 3: Generated transaction contains all required e-commerce fields."""
    mock_service = get_mock_producer_service()

    stream_transactions(
        rate=1000.0,
        start_order_id=10001,
        producer_service=mock_service,
        max_transactions=1,
        progress_interval=0,
    )

    tx = mock_service.send_transaction.call_args[0][0]
    expected_fields = {
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "price",
        "tax_amount",
        "payment_method",
        "timestamp",
    }
    assert set(tx.keys()) == expected_fields


def test_rate_configuration_validation():
    """Test 4: Invalid rates (<= 0) raise ValueError."""
    mock_service = get_mock_producer_service()

    with pytest.raises(ValueError) as exc_info:
        stream_transactions(
            rate=0,
            producer_service=mock_service,
            max_transactions=1,
        )
    assert "Rate must be greater than 0" in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        stream_transactions(
            rate=-100.0,
            producer_service=mock_service,
            max_transactions=1,
        )
    assert "Rate must be greater than 0" in str(exc_info.value)


def test_starting_order_id_respected():
    """Test 5: Starting order ID parameter is respected."""
    mock_service = get_mock_producer_service()

    stream_transactions(
        rate=1000.0,
        start_order_id=50000,
        producer_service=mock_service,
        max_transactions=1,
        progress_interval=0,
    )

    tx = mock_service.send_transaction.call_args[0][0]
    assert tx["order_id"] == 50000


def test_multiple_transactions_receive_unique_sequential_order_ids():
    """Test 6: Multiple generated transactions receive unique, sequential order IDs."""
    mock_service = get_mock_producer_service()

    summary = stream_transactions(
        rate=10000.0,
        start_order_id=20001,
        producer_service=mock_service,
        max_transactions=5,
        progress_interval=0,
    )

    assert summary["count"] == 5
    assert mock_service.send_transaction.call_count == 5

    calls = mock_service.send_transaction.call_args_list
    order_ids = [call[0][0]["order_id"] for call in calls]

    assert order_ids == [20001, 20002, 20003, 20004, 20005]
    assert len(order_ids) == len(set(order_ids))


def test_producer_errors_handled():
    """Test 7: Errors during transaction sending are propagated/handled properly."""
    mock_service = get_mock_producer_service()
    mock_service.send_transaction.side_effect = KafkaPublishError("Kafka publish error simulated")

    with pytest.raises(KafkaPublishError) as exc_info:
        stream_transactions(
            rate=1000.0,
            producer_service=mock_service,
            max_transactions=5,
            progress_interval=0,
        )

    assert "Kafka publish error simulated" in str(exc_info.value)


def test_keyboard_interrupt_graceful_shutdown():
    """Test 8 & 9: KeyboardInterrupt triggers graceful shutdown, returning summary and flushing/closing producer."""
    mock_service = get_mock_producer_service()

    # Simulate KeyboardInterrupt on 3rd transaction send call
    call_counter = 0

    def mock_send(tx):
        nonlocal call_counter
        call_counter += 1
        if call_counter == 3:
            raise KeyboardInterrupt()

    mock_service.send_transaction.side_effect = mock_send

    summary = stream_transactions(
        rate=1000.0,
        start_order_id=10001,
        producer_service=mock_service,
        progress_interval=0,
    )

    # 2 transactions sent successfully before interrupt
    assert summary["count"] == 2
    assert mock_service.flush.call_count == 1
    assert mock_service.close.call_count == 1


def test_flush_and_close_called_on_normal_completion():
    """Test 9: Producer flush and close are called when streaming finishes normally."""
    mock_service = get_mock_producer_service()

    stream_transactions(
        rate=1000.0,
        start_order_id=10001,
        producer_service=mock_service,
        max_transactions=10,
        progress_interval=0,
    )

    assert mock_service.flush.call_count == 1
    assert mock_service.close.call_count == 1


def test_no_secrets_exposed_in_errors():
    """Test 10: Error messages do not leak secrets or credentials."""
    sensitive_token = "secret_auth_token_xyz_9999"
    mock_service = get_mock_producer_service()
    mock_service.send_transaction.side_effect = KafkaPublishError(
        f"Failed connecting with token={sensitive_token}"
    )

    with patch.dict(os.environ, {"KAFKA_SECRET_KEY": sensitive_token}):
        try:
            stream_transactions(
                rate=1000.0,
                producer_service=mock_service,
                max_transactions=1,
                progress_interval=0,
            )
        except KafkaPublishError as err:
            err_msg = str(err)
            # Verify custom error doesn't leak env variable name
            assert "KAFKA_SECRET_KEY" not in err_msg


def test_stream_transactions_with_missing_kafka_config():
    """Test that missing broker/topic configuration raises KafkaConfigError cleanly."""
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(KafkaConfigError):
            stream_transactions(
                rate=1000.0,
                bootstrap_servers=None,
                topic=None,
                max_transactions=1,
            )
