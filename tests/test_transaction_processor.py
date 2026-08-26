import json
import pytest
from flink.transaction_processor import (
    ProcessingMetrics,
    TransactionProcessMapFunction,
    parse_json,
    validate_transaction,
    process_transaction,
    parse_and_process_record,
)


def get_valid_sample_transaction():
    """Returns a fresh valid sample transaction dict."""
    return {
        "order_id": 10001,
        "customer_id": "C123",
        "product_id": "P501",
        "quantity": 2,
        "price": 1000.00,
        "tax_amount": 180.00,
        "payment_method": "UPI",
        "timestamp": "2026-08-24T10:00:00+00:00",
    }


# 1. Valid transaction parsing
def test_valid_json_parsing():
    json_str = '{"order_id": 10001, "customer_id": "C123", "product_id": "P501", "quantity": 2, "price": 1000.0, "tax_amount": 180.0, "payment_method": "UPI", "timestamp": "2026-08-24T10:00:00+00:00"}'
    ok, parsed, err = parse_json(json_str)
    assert ok is True
    assert err is None
    assert isinstance(parsed, dict)
    assert parsed["order_id"] == 10001


def test_valid_json_bytes_parsing():
    json_bytes = b'{"order_id": 10002, "customer_id": "C124", "product_id": "P502", "quantity": 1, "price": 500.0, "tax_amount": 90.0, "payment_method": "Credit Card", "timestamp": "2026-08-24T10:05:00+00:00"}'
    ok, parsed, err = parse_json(json_bytes)
    assert ok is True
    assert err is None
    assert isinstance(parsed, dict)
    assert parsed["order_id"] == 10002


# 2. Required field validation & 3. Missing field rejection
@pytest.mark.parametrize(
    "missing_field",
    [
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "price",
        "tax_amount",
        "payment_method",
        "timestamp",
    ],
)
def test_missing_required_field_rejection(missing_field):
    tx = get_valid_sample_transaction()
    del tx[missing_field]
    valid, errors = validate_transaction(tx)
    assert valid is False
    assert any(f"Missing required field: {missing_field}" in e for e in errors)


# 4. Null field rejection
@pytest.mark.parametrize(
    "null_field",
    [
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "price",
        "tax_amount",
        "payment_method",
        "timestamp",
    ],
)
def test_null_field_rejection(null_field):
    tx = get_valid_sample_transaction()
    tx[null_field] = None
    valid, errors = validate_transaction(tx)
    assert valid is False
    assert any(f"NULL value is not allowed: {null_field}" in e for e in errors)


# 5. Invalid quantity rejection
def test_invalid_quantity_rejection():
    # String quantity
    tx1 = get_valid_sample_transaction()
    tx1["quantity"] = "2"
    valid, errors = validate_transaction(tx1)
    assert valid is False
    assert "quantity must be an integer" in errors

    # Boolean quantity
    tx2 = get_valid_sample_transaction()
    tx2["quantity"] = True
    valid, errors = validate_transaction(tx2)
    assert valid is False
    assert "quantity must be an integer" in errors

    # Zero or negative quantity
    tx3 = get_valid_sample_transaction()
    tx3["quantity"] = 0
    valid, errors = validate_transaction(tx3)
    assert valid is False
    assert "quantity must be greater than 0" in errors

    tx4 = get_valid_sample_transaction()
    tx4["quantity"] = -3
    valid, errors = validate_transaction(tx4)
    assert valid is False
    assert "quantity must be greater than 0" in errors


# 6. Invalid price rejection
def test_invalid_price_rejection():
    # String price
    tx1 = get_valid_sample_transaction()
    tx1["price"] = "1000.00"
    valid, errors = validate_transaction(tx1)
    assert valid is False
    assert "price must be a number" in errors

    # Negative price
    tx2 = get_valid_sample_transaction()
    tx2["price"] = -50.0
    valid, errors = validate_transaction(tx2)
    assert valid is False
    assert "price cannot be negative" in errors


# 7. Invalid tax rejection
def test_invalid_tax_rejection():
    # String tax
    tx1 = get_valid_sample_transaction()
    tx1["tax_amount"] = "180.00"
    valid, errors = validate_transaction(tx1)
    assert valid is False
    assert "tax_amount must be a number" in errors

    # Negative tax amount
    tx2 = get_valid_sample_transaction()
    tx2["tax_amount"] = -10.0
    valid, errors = validate_transaction(tx2)
    assert valid is False
    assert "tax_amount cannot be negative" in errors


# 8. Invalid timestamp rejection
def test_invalid_timestamp_rejection():
    # Non-ISO string
    tx1 = get_valid_sample_transaction()
    tx1["timestamp"] = "2026-99-99 25:00:00"
    valid, errors = validate_transaction(tx1)
    assert valid is False
    assert "timestamp must be a valid ISO 8601 datetime" in errors

    # Non-string timestamp
    tx2 = get_valid_sample_transaction()
    tx2["timestamp"] = 1700000000
    valid, errors = validate_transaction(tx2)
    assert valid is False
    assert "timestamp must be a string" in errors


# 9. Invalid JSON handling
def test_invalid_json_handling():
    malformed = '{"order_id": 10001, "customer_id": "C123", invalid_json}'
    ok, parsed, err = parse_json(malformed)
    assert ok is False
    assert parsed is None
    assert "Malformed JSON" in err

    processed, errors = parse_and_process_record(malformed)
    assert processed is None
    assert len(errors) > 0


# 10. Correct total_amount calculation & 11. Original fields preserved
def test_total_amount_calculation_and_field_preservation():
    tx = {
        "order_id": 10001,
        "customer_id": "C123",
        "product_id": "P501",
        "quantity": 2,
        "price": 1000.00,
        "tax_amount": 180.00,
        "payment_method": "UPI",
        "timestamp": "2026-08-24T10:00:00+00:00",
    }

    processed = process_transaction(tx)

    # Check original fields remain intact
    for k, v in tx.items():
        assert processed[k] == v

    # Check total_amount present and correct: (1000.00 * 2) + 180.00 = 2180.00
    assert "total_amount" in processed
    assert processed["total_amount"] == 2180.00


def test_total_amount_calculation_with_floats():
    tx = get_valid_sample_transaction()
    tx["quantity"] = 3
    tx["price"] = 15.50
    tx["tax_amount"] = 8.37

    processed = process_transaction(tx)
    # (15.50 * 3) + 8.37 = 46.50 + 8.37 = 54.87
    assert processed["total_amount"] == 54.87


# 12. Invalid records pipeline rejection
def test_invalid_records_pipeline_rejection():
    # String quantity in pipeline
    raw_invalid = '{"order_id": 10001, "customer_id": "C123", "product_id": "P501", "quantity": "invalid", "price": 100.0, "tax_amount": 18.0, "payment_method": "UPI", "timestamp": "2026-08-24T10:00:00+00:00"}'
    processed, errors = parse_and_process_record(raw_invalid)
    assert processed is None
    assert len(errors) > 0

    # Negative price in pipeline
    raw_neg_price = '{"order_id": 10001, "customer_id": "C123", "product_id": "P501", "quantity": 1, "price": -10.0, "tax_amount": 18.0, "payment_method": "UPI", "timestamp": "2026-08-24T10:00:00+00:00"}'
    processed2, errors2 = parse_and_process_record(raw_neg_price)
    assert processed2 is None
    assert "price cannot be negative" in errors2


def test_valid_string_order_id_supported():
    tx = get_valid_sample_transaction()
    tx["order_id"] = "ORD-9999"
    valid, errors = validate_transaction(tx)
    assert valid is True
    assert errors == []


# 13. Processing error handling test
def test_processing_error_handling(monkeypatch):
    tx = get_valid_sample_transaction()
    metrics = ProcessingMetrics()

    def mock_process_transaction(transaction):
        raise ValueError("Simulated calculation error")

    monkeypatch.setattr("flink.transaction_processor.process_transaction", mock_process_transaction)

    processed, errors = parse_and_process_record(tx, metrics=metrics)
    assert processed is None
    assert any("Calculation error" in e for e in errors)
    assert metrics.processing_errors == 1


# 14. Metrics tracking logic test
def test_processing_metrics_tracking():
    metrics = ProcessingMetrics()
    assert metrics.total_records_received == 0
    assert metrics.valid_records_processed == 0
    assert metrics.invalid_records == 0
    assert metrics.processing_errors == 0

    # Process valid record
    tx_valid = json.dumps(get_valid_sample_transaction())
    proc1, errs1 = parse_and_process_record(tx_valid, metrics=metrics)
    assert proc1 is not None
    assert metrics.total_records_received == 1
    assert metrics.valid_records_processed == 1
    assert metrics.invalid_records == 0

    # Process malformed JSON
    proc2, errs2 = parse_and_process_record("invalid json", metrics=metrics)
    assert proc2 is None
    assert metrics.total_records_received == 2
    assert metrics.valid_records_processed == 1
    assert metrics.invalid_records == 1

    # Check metrics dictionary export
    metrics_dict = metrics.to_dict()
    assert metrics_dict["total_records_received"] == 2
    assert metrics_dict["valid_records_processed"] == 1
    assert metrics_dict["invalid_records"] == 1
    assert "records_per_second" in metrics_dict


# MapFunction test
def test_transaction_process_map_function():
    metrics = ProcessingMetrics()
    map_func = TransactionProcessMapFunction(metrics=metrics)

    # Valid transaction
    valid_raw = json.dumps(get_valid_sample_transaction())
    res_valid = map_func.map(valid_raw)
    assert res_valid is not None
    parsed_res = json.loads(res_valid)
    assert parsed_res["total_amount"] == 2180.00
    assert map_func.metrics.valid_records_processed == 1

    # Invalid transaction
    invalid_raw = '{"order_id": 10002, "quantity": -5}'
    res_invalid = map_func.map(invalid_raw)
    assert res_invalid is None
    assert map_func.metrics.invalid_records == 1
    assert map_func.metrics.total_records_received == 2


# Anomaly records test
def test_anomaly_records_handling():
    metrics = ProcessingMetrics()

    # Anomaly 1: Malformed JSON
    malformed_json = '{"order_id": 10001, "customer_id": "C123",'
    p1, e1 = parse_and_process_record(malformed_json, metrics=metrics)
    assert p1 is None
    assert len(e1) > 0

    # Anomaly 2: Missing required field
    missing_field_tx = json.dumps({
        "order_id": 10002,
        "customer_id": "C123",
        "quantity": 2,
        "price": 100.0,
        "tax_amount": 10.0,
        "payment_method": "UPI",
        "timestamp": "2026-08-24T10:00:00+00:00"
    })
    p2, e2 = parse_and_process_record(missing_field_tx, metrics=metrics)
    assert p2 is None

    # Anomaly 3: NULL required field
    null_field_tx = json.dumps({
        "order_id": 10003,
        "customer_id": None,
        "product_id": "P501",
        "quantity": 2,
        "price": 100.0,
        "tax_amount": 10.0,
        "payment_method": "UPI",
        "timestamp": "2026-08-24T10:00:00+00:00"
    })
    p3, e3 = parse_and_process_record(null_field_tx, metrics=metrics)
    assert p3 is None

    # Anomaly 4: Invalid numeric value
    invalid_numeric_tx = json.dumps({
        "order_id": 10004,
        "customer_id": "C123",
        "product_id": "P501",
        "quantity": -10,
        "price": 100.0,
        "tax_amount": 10.0,
        "payment_method": "UPI",
        "timestamp": "2026-08-24T10:00:00+00:00"
    })
    p4, e4 = parse_and_process_record(invalid_numeric_tx, metrics=metrics)
    assert p4 is None

    # Summary metric assertions for all 4 anomaly records
    assert metrics.total_records_received == 4
    assert metrics.valid_records_processed == 0
    assert metrics.invalid_records == 4
