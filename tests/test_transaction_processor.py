import pytest
from flink.transaction_processor import (
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


# 5. Invalid numeric value rejection
def test_invalid_numeric_values():
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

    # String price
    tx3 = get_valid_sample_transaction()
    tx3["price"] = "1000.00"
    valid, errors = validate_transaction(tx3)
    assert valid is False
    assert "price must be a number" in errors

    # Negative price
    tx4 = get_valid_sample_transaction()
    tx4["price"] = -50.0
    valid, errors = validate_transaction(tx4)
    assert valid is False
    assert "price cannot be negative" in errors

    # Negative tax amount
    tx5 = get_valid_sample_transaction()
    tx5["tax_amount"] = -10.0
    valid, errors = validate_transaction(tx5)
    assert valid is False
    assert "tax_amount cannot be negative" in errors

    # Zero or negative quantity
    tx6 = get_valid_sample_transaction()
    tx6["quantity"] = 0
    valid, errors = validate_transaction(tx6)
    assert valid is False
    assert "quantity must be greater than 0" in errors


# 6. Invalid JSON handling
def test_invalid_json_handling():
    malformed = '{"order_id": 10001, "customer_id": "C123", invalid_json}'
    ok, parsed, err = parse_json(malformed)
    assert ok is False
    assert parsed is None
    assert "Malformed JSON" in err

    processed, errors = parse_and_process_record(malformed)
    assert processed is None
    assert len(errors) > 0


# 7. Correct total_amount calculation & 8. Original fields preserved & 9. Processed transaction contains total_amount
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


# 10. Invalid transactions do not silently become valid transactions
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
