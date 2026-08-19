from validation.transaction_validator import validate_transaction


def get_valid_transaction():
    """
    Return a valid transaction that can be reused
    by multiple tests.
    """

    return {
        "order_id": "ORD001",
        "customer_id": "CUS001",
        "product_id": "PROD001",
        "quantity": 2,
        "price": 499.99,
        "tax_amount": 50.00,
        "payment_method": "UPI",
        "timestamp": "2026-08-19T10:30:00",
    }


def test_valid_transaction():
    transaction = get_valid_transaction()

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is True
    assert errors == []


def test_missing_order_id():
    transaction = get_valid_transaction()

    del transaction["order_id"]

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "Missing required field: order_id" in errors


def test_null_customer_id():
    transaction = get_valid_transaction()

    transaction["customer_id"] = None

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "NULL value is not allowed: customer_id" in errors


def test_negative_quantity():
    transaction = get_valid_transaction()

    transaction["quantity"] = -5

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "quantity must be greater than 0" in errors


def test_negative_price():
    transaction = get_valid_transaction()

    transaction["price"] = -100

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "price cannot be negative" in errors


def test_invalid_payment_method():
    transaction = get_valid_transaction()

    transaction["payment_method"] = "Bitcoin"

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "Invalid payment_method: Bitcoin" in errors


def test_invalid_timestamp():
    transaction = get_valid_transaction()

    transaction["timestamp"] = "not-a-valid-date"

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "timestamp must be a valid ISO 8601 datetime" in errors