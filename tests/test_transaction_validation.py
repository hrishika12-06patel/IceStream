import pytest

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

def test_missing_customer_id():
    transaction = get_valid_transaction()

    del transaction["customer_id"]

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "Missing required field: customer_id" in errors


def test_missing_product_id():
    transaction = get_valid_transaction()

    del transaction["product_id"]

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "Missing required field: product_id" in errors


def test_missing_quantity():
    transaction = get_valid_transaction()

    del transaction["quantity"]

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "Missing required field: quantity" in errors


def test_missing_price():
    transaction = get_valid_transaction()

    del transaction["price"]

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "Missing required field: price" in errors


def test_missing_tax_amount():
    transaction = get_valid_transaction()

    del transaction["tax_amount"]

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "Missing required field: tax_amount" in errors


def test_missing_payment_method():
    transaction = get_valid_transaction()

    del transaction["payment_method"]

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "Missing required field: payment_method" in errors


def test_missing_timestamp():
    transaction = get_valid_transaction()

    del transaction["timestamp"]

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "Missing required field: timestamp" in errors

def test_null_order_id():
    transaction = get_valid_transaction()

    transaction["order_id"] = None

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "NULL value is not allowed: order_id" in errors

def test_null_customer_id():
    transaction = get_valid_transaction()

    transaction["customer_id"] = None

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "NULL value is not allowed: customer_id" in errors

def test_null_product_id():
    transaction = get_valid_transaction()

    transaction["product_id"] = None

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "NULL value is not allowed: product_id" in errors


def test_null_quantity():
    transaction = get_valid_transaction()

    transaction["quantity"] = None

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "NULL value is not allowed: quantity" in errors


def test_null_price():
    transaction = get_valid_transaction()

    transaction["price"] = None

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "NULL value is not allowed: price" in errors


def test_null_tax_amount():
    transaction = get_valid_transaction()

    transaction["tax_amount"] = None

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "NULL value is not allowed: tax_amount" in errors


def test_null_payment_method():
    transaction = get_valid_transaction()

    transaction["payment_method"] = None

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "NULL value is not allowed: payment_method" in errors


def test_null_timestamp():
    transaction = get_valid_transaction()

    transaction["timestamp"] = None

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "NULL value is not allowed: timestamp" in errors

def test_order_id_must_be_string():
    transaction = get_valid_transaction()

    transaction["order_id"] = 12345

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "order_id must be a string" in errors


def test_customer_id_must_be_string():
    transaction = get_valid_transaction()

    transaction["customer_id"] = 12345

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "customer_id must be a string" in errors


def test_product_id_must_be_string():
    transaction = get_valid_transaction()

    transaction["product_id"] = 12345

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "product_id must be a string" in errors

def test_empty_order_id():
    transaction = get_valid_transaction()

    transaction["order_id"] = ""

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "order_id cannot be empty" in errors


def test_empty_customer_id():
    transaction = get_valid_transaction()

    transaction["customer_id"] = ""

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "customer_id cannot be empty" in errors


def test_empty_product_id():
    transaction = get_valid_transaction()

    transaction["product_id"] = ""

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "product_id cannot be empty" in errors


def test_whitespace_order_id():
    transaction = get_valid_transaction()

    transaction["order_id"] = "   "

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "order_id cannot be empty" in errors


def test_negative_quantity():
    transaction = get_valid_transaction()

    transaction["quantity"] = -5

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "quantity must be greater than 0" in errors


def test_zero_quantity():
    transaction = get_valid_transaction()

    transaction["quantity"] = 0

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "quantity must be greater than 0" in errors


def test_quantity_must_be_integer():
    transaction = get_valid_transaction()

    transaction["quantity"] = "2"

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "quantity must be an integer" in errors


def test_boolean_quantity_is_invalid():
    transaction = get_valid_transaction()

    transaction["quantity"] = True

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "quantity must be an integer" in errors


def test_negative_price():
    transaction = get_valid_transaction()

    transaction["price"] = -100

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "price cannot be negative" in errors


def test_price_must_be_number():
    transaction = get_valid_transaction()

    transaction["price"] = "499.99"

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "price must be a number" in errors


def test_zero_price_is_valid():
    transaction = get_valid_transaction()

    transaction["price"] = 0

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is True
    assert errors == []

def test_negative_tax_amount():
    transaction = get_valid_transaction()

    transaction["tax_amount"] = -50

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "tax_amount cannot be negative" in errors


def test_tax_amount_must_be_number():
    transaction = get_valid_transaction()

    transaction["tax_amount"] = "50.00"

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "tax_amount must be a number" in errors


def test_zero_tax_amount_is_valid():
    transaction = get_valid_transaction()

    transaction["tax_amount"] = 0

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is True
    assert errors == []


@pytest.mark.parametrize(
    "payment_method",
    [
        "UPI",
        "Credit Card",
        "Debit Card",
        "Cash",
        "Net Banking",
        "Wallet",
    ],
)
def test_valid_payment_methods(payment_method):
    transaction = get_valid_transaction()

    transaction["payment_method"] = payment_method

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is True
    assert errors == []


def test_invalid_payment_method():
    transaction = get_valid_transaction()

    transaction["payment_method"] = "Bitcoin"

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "Invalid payment_method: Bitcoin" in errors


def test_valid_timestamp():
    transaction = get_valid_transaction()

    transaction["timestamp"] = "2026-08-20T15:45:30"

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is True
    assert errors == []


def test_invalid_timestamp():
    transaction = get_valid_transaction()

    transaction["timestamp"] = "not-a-valid-date"

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "timestamp must be a valid ISO 8601 datetime" in errors


def test_timestamp_must_be_string():
    transaction = get_valid_transaction()

    transaction["timestamp"] = 123456789

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False
    assert "timestamp must be a string" in errors


def test_multiple_validation_errors():
    transaction = get_valid_transaction()

    transaction["customer_id"] = None
    transaction["quantity"] = -5
    transaction["price"] = -100
    transaction["tax_amount"] = -20
    transaction["payment_method"] = "Bitcoin"
    transaction["timestamp"] = "invalid-date"

    is_valid, errors = validate_transaction(transaction)

    assert is_valid is False

    assert "NULL value is not allowed: customer_id" in errors
    assert "quantity must be greater than 0" in errors
    assert "price cannot be negative" in errors
    assert "tax_amount cannot be negative" in errors
    assert "Invalid payment_method: Bitcoin" in errors
    assert "timestamp must be a valid ISO 8601 datetime" in errors