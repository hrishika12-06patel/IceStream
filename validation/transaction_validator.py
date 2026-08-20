from datetime import datetime


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

ALLOWED_PAYMENT_METHODS = {
    "UPI",
    "Credit Card",
    "Debit Card",
    "Cash",
    "Net Banking",
    "Wallet",
}


def validate_transaction(transaction: dict) -> tuple[bool, list[str]]:
    """
    Validate a single IceStream transaction.

    Returns:
        tuple:
            - True if the transaction is valid
            - False if the transaction is invalid
            - List of validation errors
    """

    errors = []

    # --------------------------------------------------
    # 1. Check whether all required fields are present
    # --------------------------------------------------

    for field in REQUIRED_FIELDS:
        if field not in transaction:
            errors.append(f"Missing required field: {field}")

    # Stop here if required fields are missing.
    # This prevents KeyError while checking the remaining fields.
    if errors:
        return False, errors

    # --------------------------------------------------
    # 2. Check for NULL values
    # --------------------------------------------------

    for field in REQUIRED_FIELDS:
        if transaction[field] is None:
            errors.append(f"NULL value is not allowed: {field}")

    # --------------------------------------------------
    # 3. Validate order_id
    # --------------------------------------------------

    if not isinstance(transaction["order_id"], str):
        errors.append("order_id must be a string")
    elif not transaction["order_id"].strip():
        errors.append("order_id cannot be empty")

    # --------------------------------------------------
    # 4. Validate customer_id
    # --------------------------------------------------

    if not isinstance(transaction["customer_id"], str):
        errors.append("customer_id must be a string")
    elif not transaction["customer_id"].strip():
        errors.append("customer_id cannot be empty")

    # --------------------------------------------------
    # 5. Validate product_id
    # --------------------------------------------------

    if not isinstance(transaction["product_id"], str):
        errors.append("product_id must be a string")
    elif not transaction["product_id"].strip():
        errors.append("product_id cannot be empty")

    # --------------------------------------------------
    # 6. Validate quantity
    # --------------------------------------------------

    if isinstance(transaction["quantity"], bool) or not isinstance(
        transaction["quantity"], int
    ):
        errors.append("quantity must be an integer")
    elif transaction["quantity"] <= 0:
        errors.append("quantity must be greater than 0")

    # --------------------------------------------------
    # 7. Validate price
    # --------------------------------------------------

    if isinstance(transaction["price"], bool) or not isinstance(
        transaction["price"], (int, float)
    ):
        errors.append("price must be a number")
    elif transaction["price"] < 0:
        errors.append("price cannot be negative")

    # --------------------------------------------------
    # 8. Validate tax_amount
    # --------------------------------------------------

    if isinstance(transaction["tax_amount"], bool) or not isinstance(
        transaction["tax_amount"], (int, float)
    ):
        errors.append("tax_amount must be a number")
    elif transaction["tax_amount"] < 0:
        errors.append("tax_amount cannot be negative")

    # --------------------------------------------------
    # 9. Validate payment method
    # --------------------------------------------------

    if transaction["payment_method"] not in ALLOWED_PAYMENT_METHODS:
        errors.append(
            f"Invalid payment_method: {transaction['payment_method']}"
        )

    # --------------------------------------------------
    # 10. Validate timestamp
    # --------------------------------------------------

    if not isinstance(transaction["timestamp"], str):
        errors.append("timestamp must be a string")
    else:
        try:
            datetime.fromisoformat(transaction["timestamp"])
        except ValueError:
            errors.append(
                "timestamp must be a valid ISO 8601 datetime"
            )

    # --------------------------------------------------
    # Final result
    # --------------------------------------------------

    if errors:
        return False, errors

    return True, []