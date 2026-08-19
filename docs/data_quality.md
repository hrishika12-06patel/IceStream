# IceStream Data Quality Validation

## Purpose

The IceStream validation layer checks incoming e-commerce transactions before they are processed by the streaming pipeline.

The validator ensures that transactions follow the expected transaction structure and identifies invalid or incomplete data.

## Required Fields

Every transaction must contain the following fields:

- `order_id`
- `customer_id`
- `product_id`
- `quantity`
- `price`
- `tax_amount`
- `payment_method`
- `timestamp`

## Validation Rules

### order_id

- Must be present.
- NULL values are not allowed.
- Must be a non-empty string.

### customer_id

- Must be present.
- NULL values are not allowed.
- Must be a non-empty string.

### product_id

- Must be present.
- NULL values are not allowed.
- Must be a non-empty string.

### quantity

- Must be present.
- Must be an integer.
- Must be greater than 0.
- NULL values are not allowed.

### price

- Must be present.
- Must be a number.
- Cannot be negative.
- NULL values are not allowed.

### tax_amount

- Must be present.
- Must be a number.
- Cannot be negative.
- NULL values are not allowed.

### payment_method

Allowed payment methods are:

- UPI
- Credit Card
- Debit Card
- Cash
- Net Banking
- Wallet

Any other payment method is considered invalid.

### timestamp

- Must be present.
- Must be a string.
- Must contain a valid ISO 8601 datetime.
- NULL values are not allowed.

## Invalid Data Handling

When a transaction violates one or more validation rules, the validator returns:

1. A `False` validation status.
2. A list containing the detected validation errors.

Valid transactions return:

1. A `True` validation status.
2. An empty error list.

## Testing

Automated tests are located in:

`tests/test_transaction_validation.py`

Run the tests using:

```bash
pytest