# IceStream Transaction Schema

## Overview

IceStream uses synthetic e-commerce transaction data to simulate
a real-time transaction stream.

All components of the project should follow this common
transaction schema.

## Transaction Fields

| Field | Data Type | Required | Description |
|---|---|---|---|
| order_id | Integer | Yes | Unique identifier for each order |
| customer_id | String | Yes | Identifier of the customer |
| product_id | String | Yes | Identifier of the purchased product |
| quantity | Integer | Yes | Number of units purchased |
| price | Float | Yes | Price of one unit of the product |
| tax_amount | Float | Yes | Tax amount associated with the transaction |
| payment_method | String | Yes | Payment method used for the transaction |
| timestamp | ISO 8601 String | Yes | Time at which the transaction was generated |

## Example Transaction

```json
{
  "order_id": 10001,
  "customer_id": "C101",
  "product_id": "P501",
  "quantity": 2,
  "price": 850.00,
  "tax_amount": 306.00,
  "payment_method": "Credit Card",
  "timestamp": "2026-08-18T10:00:01Z"
}