# IceStream Data-Quality Rules Reference

## 1. Required Fields & Schema Mapping
This section maps the official IceStream schema fields derived directly from the transaction dataset.

| Field Name | Data Type | Constraint | Description |
| :--- | :--- | :--- | :--- |
| **`order_id`** | INTEGER | `PRIMARY KEY`, `NOT NULL` | Unique identifier for the transaction order. |
| **`customer_id`** | VARCHAR | `NOT NULL` | Unique identifier of the purchasing customer. |
| **`product_id`** | VARCHAR | `NOT NULL` | Unique identifier of the product purchased. |
| **`quantity`** | INTEGER | `NOT NULL` | Total units of the product purchased. Must be > 0. |
| **`price`** | DECIMAL | `NOT NULL` | Individual unit price of the item. Must be >= 0. |
| **`tax_amount`** | DECIMAL | `NOT NULL` | Calculated tax applied to the order. Must be >= 0. |
| **`payment_method`** | VARCHAR | `NOT NULL` | Method used to complete payment. Restricted to allowed set. |
| **`timestamp`** | TIMESTAMP | `NOT NULL` | ISO 8601 formatted date-time string of the event. |

---

## 2. Validation Rules & Implementation

### DDL Schema Constraints
```sql
CREATE TABLE icestream_transactions (
    order_id INT PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    price DECIMAL(10,2) NOT NULL CHECK (price >= 0),
    tax_amount DECIMAL(10,2) NOT NULL CHECK (tax_amount >= 0),
    payment_method VARCHAR(50) NOT NULL CHECK (payment_method IN ('Credit Card', 'UPI', 'Debit Card', 'Net Banking')),
    timestamp TIMESTAMP NOT NULL
);
```

### Pipeline Audit Script
Run this diagnostic query against your staging table to isolate invalid transactional data and flag specific rule breaks.
```sql
SELECT 
    order_id,
    customer_id,
    CASE 
        WHEN order_id IS NULL THEN 'MISSING_ORDER_ID'
        WHEN customer_id IS NULL THEN 'MISSING_CUSTOMER_ID'
        WHEN quantity <= 0 THEN 'INVALID_QUANTITY_VALUE'
        WHEN price < 0 THEN 'NEGATIVE_PRICE_VALUE'
        WHEN tax_amount < 0 THEN 'NEGATIVE_TAX_VALUE'
        WHEN payment_method NOT IN ('Credit Card', 'UPI', 'Debit Card', 'Net Banking') THEN 'UNSUPPORTED_PAYMENT_METHOD'
        WHEN timestamp IS NULL THEN 'MISSING_TIMESTAMP'
        ELSE 'PASSED'
    END AS validation_status
FROM staging_icestream_transactions;
```

---

## 3. Dimensions of Data-Quality Problems

*   **Completeness:** Data contains all required fields. Failures happen when optional attributes or critical elements (like `customer_id` or `timestamp`) arrive as `NULL` or empty strings.
*   **Validity:** Values follow specified structural rules and lookup configurations. For example, a `payment_method` set to `"Crypto"` fails validity because it sits outside the accepted list of payment formats.
*   **Uniqueness:** Records must not contain duplicate identifying keys. A failure occurs if two distinct transaction payloads attempt to write into the streaming engine with an identical `order_id`.
*   **Data Type:** Raw fields must match predefined field layouts. A failure occurs if an alpha-numeric alphanumeric string sequence (e.g., `"abc"`) is submitted into the numeric `quantity` or `price` metrics.
*   **Range Validation:** Numerical quantities must fall within logical boundaries. A failure occurs if fields tracking physical variables yield impossible negative evaluations (e.g., `price = -850.00`).
*   **Schema Consistency:** Payloads must structurally match the master template blueprint. A structural failure triggers if an event contains unexpected schema modifications, missing core properties, or misaligned structural fields.

---

## 4. Transaction Code Examples

### Valid Transaction
This record matches all schema properties, data types, value boundaries, and accepted payment constraints.
```json
{
  "order_id": 10001,
  "customer_id": "C101",
  "product_id": "P501",
  "quantity": 2,
  "price": 850.00,
  "tax_amount": 153.00,
  "payment_method": "Credit Card",
  "timestamp": "2026-08-15T10:00:01"
}
```

### Invalid Transaction
This payload triggers several data-quality anomalies during validation checks.
```json
{
  "order_id": null,
  "customer_id": "C102",
  "product_id": "P502",
  "quantity": -1,
  "price": 1200.00,
  "tax_amount": 216.00,
  "payment_method": "Bitcoin",
  "timestamp": "2026-08-15T10:00:02"
}
```

#### Why it is Invalid:
1.  **Completeness/Uniqueness Failure:** The primary index attribute `order_id` is passed as `null`. It must contain a distinct, non-null integer.
2.  **Range Validation Failure:** The `quantity` property contains a negative value (`-1`), which is a physical impossibility for a purchase event.
3.  **Validity Failure:** The `payment_method` is set to `"Bitcoin"`, which violates the accepted value whitelist check constraint.