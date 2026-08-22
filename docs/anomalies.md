# IceStream Controlled Anomaly Injection

## Purpose

The IceStream Anomaly Generator (`generator/anomaly_generator.py`) produces synthetic e-commerce transactions deliberately injected with controlled data-quality issues. 

In production streaming systems, data quality anomalies (such as missing attributes, corrupted schemas, invalid field values, or duplicate records) frequently occur due to network retries, upstream service bugs, or software updates. The anomaly generator allows testing data quality validation pipelines, error handling, dead-letter queues, and monitoring frameworks under controlled conditions.

> [!NOTE]
> Kafka integration is **not** included in this task or module.

## Supported Anomaly Types

| Anomaly Type | CLI Value | Description & Modification |
|---|---|---|
| **Null Value** | `null` | Deliberately sets one or more normally required fields (`customer_id`, `product_id`, `payment_method`, `price`, `quantity`, `tax_amount`, `timestamp`) to `null` (`None`). |
| **Schema Change** | `schema` | Modifies the record structure: renames expected fields (e.g. `customer_id` &rarr; `customer`), adds unexpected extra fields (`unexpected_field`), drops required fields, or changes field data types (e.g. string price). |
| **Invalid Numeric** | `negative-price`, `invalid-numeric` | Injects invalid numerical values such as negative prices (`price < 0`), negative quantities (`quantity < 0`), or negative tax amounts. |
| **Invalid Enum** | `invalid-enum` | Replaces expected categorical fields with invalid choices (e.g., `payment_method = "UnknownMethod"` or `product_id = "P999_INVALID"`). |
| **Duplicate Order ID** | `duplicate-id` | Deliberately reuses an existing `order_id` from a previously generated transaction to simulate duplicate records. |
| **All Anomalies** | `all` | Randomly samples and injects any of the supported controlled anomaly types for each anomalous transaction. |

## CLI Options

| Argument | Short | Type | Default | Description |
|---|---|---|---|---|
| `--anomaly` | `-a` | String | **Required** | Type of anomaly to inject (`null`, `schema`, `negative-price`, `invalid-numeric`, `invalid-enum`, `duplicate-id`, `all`). |
| `--count` | `-c` | Integer | `1000` | Total number of transactions to generate. |
| `--anomaly-rate` | `-r` | Float | `0.05` | Fraction of transactions receiving anomalies (e.g. `0.05` = 5%). |
| `--anomaly-count` | N/A | Integer | `None` | Explicit count of anomalous transactions. Overrides `--anomaly-rate` if set. |
| `--seed` | `-s` | Integer | `None` | Optional random seed for reproducible testing. |
| `--start-order-id` | N/A | Integer | `10001` | Starting integer for order IDs. |
| `--verbose` | `-v` | Flag | `False` | Prints generated transactions as formatted JSON. |

## Anomaly Rate, Count, & Reproducibility Behavior

- **Anomaly Rate (`--anomaly-rate`)**: Specifies the probability (between `0.0` and `1.0`) of corrupting any individual transaction. For example, `--anomaly-rate 0.05` corrupts approximately 5% of generated records while preserving ~95% valid transactions.
- **Anomaly Count (`--anomaly-count`)**: When set, exactly N transactions out of `--count` are selected for anomaly injection, overriding rate-based probability.
- **Reproducibility (`--seed`)**: When `--seed <int>` is supplied, pseudo-random selections (such as index selection and corrupt field choice) are deterministic. Running the same command with the same seed yields identical corrupted output.
- **Valid Data Preservation**: Unless a transaction is specifically selected for corruption, it is generated using standard clean rules from `generator/transaction_generator.py`.

## Expected Schemas & Examples

### Normal Expected Schema

```json
{
  "order_id": 10001,
  "customer_id": "C482",
  "product_id": "P503",
  "quantity": 3,
  "price": 450.50,
  "tax_amount": 243.27,
  "payment_method": "UPI",
  "timestamp": "2026-08-21T20:30:00+00:00"
}
```

### Sample Anomalous Transactions

#### 1. Null Value Anomaly (`--anomaly null`)
```json
{
  "order_id": 10002,
  "customer_id": null,
  "product_id": "P503",
  "quantity": 3,
  "price": 450.50,
  "tax_amount": 243.27,
  "payment_method": "UPI",
  "timestamp": "2026-08-21T20:30:00+00:00"
}
```

#### 2. Schema Anomaly (`--anomaly schema`)
```json
{
  "order_id": 10003,
  "customer": "C109",
  "product_id": "P501",
  "quantity": 1,
  "price": "1200.0",
  "tax_amount": 216.0,
  "payment_method": "Credit Card",
  "unexpected_field": "extra_unstructured_data"
}
```

#### 3. Negative Price Anomaly (`--anomaly negative-price`)
```json
{
  "order_id": 10004,
  "customer_id": "C221",
  "product_id": "P508",
  "quantity": 2,
  "price": -350.00,
  "tax_amount": 126.00,
  "payment_method": "Net Banking",
  "timestamp": "2026-08-21T20:30:00+00:00"
}
```

#### 4. Invalid Enum Anomaly (`--anomaly invalid-enum`)
```json
{
  "order_id": 10005,
  "customer_id": "C310",
  "product_id": "P502",
  "quantity": 1,
  "price": 299.99,
  "tax_amount": 54.00,
  "payment_method": "UnknownMethod",
  "timestamp": "2026-08-21T20:30:00+00:00"
}
```

#### 5. Duplicate Order ID Anomaly (`--anomaly duplicate-id`)
```json
{
  "order_id": 10001,
  "customer_id": "C901",
  "product_id": "P505",
  "quantity": 4,
  "price": 199.00,
  "tax_amount": 143.28,
  "payment_method": "Debit Card",
  "timestamp": "2026-08-21T20:30:05+00:00"
}
```

## Sample Commands

```bash
# Inject 5% null values into 1,000 transactions
python generator/anomaly_generator.py --anomaly null --count 1000 --anomaly-rate 0.05

# Inject schema anomalies into 5% of 1,000 transactions
python generator/anomaly_generator.py --anomaly schema --count 1000 --anomaly-rate 0.05

# Inject negative prices into 2% of 1,000 transactions
python generator/anomaly_generator.py --anomaly negative-price --count 1000 --anomaly-rate 0.02

# Inject duplicate order IDs into 1% of 1,000 transactions
python generator/anomaly_generator.py --anomaly duplicate-id --count 1000 --anomaly-rate 0.01

# Inject all anomaly types into 5% of 1,000 transactions with a fixed seed for testing
python generator/anomaly_generator.py --anomaly all --count 1000 --anomaly-rate 0.05 --seed 42
```

## Output & Summary Example

```text
Generated transactions: 1,000
Anomalous transactions: 52
Anomaly type: all
Anomaly rate: 5.2% (target: 5.0%)
Anomaly type breakdown:
  - null: 11
  - schema: 12
  - negative-price: 8
  - invalid-enum: 11
  - duplicate-id: 10
Random seed: 42
```

## Limitations

- Anomalies are generated in memory and returned as Python dictionary representations or stdout JSON text.
- No direct database writes, stream publishing, or network sockets are established by default.
- Kafka producer/consumer integration is explicitly **out of scope**.
