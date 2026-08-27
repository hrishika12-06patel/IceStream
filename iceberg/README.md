# Apache Iceberg Storage Layer for IceStream

## Purpose

The **Apache Iceberg Storage Layer** serves as the lakehouse data store in the IceStream pipeline. It receives validated, enriched transaction records from the stream processor (Apache Flink) and stores them in open table format with ACID transactional guarantees, time travel capabilities, schema evolution, and efficient analytical querying support.

## Architecture

```text
Transaction Generator
        ↓
  Kafka Topic (`transactions`)
        ↓
  PyFlink Transaction Processor (Parsing & Validation)
        ↓
  Enriched Transactions (`total_amount`)
        ↓
   Apache Iceberg Storage Layer (`icestream.transactions`)
```

## Configuration

Iceberg storage behavior is managed via environment variables defined in `.env`:

| Environment Variable | Default (Local) | Description |
|---|---|---|
| `ICEBERG_CATALOG` | `local` | Iceberg catalog implementation (e.g. `local`, `sql`, `hadoop`) |
| `ICEBERG_WAREHOUSE` | `./warehouse` | Target storage path / URI for Iceberg data & metadata files |
| `ICEBERG_NAMESPACE` | `icestream` | Database or namespace identifier |
| `ICEBERG_TABLE` | `transactions` | Target Iceberg table name |

### Upstream Kafka & Flink Configuration

| Environment Variable | Default (Local) | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker endpoint |
| `KAFKA_TOPIC` | `transactions` | Source topic consumed by Flink |

---

## Table Schema

The logical Iceberg transaction table (`icestream.transactions`) stores 9 fields:

| Field ID | Field Name | Data Type | Nullable | Description |
|---|---|---|---|---|
| `1` | `order_id` | `StringType` | No | Unique order identifier (string or stringified integer) |
| `2` | `customer_id` | `StringType` | No | Customer identifier |
| `3` | `product_id` | `StringType` | No | Product identifier |
| `4` | `quantity` | `IntegerType` | No | Quantity of product purchased (`> 0`) |
| `5` | `price` | `DecimalType(10, 2)` | No | Unit price of product (`>= 0.00`) |
| `6` | `tax_amount` | `DecimalType(10, 2)` | No | Tax amount for order (`>= 0.00`) |
| `7` | `payment_method` | `StringType` | No | Payment method (e.g., Credit Card, UPI) |
| `8` | `timestamp` | `TimestamptzType` | No | ISO 8601 UTC transaction timestamp |
| `9` | `total_amount` | `DecimalType(12, 2)` | No | Flink-calculated total order amount: $(price \times quantity) + tax\_amount$ |

---

## Catalog & Warehouse

- **Catalog Type**: Local filesystem catalog (`ICEBERG_CATALOG=local`), using SQLite metadata or direct filesystem tracking suitable for local development.
- **Warehouse Location**: `./warehouse` directory in the repository root (or configured absolute path). Data files (Parquet/ORC) and metadata manifests are organized under `icestream/transactions/`.

---

## Flink → Iceberg Data Mapping

1. **Filtering**: Invalid transactions (malformed JSON, missing required fields, or illegal values) are filtered out by Flink before downstream routing.
2. **Enrichment**: Valid records are enriched with `total_amount`.
3. **Type Casting & Precision**:
   - `order_id` $\rightarrow$ String
   - `price`, `tax_amount`, `total_amount` $\rightarrow$ `Decimal` with 2 decimal place precision (avoids floating-point imprecision)
   - `timestamp` $\rightarrow$ ISO 8601 string / Timestamptz format

---

## Dependencies

### Python Dependencies
- `pyiceberg==0.11.1`: Python library for Iceberg table catalog management, schema definition, and table inspection.

### Flink Runtime Dependencies (Cluster Deployment)
To execute Flink-to-Iceberg streaming sinks in a containerized Flink cluster, the Flink jobmanager/taskmanager classpath requires:
- `iceberg-flink-runtime-1.18` (or matching Flink version connector JAR placed in Flink `/lib` directory).

---

## Testing

### Unit Tests
Run the dedicated Iceberg configuration & schema test suite:
```bash
python -m pytest tests/test_iceberg_config.py -v
```

### Flink Regression Tests
Verify stream processing validation and calculation logic remain fully operational:
```bash
python -m pytest tests/test_transaction_processor.py -v
```

### Full Repository Test Suite
```bash
python -m pytest -v
```

---

## Integration Status

- [x] **Iceberg Configuration**: Verified via `IcebergConfig` unit tests.
- [x] **Iceberg Schema Definition**: Verified via PyIceberg schema inspection & field type validation.
- [x] **Data Mapping**: Verified via `map_transaction_to_iceberg_record()` tests.
- [x] **Offline Flink Logic**: Verified via 31 transaction processor unit tests.
- [ ] **Live Flink → Iceberg Container Sink**: Skipped containerized live streaming sink test due to local environment missing Docker installation (`docker: command not found`).
