# Apache Iceberg Storage Layer for IceStream

## Purpose

The **Apache Iceberg Storage Layer** serves as the open lakehouse data store in the IceStream real-time streaming pipeline. It consumes validated and enriched transaction records from Apache Flink, applying schema enforcement, ACID transactional commits, time travel capabilities, and efficient parquet data storage.

---

## Architecture

```text
Transaction Generator
        ↓
      Kafka Topic (`transactions`)
        ↓
  PyFlink Transaction Processor (JSON Parsing, Schema Validation & total_amount Calculation)
        ↓
  Apache Iceberg Connector (`iceberg-flink-runtime-1.18`)
        ↓
  Iceberg Catalog (`local` SQLite Catalog)
        ↓
  Iceberg Warehouse (`./warehouse`)
        ↓
  Iceberg Table (`icestream.transactions`)
```

---

## Prerequisites

- **Python**: 3.10+ (with `pyiceberg==0.11.1` installed)
- **Java**: Java 11 / 17 (for Flink runtime)
- **Docker & Docker Compose**: Optional. Required for containerized live integration execution. If Docker is unavailable, the integration test suite falls back cleanly to offline logic verification.

---

## Service Architecture & Docker Setup

| Container / Service | Image | Ports | Role |
|---|---|---|---|
| `icestream-kafka` | `apache/kafka:4.0.0` | `9092` | Stream message broker |
| `icestream-jobmanager` | `flink:2.3.0-scala_2.12` | `8081` | Flink master & job coordinator |
| `icestream-taskmanager` | `flink:2.3.0-scala_2.12` | `6122` | Flink worker processing slots |

### Docker Startup Instructions

```bash
# Start infrastructure containers
docker compose up -d

# Verify container status
docker compose ps
```

---

## Configuration

Iceberg storage configuration is managed via environment variables defined in `.env`:

| Environment Variable | Default (Local) | Description |
|---|---|---|
| `ICEBERG_CATALOG` | `local` | Iceberg catalog name / implementation type |
| `ICEBERG_WAREHOUSE` | `./warehouse` | Relative or absolute path for Iceberg data & metadata storage |
| `ICEBERG_NAMESPACE` | `icestream` | Iceberg namespace / database name |
| `ICEBERG_TABLE` | `transactions` | Target Iceberg table name |

**Full Table Identifier**: `icestream.transactions`

---

## Flink & Iceberg Connector Version Compatibility

- **Flink Version**: `2.3.0` (compatible with Flink `1.18.x` / `1.19.x` Stream & Table APIs)
- **Iceberg Version**: `1.5.0` (PyIceberg `0.11.1` client)
- **Apache Iceberg Connector Artifact**: `org.apache.iceberg:iceberg-flink-runtime-1.18:1.5.0`
- **Compatibility Rationale**: The `iceberg-flink-runtime-1.18` connector artifact packages Apache Iceberg's Table API source and sink operators, catalog serialization, and Parquet writer dependencies tailored for Flink's runtime environment. Mount `./flink/lib` to `/opt/flink/usrlib` to make the JAR available on Flink container classpaths.

---

## Table Schema

The Iceberg table (`icestream.transactions`) defines 9 structured fields:

| Field ID | Field Name | Data Type | Nullable | Description |
|---|---|---|---|---|
| `1` | `order_id` | `StringType` | No | Unique order identifier |
| `2` | `customer_id` | `StringType` | No | Customer identifier |
| `3` | `product_id` | `StringType` | No | Product identifier |
| `4` | `quantity` | `IntegerType` | No | Purchased item quantity (`> 0`) |
| `5` | `price` | `DecimalType(10, 2)` | No | Unit product price (`>= 0.00`) |
| `6` | `tax_amount` | `DecimalType(10, 2)` | No | Order tax amount (`>= 0.00`) |
| `7` | `payment_method` | `StringType` | No | Payment method (e.g. UPI, Credit Card) |
| `8` | `timestamp` | `TimestamptzType` | No | Transaction ISO 8601 UTC timestamp |
| `9` | `total_amount` | `DecimalType(12, 2)` | No | Flink-calculated total order amount: $(price \times quantity) + tax\_amount$ |

---

## How the Flink Iceberg Sink Works

1. **Stream Consumption**: `KafkaSource` consumes JSON transaction payloads from the `transactions` topic.
2. **Parsing & Validation**: `TransactionProcessMapFunction` parses JSON and validates schema requirements (non-empty strings, positive quantities, non-negative prices).
3. **Calculation & Enrichment**: Valid records compute `total_amount = (price * quantity) + tax_amount`.
4. **Metrics Tracking**: `ProcessingMetrics` records total received, valid, invalid, and processing error counts. Invalid records are rejected and never written to Iceberg.
5. **Iceberg Sink**: `write_record_to_iceberg()` maps processed records via `map_transaction_to_iceberg_record()` into fixed decimal and UTC timestamp types, writing them to `icestream.transactions`.

---

## Running the Live Pipeline

```bash
# 1. Start Docker containers (if Docker is available)
docker compose up -d

# 2. Start streaming transaction generator
python generator/stream_transactions.py

# 3. Start Flink transaction processor with Iceberg sink
python flink/transaction_processor.py
```

---

## Verifying Records in Iceberg

You can inspect metadata files and query records in `icestream.transactions` using PyIceberg:

```python
from pyiceberg.catalog.sql import SqlCatalog
from iceberg.iceberg_config import IcebergConfig
import os

cfg = IcebergConfig.from_env()
db_path = os.path.abspath(os.path.join(cfg.warehouse, "catalog.db"))
catalog = SqlCatalog(cfg.catalog, **{"uri": f"sqlite:///{db_path}", "warehouse": os.path.abspath(cfg.warehouse)})

# Load table
table = catalog.load_table("icestream.transactions")
print("Table metadata location:", table.metadata_location)
print("Table schema:", table.schema())
```

---

## Testing

### Flink → Iceberg Integration Test Suite
```bash
python -m pytest tests/test_flink_iceberg_integration.py -v
```

### Full Repository Regression Tests
```bash
python -m pytest -v
```

---

## Troubleshooting

1. **Docker is Unavailable**:
   If Docker is not running or installed (`docker: command not found`), the integration test suite automatically skips containerized execution and runs offline verification tests.
2. **Iceberg Connector JAR Missing in Container**:
   Download `iceberg-flink-runtime-1.18-1.5.0.jar` into `./flink/lib/`. The mounted volume `./flink/lib:/opt/flink/usrlib` will expose it to Flink JobManager and TaskManager.
3. **Decimal Precision Mismatches**:
   Monetary fields (`price`, `tax_amount`, `total_amount`) use Python `Decimal` formatted to 2 decimal places (`Decimal("0.01")`). Avoid floating-point arithmetic when writing to Iceberg tables.
