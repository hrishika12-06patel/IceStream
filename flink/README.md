# IceStream Flink Transaction Processor

## Overview

The **Flink Transaction Processor** is the stream processing core of IceStream. It consumes real-time e-commerce transaction events from Apache Kafka, parses JSON payloads, validates schemas and business rules, calculates `total_amount`, tracks stream processing metrics, and routes processed transactions downstream.

## Architecture

```
Transaction Generator
        ↓
  Kafka Topic (`transactions`)
        ↓
   PyFlink Kafka Source
        ↓
  JSON Parsing & Schema Validation
        ↓
  Total Amount Calculation (`total_amount`)
        ↓
Processed Output / Metrics Counter
```

## Input Schema

The incoming transaction JSON messages contain the following 8 required fields:

| Field | Type | Constraint / Validation | Example |
|---|---|---|---|
| `order_id` | Integer / String | Non-negative integer or non-empty string | `10001` or `"ORD001"` |
| `customer_id` | String | Non-empty string | `"C123"` |
| `product_id` | String | Non-empty string | `"P501"` |
| `quantity` | Integer | Integer `> 0` (boolean disallowed) | `2` |
| `price` | Float / Number | Numeric `>= 0.0` | `1000.00` |
| `tax_amount` | Float / Number | Numeric `>= 0.0` | `180.00` |
| `payment_method` | String | Non-empty string | `"UPI"` |
| `timestamp` | String | ISO 8601 UTC timestamp string | `"2026-08-24T10:00:00+00:00"` |

## Processing & Validation Pipeline

1. **JSON Parsing**: Raw Kafka byte or string messages are safely parsed into Python dictionaries. Malformed JSON payloads log a warning, increment the `invalid_records` metric, and are filtered out without crashing the stream job.
2. **Validation**: Each transaction payload is checked against schema requirements:
   - Mandatory presence of all 8 fields.
   - Non-NULL checks.
   - Type and range validations (`quantity > 0`, `price >= 0`, `tax_amount >= 0`).
   - ISO 8601 timestamp string format validation.
   Invalid messages are rejected cleanly and counted in `invalid_records`.
3. **Total Amount Calculation**: Valid records are enriched with `total_amount`:
   $$\text{total\_amount} = \text{round}((\text{price} \times \text{quantity}) + \text{tax\_amount}, 2)$$
4. **Processed Output**: Valid enriched transactions are output as JSON strings.

### Example

**Input Event**:
```json
{
  "order_id": 10001,
  "customer_id": "C123",
  "product_id": "P501",
  "quantity": 2,
  "price": 1000.00,
  "tax_amount": 180.00,
  "payment_method": "UPI",
  "timestamp": "2026-08-24T10:00:00+00:00"
}
```

**Processed Event**:
```json
{
  "order_id": 10001,
  "customer_id": "C123",
  "product_id": "P501",
  "quantity": 2,
  "price": 1000.00,
  "tax_amount": 180.00,
  "payment_method": "UPI",
  "timestamp": "2026-08-24T10:00:00+00:00",
  "total_amount": 2180.00
}
```

## Stream Processing Metrics

The Flink job tracks stream performance and health using the following metrics:

| Metric Name | Type | Description |
|---|---|---|
| `total_records_received` | Counter | Total raw messages consumed from Kafka. |
| `valid_records_processed` | Counter | Total messages successfully validated and processed. |
| `invalid_records` | Counter | Messages rejected due to malformed JSON, missing fields, or invalid values. |
| `processing_errors` | Counter | Internal exceptions during processing/computation. |
| `records_per_second` | Gauge | Calculation rate of records processed per second. |

In a Flink cluster environment, these metrics integrate directly into Flink's native MetricGroup (`runtime_context.get_metrics_group()`). In standalone/testing environments, `ProcessingMetrics` tracks these values in memory.

## Configuration

Kafka endpoints and topic names are controlled via environment variables:

| Environment Variable | Default (Local) | Docker Container Value | Description |
|---|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | `kafka:9092` | Kafka broker endpoint |
| `KAFKA_TOPIC` | `transactions` | `transactions` | Target Kafka transaction topic |

## Execution

### 1. Local / Standalone Execution
```bash
# Set environment variables (optional, falls back to defaults)
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export KAFKA_TOPIC="transactions"

# Run transaction processor
python flink/transaction_processor.py
```

### 2. Docker Container Execution
```bash
# Spin up Kafka and Flink cluster
docker compose up -d

# Submit PyFlink job to Flink JobManager
docker compose exec jobmanager flink run -py /opt/flink/flink/transaction_processor.py
```

## Testing

### Unit Tests & Offline Pipeline Verification
Runs 31 comprehensive unit tests covering JSON parsing, mandatory schema validation, invalid numerical handling, error recovery, metric updates, and anomaly payload rejection:
```bash
python -m pytest tests/test_transaction_processor.py -v
```

### Full Repository Regression Test
```bash
python -m pytest -v
```

## Environment & Integration Test Limitations

> [!NOTE]
> **Docker Integration Test Status**: Docker is not installed on the local test runner environment (`docker: command not found`). Live end-to-end integration testing against a containerized Kafka broker and Flink cluster was skipped due to missing container infrastructure. Offline unit tests verified all parsing, validation, metric tracking, calculation, and anomaly handling logic.
