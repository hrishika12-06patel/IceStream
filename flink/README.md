# IceStream Flink Transaction Processor

## Purpose

The **Flink Transaction Processor** is the stream processing component of IceStream. It consumes raw e-commerce transaction events from Apache Kafka, parses JSON payloads, validates transaction fields, calculates the total transaction amount (`total_amount`), and emits processed transactions to the stream output.

## Architecture

```
Transaction Generator
        ↓
      Kafka
        ↓
      Flink
        ↓
Processed Transaction Stream
```

## Input Schema

The incoming transaction JSON messages contain the following 8 required fields:

| Field | Type | Description | Example |
|---|---|---|---|
| `order_id` | Integer / String | Unique order identifier | `10001` or `"ORD001"` |
| `customer_id` | String | Unique customer identifier | `"C123"` |
| `product_id` | String | Unique product identifier | `"P501"` |
| `quantity` | Integer | Units purchased (`> 0`) | `2` |
| `price` | Float / Number | Unit price (`>= 0.0`) | `1000.00` |
| `tax_amount` | Float / Number | Tax amount (`>= 0.0`) | `180.00` |
| `payment_method` | String | Payment method used | `"UPI"` |
| `timestamp` | String | ISO 8601 UTC timestamp | `"2026-08-24T10:00:00+00:00"` |

## Processing Pipeline

1. **JSON Parsing**: Raw Kafka byte/string payloads are parsed as JSON. Malformed payloads log a warning and are cleanly filtered out without crashing the job.
2. **Validation**: Each transaction is verified for mandatory field presence, non-null values, correct types, and numeric constraints (`quantity > 0`, `price >= 0`, `tax_amount >= 0`). Invalid transactions are rejected safely.
3. **Total Amount Calculation**: Valid transactions are enriched with a calculated field:
   $$\text{total\_amount} = (\text{price} \times \text{quantity}) + \text{tax\_amount}$$
4. **Output Stream**: Processed records with original fields + `total_amount` are routed to the output sink (printed/logged for Day 8).

### Example Output

**Input**:
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

**Processed Output**:
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

## Configuration

Environment variables control Kafka broker endpoints and target topics:

| Environment Variable | Default Value (Local) | Docker Container Value | Description |
|---|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | `kafka:9092` | Kafka broker endpoint(s) |
| `KAFKA_TOPIC` | `transactions` | `transactions` | Kafka input topic |

## Running the Processor

### 1. Local Execution

To run the job locally outside Docker (requires PyFlink and dependencies installed):

```bash
# Set environment variables (or rely on .env / defaults)
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export KAFKA_TOPIC="transactions"

# Execute transaction processor
python flink/transaction_processor.py
```

### 2. Docker Container Execution

When running inside the Docker Compose network:

```bash
# Start Kafka and Flink cluster infrastructure
docker compose up -d

# Submit Flink job to JobManager container
docker compose exec jobmanager flink run -py /opt/flink/flink/transaction_processor.py
```

## Testing

Unit tests for JSON parsing, schema validation, and total amount calculation can be run offline without requiring a running Kafka cluster or Flink environment:

```bash
python -m pytest tests/test_transaction_processor.py -v
```
