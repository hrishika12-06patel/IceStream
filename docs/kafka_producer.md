# Kafka Producer Module

## Purpose

The Kafka Producer module (`generator/kafka_producer.py`) is responsible for publishing e-commerce transaction dictionaries to an Apache Kafka topic. It handles serialization to JSON, UTF-8 encoding, topic/broker configuration, and error management cleanly without exposing sensitive environment data.

---

## Configuration

Kafka settings are externalized to environment variables and loaded dynamically (with optional `.env` file support via `python-dotenv`).

| Environment Variable | Description | Example |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker connection string | `localhost:9092` |
| `KAFKA_TOPIC` | Kafka topic for transaction messages | `transactions` |

### Environment Setup

Create a `.env` file in the project root based on `.env.example`:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=transactions
```

---

## Expected Transaction Format

The producer expects transaction input as a Python `dict`:

```json
{
    "order_id": 10001,
    "customer_id": "C123",
    "product_id": "P501",
    "quantity": 2,
    "price": 1200.50,
    "tax_amount": 216.09,
    "payment_method": "UPI",
    "timestamp": "2026-08-22T12:00:00+00:00"
}
```

This is serialized into a UTF-8 encoded JSON string byte payload before being published to Kafka.

---

## Usage Examples

### 1. Python API Usage

```python
from generator.kafka_producer import KafkaProducerService

transaction = {
    "order_id": 10001,
    "customer_id": "C123",
    "product_id": "P501",
    "quantity": 2,
    "price": 1200.50,
    "tax_amount": 216.09,
    "payment_method": "UPI",
    "timestamp": "2026-08-22T12:00:00+00:00"
}

# Context manager handles closing producer cleanly
with KafkaProducerService() as service:
    service.send_transaction(transaction)
    service.flush()
```

### 2. Command Line Interface (CLI)

Validate configuration and test JSON serialization without connecting to Kafka:

```bash
python generator/kafka_producer.py --dry-run --bootstrap-servers localhost:9092 --topic transactions
```

---

## Error Handling

The module provides clear, custom exception classes under `KafkaProducerError`:

- `KafkaConfigError`: Missing broker (`KAFKA_BOOTSTRAP_SERVERS`) or topic (`KAFKA_TOPIC`).
- `TransactionValidationError`: Input is not a dictionary.
- `SerializationError`: JSON serialization failure.
- `KafkaPublishError`: Kafka connection, initialization, or message delivery failure.

> [!IMPORTANT]
> Error messages and logs are sanitized and never log sensitive passwords, credentials, or raw environment dictionaries.

---

## Testing Without Kafka

Unit tests use mock objects to verify producer functionality, configuration validation, JSON serialization, and error handling without requiring a running Kafka broker:

```bash
pytest tests/test_kafka_producer.py
```

---

## Optional Local Kafka Testing

To perform live integration testing with a real Kafka broker:

1. Start the local Kafka container:
   ```bash
   docker compose up -d
   ```
2. Run the producer CLI without `--dry-run`:
   ```bash
   python generator/kafka_producer.py --bootstrap-servers localhost:9092 --topic transactions
   ```
