# Continuous Transaction Streaming Pipeline

## Purpose

The Transaction Streaming module (`generator/stream_transactions.py`) orchestrates continuous streaming of generated e-commerce transactions directly into an Apache Kafka topic. It connects the transaction generator (`generator/transaction_generator.py`) with the Kafka producer service (`generator/kafka_producer.py`).

---

## Pipeline Architecture

```
+---------------------------------+
| generator/transaction_generator |
+---------------------------------+
                |
                v  (generate_transaction)
+---------------------------------+
|  generator/stream_transactions  |  <-- Orchestration Layer (Rate limiter, Order ID tracker)
+---------------------------------+
                |
                v  (send_transaction)
+---------------------------------+
|     generator/kafka_producer    |
+---------------------------------+
                |
                v  (UTF-8 JSON bytes)
+---------------------------------+
|           Apache Kafka          |
+---------------------------------+
```

---

## Configuration

Kafka settings are retrieved from environment variables or overridden via CLI flags.

| Parameter | Environment Variable | CLI Flag | Default | Description |
|---|---|---|---|---|
| Bootstrap Servers | `KAFKA_BOOTSTRAP_SERVERS` | `--bootstrap-servers` | Environment / `.env` | Kafka broker endpoint string |
| Topic | `KAFKA_TOPIC` | `--topic` | Environment / `.env` | Target Kafka topic |
| Rate | N/A | `-r`, `--rate` | `1000.0` | Target rate in transactions per second |
| Start Order ID | N/A | `-s`, `--start-order-id` | `10001` | Starting integer for unique sequential order IDs |
| Max Transactions | N/A | `-m`, `--max-transactions` | `None` (Unlimited) | Optional transaction count limit for finite execution |

---

## Usage Examples

### 1. Basic Command Line Usage

Run at default rate (1,000 tx/sec) using environment configuration (`.env`):

```bash
python generator/stream_transactions.py
```

### 2. Custom Rate & Order ID

Stream at 500 tx/sec starting with order ID 50000:

```bash
python generator/stream_transactions.py --rate 500 --start-order-id 50000
```

### 3. Explicit Broker & Topic Override

Override environment settings with explicit Kafka broker and topic parameters:

```bash
python generator/stream_transactions.py --rate 1000 --bootstrap-servers localhost:9092 --topic transactions
```

### 4. Help Command

View all available options and descriptions:

```bash
python generator/stream_transactions.py --help
```

---

## Rate Control Mechanism

The streaming module uses high-resolution monotonic timing (`time.perf_counter()`) to enforce target throughput. It calculates expected elapsed time per transaction to eliminate timing drift while avoiding unnecessary CPU spin-locking or heavy busy-waiting.

---

## Order ID Tracking

During a streaming session:
- Order IDs strictly increment (`start_order_id`, `start_order_id + 1`, `start_order_id + 2`, ...) across iterations.
- Order IDs do not reset on every iteration.

---

## Expected Transaction Structure

Each emitted transaction contains all required fields:

```json
{
  "order_id": 10001,
  "customer_id": "C482",
  "product_id": "P503",
  "quantity": 3,
  "price": 2450.0,
  "tax_amount": 1323.0,
  "payment_method": "UPI",
  "timestamp": "2026-08-23T14:58:00+00:00"
}
```

---

## Graceful Shutdown & Summary

When stopped via `Ctrl+C` (`KeyboardInterrupt`):
1. Transaction generation halts immediately.
2. Buffered Kafka messages are flushed (`producer.flush()`).
3. The Kafka producer connection is closed cleanly (`producer.close()`).
4. A concise execution summary is printed.
5. Exits with status code `0` without tracebacks.

### Example Summary Output:

```
Generated and published: 1,000
Generated and published: 2,000

Streaming stopped.
Transactions generated: 2,450
Elapsed time: 2.45 seconds
Average rate: 1000 tx/sec
```

---

## Testing Without Kafka

Unit tests in `tests/test_stream_transactions.py` use a mocked `KafkaProducerService` and run without requiring a live Kafka broker:

```bash
pytest tests/test_stream_transactions.py -v
```

---

## Optional Local Kafka Integration

To test against a real Kafka broker:

1. Start Kafka container stack:
   ```bash
   docker compose up -d
   ```
2. Set environment variables or pass CLI flags:
   ```bash
   python generator/stream_transactions.py --rate 10 --bootstrap-servers localhost:9092 --topic transactions
   ```
3. Stop the stream cleanly using `Ctrl+C`.
