# IceStream Observability Metrics API

## Purpose

The **IceStream Observability Metrics API** is a lightweight FastAPI backend service that exposes real-time streaming pipeline status, processing metrics, data quality statistics, incident notifications, and Apache Iceberg lakehouse storage metadata to the IceStream frontend interface.

---

## Architecture & Runtime Inspection

```text
Kafka Broker (9092) ──────┐
                          │
Flink JobManager (8081) ──┼──→ Observability Service (`backend/services/pipeline_metrics.py`)
                          │         │
Iceberg Warehouse ────────┘         ↓
                               FastAPI API (`backend/app.py`)
```

The API acts as a secure facade hiding internal infrastructure details from the frontend. It performs non-blocking, fail-safe probes against live services and falls back to clean, predictable response contracts when infrastructure components are offline.

### Metrics Collection Architecture

1. **Kafka Runtime Metrics**:
   - Performs rapid TCP socket probe (`timeout=0.5s`) against `KAFKA_BOOTSTRAP_SERVERS` (default: `localhost:9092`).
   - If reachable, metadata inspection via `KafkaConsumer` inspects topic existence and partition count for configured topic (default: `transactions`).
   - If offline or unreachable, returns structured fallback (`status: "not_running"`, `topic_exists: null`, `partition_count: null`).

2. **Flink REST API Metrics**:
   - Queries Flink JobManager REST API endpoints (`/overview` and `/jobs/overview`) at `FLINK_REST_URL` (default: `http://localhost:8081`) with short HTTP timeouts (`0.5s`).
   - Collects `flink_version`, `taskmanagers`, `slots_total`, `slots_available`, `jobs_running`, `jobs_failed`, and active running job metadata (`id`, `name`, `state`).
   - If JobManager is offline, returns fallback (`status: "not_running"`, `jobs_running: 0`, `jobs: []`).

3. **Iceberg Storage Metadata**:
   - Inspects local PyIceberg `SqlCatalog` or filesystem metadata files (`version-hint.text` and `v*.metadata.json`) under `ICEBERG_WAREHOUSE` (default: `./warehouse`).
   - Collects `catalog`, `namespace`, `table`, `table_exists`, `snapshot_count`, `latest_snapshot_id`, and `latest_metadata_file` without scanning Parquet data files.
   - If warehouse path is missing, returns fallback (`status: "unavailable"`, `table_exists: false`, `snapshot_count: 0`).

4. **Pipeline Status Aggregation Logic**:
   - **`healthy`**: Kafka, Flink, and Iceberg are all healthy.
   - **`degraded`**: At least one component is healthy and at least one is offline/unavailable.
   - **`unavailable`**: No infrastructure components are healthy.

---

## Available Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Service health status |
| `GET` | `/api/pipeline/status` | Aggregated component status (Kafka, Flink, Iceberg) and runtime details |
| `GET` | `/api/pipeline/metrics` | Stream processing metrics and component runtime summaries |
| `GET` | `/api/data-quality` | Data quality rules, validation counts, and score (`null` when inactive) |
| `GET` | `/api/incidents` | Active component incidents derived from runtime availability |
| `GET` | `/api/lakehouse` | Apache Iceberg catalog, table existence, and metadata snapshot state |

---

## Example Responses

### `GET /health`
```json
{
  "status": "healthy",
  "service": "icestream-observability-api"
}
```

### `GET /api/pipeline/status`
```json
{
  "overall_status": "degraded",
  "components": {
    "kafka": {
      "status": "not_running",
      "bootstrap_servers": "localhost:9092",
      "topic": "transactions",
      "topic_exists": null,
      "partition_count": null
    },
    "flink": {
      "status": "not_running",
      "flink_version": null,
      "taskmanagers": null,
      "slots_total": null,
      "slots_available": null,
      "jobs_running": 0,
      "jobs_failed": 0,
      "jobs": []
    },
    "iceberg": {
      "status": "healthy",
      "catalog": "local",
      "namespace": "icestream",
      "table": "transactions",
      "warehouse": "./warehouse",
      "table_exists": true,
      "snapshot_count": 45,
      "latest_snapshot_id": "954645740271090959",
      "latest_metadata_file": "v46.metadata.json",
      "record_count": null
    }
  }
}
```

### `GET /api/pipeline/metrics`
```json
{
  "source": "unavailable",
  "pipeline_status": "degraded",
  "transactions_processed": null,
  "valid_records": null,
  "invalid_records": null,
  "processing_errors": null,
  "records_per_second": null,
  "runtime": {
    "kafka": {
      "topic": "transactions",
      "partition_count": null
    },
    "flink": {
      "jobs_running": 0,
      "taskmanagers": null
    },
    "iceberg": {
      "snapshot_count": 45,
      "latest_snapshot_id": "954645740271090959"
    }
  }
}
```

### `GET /api/data-quality`
```json
{
  "total_records": 0,
  "valid_records": 0,
  "invalid_records": 0,
  "quality_score": null,
  "status": "no_data",
  "rules": [
    {
      "rule": "non_null_fields",
      "description": "Required fields order_id, customer_id, product_id, quantity, price, tax_amount, payment_method, timestamp must not be null",
      "status": "passed"
    }
  ]
}
```

### `GET /api/incidents`
```json
{
  "total_incidents": 2,
  "incidents": [
    {
      "id": "INC-KAFKA-OFFLINE",
      "severity": "high",
      "component": "kafka",
      "message": "Kafka broker is not reachable.",
      "timestamp": "2026-09-01T12:53:43.573778+00:00",
      "status": "open"
    },
    {
      "id": "INC-FLINK-OFFLINE",
      "severity": "high",
      "component": "flink",
      "message": "Apache Flink JobManager REST API is not reachable.",
      "timestamp": "2026-09-01T12:53:43.573778+00:00",
      "status": "open"
    }
  ]
}
```

### `GET /api/lakehouse`
```json
{
  "catalog": "local",
  "namespace": "icestream",
  "table": "transactions",
  "warehouse": "./warehouse",
  "table_exists": true,
  "snapshot_count": 45,
  "latest_snapshot_id": "954645740271090959",
  "latest_metadata_file": "v46.metadata.json",
  "record_count": null,
  "status": "healthy"
}
```

---

## How to Run Locally

### Start Development Server

Run the API using `uvicorn`:

```bash
python -m uvicorn backend.app:app --reload
```

The API server will run at:
- Base URL: `http://localhost:8000`
- Interactive API Docs (Swagger): `http://localhost:8000/docs`
- ReDoc Docs: `http://localhost:8000/redoc`

---

## How to Run Tests

Run the backend Observability API unit test suite:

```bash
python -m pytest tests/test_observability_api.py -v
```

Run the full repository regression test suite:

```bash
python -m pytest -v
```

---

## CORS Configuration

CORS middleware is pre-configured to allow requests from the React frontend development server (`http://localhost:5173`).

To customize allowed origins, set the `CORS_ORIGINS` environment variable (comma-separated):

```bash
export CORS_ORIGINS="http://localhost:5173,http://127.0.0.1:5173,http://localhost:3000"
```

---

## Offline Fallback Handling

1. **Structured Non-Blocking Probes**: Socket and REST API probes use short timeouts (0.5s) to guarantee response latency remains under <1s.
2. **Predictable Schema Fallbacks**: Offline infrastructure items return `"status": "not_running"` or `"status": "unavailable"` with `null` metric values instead of throwing 500 Internal Server Errors or fabricating metrics.
3. **Iceberg Record Count**: `record_count` remains `null` to avoid expensive Parquet scans during API GET requests. Snapshot counts and metadata file versions are extracted directly from lightweight metadata JSON files.

