# IceStream Observability Metrics API

## Purpose

The **IceStream Observability Metrics API** is a lightweight FastAPI backend service that exposes real-time streaming pipeline status, processing metrics, data quality statistics, incident notifications, and Apache Iceberg lakehouse storage metadata to the IceStream frontend interface.

---

## Architecture

```text
Frontend (React / Vite)
        ↓  (HTTP REST API)
FastAPI Observability API (`backend/app.py`)
        ↓
Observability Service Layer (`backend/services/pipeline_metrics.py`)
        ↓  (Health Checks / Metrics / Lakehouse Metadata)
┌─────────────────┬──────────────────────┬──────────────────────┐
│  Kafka Broker   │  PyFlink JobManager  │   Apache Iceberg     │
│  (Port 9092)    │  REST API (8081)     │   Catalog & Warehouse│
└─────────────────┴──────────────────────┴──────────────────────┘
```

The API acts as a secure facade hiding internal infrastructure details from the frontend. It performs non-blocking, fail-safe health probes against live services and falls back to clean, predictable response contracts when infrastructure components are offline.

---

## Available Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Service health status |
| `GET` | `/api/pipeline/status` | Real-time status of pipeline components (Kafka, Flink, Iceberg) |
| `GET` | `/api/pipeline/metrics` | Stream processing metrics (records processed, valid/invalid, throughput) |
| `GET` | `/api/data-quality` | Data quality rules, validation counts, and overall quality score |
| `GET` | `/api/incidents` | Detected pipeline outages and active operational incidents |
| `GET` | `/api/lakehouse` | Apache Iceberg catalog, namespace, table existence, and snapshot counts |

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
  "overall_status": "healthy",
  "components": {
    "kafka": {
      "status": "healthy"
    },
    "flink": {
      "status": "healthy"
    },
    "iceberg": {
      "status": "healthy"
    }
  }
}
```

### `GET /api/pipeline/metrics`
```json
{
  "source": "runtime",
  "transactions_processed": 1200,
  "valid_records": 1180,
  "invalid_records": 20,
  "processing_errors": 0,
  "records_per_second": 120.5
}
```

### `GET /api/data-quality`
```json
{
  "total_records": 1200,
  "valid_records": 1180,
  "invalid_records": 20,
  "quality_score": 98.33,
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
  "total_incidents": 0,
  "incidents": []
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
  "snapshot_count": 1,
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

## Current Limitations

1. **Offline Environment Fallback**: When Docker, Kafka, or Flink are not running, endpoints return graceful fallback statuses (`"unknown"`, `"not_running"`, `"unavailable"`) without fabricating live metrics.
2. **Flink Metric Granularity**: Flink runtime metrics rely on Flink REST API (`http://localhost:8081`). When PyFlink is executing locally outside of JobManager, standard metric fallbacks apply.
3. **Iceberg Record Count**: Exact record counts require table scans over Iceberg metadata; when metadata scanning is costly or offline, `record_count` remains `null`.
