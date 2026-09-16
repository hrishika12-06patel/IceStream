# IceStream

## Real-Time Lakehouse Observability

**IceStream** is a real-time data quality and observability platform for streaming e-commerce transaction data.

It provides an end-to-end pipeline for generating transaction events, streaming them through Kafka, processing them with Apache Flink, storing them in Apache Iceberg, and monitoring the complete pipeline through an observability API and interactive frontend dashboard.

---

## Overview

Modern streaming systems need more than just data movement. They also need visibility into pipeline health, processing performance, data quality, incidents, and storage.

IceStream provides this visibility across the complete streaming pipeline:

```text
Transaction Generator
        ↓
      Kafka
        ↓
 Apache Flink
        ↓
 Apache Iceberg
        ↓
Observability API
        ↓
 React Dashboard
```

The project combines streaming data processing, lakehouse storage, data-quality validation, operational monitoring, and frontend visualization in one system.

---

## Problem

Traditional batch-oriented data pipelines may detect data-quality problems only after data has already been processed or stored.

Issues such as:

- Missing values
- Invalid numeric values
- Duplicate records
- Schema inconsistencies
- Invalid transaction fields
- Processing failures
- Pipeline service outages

can reduce the reliability of downstream analytics.

IceStream is designed to detect and expose these problems while providing visibility into the health of the streaming pipeline.

---

## Architecture

```text
┌──────────────────────────┐
│ Transaction Generator    │
│ + Anomaly Generator      │
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│ Apache Kafka             │
│ Streaming Ingestion      │
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│ Apache Flink             │
│ Stream Processing        │
│ + Validation             │
│ + Transformations        │
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│ Apache Iceberg           │
│ Lakehouse Storage        │
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│ FastAPI Observability    │
│ API                      │
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│ React Frontend           │
│ Monitoring Dashboard     │
└──────────────────────────┘
```

---

## Core Features

### Transaction Generation

IceStream includes a configurable transaction generator for creating realistic e-commerce transaction data.

The generator supports transaction attributes such as:

- Order IDs
- Customer IDs
- Product IDs
- Quantity
- Price
- Payment method
- Timestamp

The project also contains an anomaly generator for introducing controlled data-quality problems for testing and validation.

---

### Kafka Streaming

Generated transactions can be published to Apache Kafka using the Kafka producer included in the project.

Relevant files:

```text
generator/kafka_producer.py
generator/stream_transactions.py
```

Kafka acts as the ingestion layer between the transaction generator and stream-processing system.

---

### Apache Flink Processing

Apache Flink processes the incoming Kafka transaction stream.

The Flink layer contains:

```text
flink/
├── README.md
├── submit_job.sql
└── transaction_processor.py
```

It handles streaming transaction processing and integration with the Iceberg storage layer.

---

### Apache Iceberg Storage

Processed transactions are stored using Apache Iceberg.

The Iceberg configuration is maintained in:

```text
iceberg/
├── README.md
└── iceberg_config.py
```

The storage layer provides configuration for the Iceberg catalog, warehouse, namespace, table, and transaction schema.

---

### Data Quality Validation

IceStream contains a dedicated transaction validation layer:

```text
validation/
└── transaction_validator.py
```

Validation is used to identify invalid or anomalous transaction records before or during downstream processing.

Data-quality behavior and rules are documented under:

```text
docs/data_quality.md
docs/data_quality_rules.md
docs/anomalies.md
```

---

## Observability API

IceStream includes a FastAPI backend for exposing pipeline health and operational information.

Backend structure:

```text
backend/
├── services/
│   ├── __init__.py
│   └── pipeline_metrics.py
├── README.md
├── __init__.py
└── app.py
```

The backend provides APIs for monitoring areas such as:

- Pipeline status
- Pipeline metrics
- Metrics history
- Data quality
- Incidents
- Lakehouse status
- Service health

Examples of implemented API routes include:

```text
GET /health

GET /api/pipeline/status

GET /api/pipeline/metrics

GET /api/pipeline/metrics/history

GET /api/data-quality

GET /api/incidents

GET /api/lakehouse
```

The metrics-history API supports retrieval of recent pipeline metric snapshots for historical monitoring and frontend comparison views.

---

## Frontend Dashboard

IceStream includes a React-based observability dashboard located in:

```text
frontend/
```

The frontend provides visual monitoring of the streaming infrastructure and communicates with the FastAPI backend.

### Dashboard Features

The frontend includes:

- Pipeline overview
- Kafka, Flink, and Iceberg status
- Interactive pipeline visualization
- Pipeline node details
- Pipeline metrics
- Metrics history
- Latest-vs-previous metrics comparison
- Data-quality monitoring
- Incident monitoring
- Lakehouse information
- Loading and backend-error states
- Responsive dashboard layouts

### Frontend Views

```text
frontend/src/views/
├── AlertsView.jsx
├── DataQualityView.jsx
├── HistoryView.jsx
├── IncidentsView.jsx
├── LakehouseView.jsx
├── MetricsView.jsx
└── SettingsView.jsx
```

### Frontend Components

```text
frontend/src/components/
├── DataQualityPanel.jsx
├── IncidentPanel.jsx
├── LakehousePanel.jsx
├── MetricsComparisonCard.jsx
└── MetricsHistoryChart.jsx
```

API communication is centralized in:

```text
frontend/src/api/pipelineApi.js
```

This keeps backend communication separate from presentation logic.

---

## Metrics History and Comparison

IceStream stores a bounded recent history of pipeline metric snapshots.

Historical information can be used by the frontend to visualize changes in pipeline behavior over time.

The dashboard also supports comparison between the latest and previous snapshots for metrics such as:

- Transactions processed
- Records per second
- Processing errors

The comparison interface safely handles missing values and avoids displaying invalid calculations such as `NaN%` or `Infinity%`.

Snapshot timestamps can also be used to show when each metric snapshot was captured.

---

## Project Structure

```text
IceStream/
│
├── backend/
│   ├── services/
│   │   ├── __init__.py
│   │   └── pipeline_metrics.py
│   ├── README.md
│   ├── __init__.py
│   └── app.py
│
├── data/
│   └── sample_transactions.json
│
├── docs/
│   ├── anomalies.md
│   ├── data_quality.md
│   ├── data_quality_rules.md
│   ├── flink.md
│   ├── kafka.md
│   ├── kafka_producer.md
│   ├── live_pipeline_verification.md
│   ├── stream_transactions.md
│   ├── transaction_generator.md
│   └── transaction_schema.md
│
├── flink/
│   ├── README.md
│   ├── submit_job.sql
│   └── transaction_processor.py
│
├── frontend/
│   ├── public/
│   │   ├── favicon.svg
│   │   └── icons.svg
│   │
│   ├── src/
│   │   ├── api/
│   │   │   └── pipelineApi.js
│   │   │
│   │   ├── assets/
│   │   │   ├── hero.png
│   │   │   ├── react.svg
│   │   │   └── vite.svg
│   │   │
│   │   ├── components/
│   │   │   ├── DataQualityPanel.jsx
│   │   │   ├── IncidentPanel.jsx
│   │   │   ├── LakehousePanel.jsx
│   │   │   ├── MetricsComparisonCard.jsx
│   │   │   └── MetricsHistoryChart.jsx
│   │   │
│   │   ├── data/
│   │   │   ├── dataQualityMock.js
│   │   │   ├── historyMock.js
│   │   │   ├── incidentMock.js
│   │   │   └── lakehouseMock.js
│   │   │
│   │   ├── views/
│   │   │   ├── AlertsView.jsx
│   │   │   ├── DataQualityView.jsx
│   │   │   ├── HistoryView.jsx
│   │   │   ├── IncidentsView.jsx
│   │   │   ├── LakehouseView.jsx
│   │   │   ├── MetricsView.jsx
│   │   │   └── SettingsView.jsx
│   │   │
│   │   ├── App.css
│   │   ├── App.jsx
│   │   ├── index.css
│   │   └── main.jsx
│   │
│   ├── .gitignore
│   ├── README.md
│   ├── eslint.config.js
│   ├── index.html
│   ├── package-lock.json
│   ├── package.json
│   └── vite.config.js
│
├── generator/
│   ├── anomaly_generator.py
│   ├── kafka_producer.py
│   ├── stream_transactions.py
│   └── transaction_generator.py
│
├── iceberg/
│   ├── README.md
│   └── iceberg_config.py
│
├── tests/
│   ├── test_flink_iceberg_integration.py
│   ├── test_iceberg_config.py
│   ├── test_kafka_producer.py
│   ├── test_observability_api.py
│   ├── test_stream_transactions.py
│   ├── test_transaction_processor.py
│   └── test_transaction_validation.py
│
├── validation/
│   └── transaction_validator.py
│
├── .env.example
└── .gitignore
```

---

## Technology Stack

| Layer | Technology |
|---|---|
| Data Generation | Python |
| Streaming | Apache Kafka |
| Stream Processing | Apache Flink |
| Lakehouse Storage | Apache Iceberg |
| Backend API | FastAPI |
| Validation | Python |
| Frontend | React |
| Build Tool | Vite |
| Pipeline Visualization | React Flow / XYFlow |
| Frontend Icons | Lucide React |
| Testing | Pytest |
| Containerized Infrastructure | Docker |

---

## Environment Configuration

The repository includes:

```text
.env.example
```

Example configuration:

```env
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=transactions

# Iceberg Storage Configuration
ICEBERG_CATALOG=local
ICEBERG_WAREHOUSE=./warehouse
ICEBERG_NAMESPACE=icestream
ICEBERG_TABLE=transactions

VITE_API_BASE_URL=http://127.0.0.1:8000
```

Create your local environment configuration from the example file as required.

Do not commit secrets or machine-specific configuration.

---

## Running the Backend

Create and activate a Python virtual environment, then install the project dependencies.

From the project root:

```bash
pip install -r requirements.txt
```

Start the FastAPI observability backend:

```bash
uvicorn backend.app:app --reload
```

The backend will normally be available at:

```text
http://127.0.0.1:8000
```

---

## Running the Frontend

Move into the frontend directory:

```bash
cd frontend
```

Install dependencies:

```bash
npm install
```

Start the development server:

```bash
npm run dev
```

Vite will display the local development URL, typically:

```text
http://localhost:5173
```

For live monitoring information, the IceStream backend must also be running.

---

## Frontend Environment

Create:

```text
frontend/.env
```

and configure the backend URL:

```env
VITE_API_BASE_URL=http://127.0.0.1:8000
```

Restart the Vite development server after changing environment variables.

---

## Testing

IceStream includes tests for multiple parts of the pipeline.

Run the complete Python test suite from the project root:

```bash
python -m pytest -v
```

Tests cover areas including:

- Transaction validation
- Kafka producer behavior
- Transaction streaming
- Flink transaction processing
- Iceberg configuration
- Flink → Iceberg integration
- Observability API behavior

A Docker-dependent integration test may be skipped when the required containerized infrastructure is unavailable.

---

## Frontend Validation

From:

```bash
cd frontend
```

Run ESLint:

```bash
npm run lint
```

Create a production build:

```bash
npm run build
```

The production output is generated under:

```text
frontend/dist/
```

---

## Documentation

Detailed documentation is available in the `docs/` directory.

| Document | Purpose |
|---|---|
| `transaction_generator.md` | Transaction generation |
| `transaction_schema.md` | Transaction structure |
| `anomalies.md` | Generated anomaly behavior |
| `data_quality.md` | Data-quality approach |
| `data_quality_rules.md` | Validation rules |
| `kafka.md` | Kafka setup and architecture |
| `kafka_producer.md` | Kafka producer |
| `stream_transactions.md` | Streaming transactions |
| `flink.md` | Flink processing |
| `live_pipeline_verification.md` | Live pipeline verification |

Additional component-specific documentation is available in:

```text
backend/README.md
flink/README.md
iceberg/README.md
frontend/README.md
```

---

## Current Pipeline

IceStream currently follows this end-to-end architecture:

```text
E-Commerce Transaction Generator
              ↓
       Anomaly Injection
              ↓
         Apache Kafka
              ↓
         Apache Flink
              ↓
       Apache Iceberg
              ↓
      Pipeline Monitoring
              ↓
     FastAPI Observability
              ↓
      React Dashboard
```

The project has progressed beyond the original planned architecture into an integrated streaming and observability system with backend monitoring, lakehouse storage, automated tests, and an interactive frontend.