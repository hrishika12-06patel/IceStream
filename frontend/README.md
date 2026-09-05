# IceStream Frontend

IceStream Frontend is the real-time observability dashboard for the **IceStream streaming data pipeline**.

It provides a visual interface for monitoring transaction data as it moves through:

**Kafka → Apache Flink → Apache Iceberg**

The frontend is built with **React, Vite, React Flow, and Lucide React** and connects to the IceStream backend API to display pipeline health, operational metrics, data quality, incidents, and lakehouse information.

---

## Features

### Real-Time Pipeline Dashboard

The main dashboard provides an overview of the IceStream streaming pipeline, including:

- Kafka ingestion status
- Flink processing status
- Apache Iceberg storage status
- Events processed per second
- Pipeline latency
- Data quality score
- Total records processed
- Recent pipeline alerts
- Stage-wise latency
- Active pipeline status

### Interactive Pipeline Visualisation

The streaming architecture is visualised using **React Flow**.

```text
Transaction Generator
        ↓
      Kafka
        ↓
 Apache Flink
        ↓
 Apache Iceberg
```

The dashboard contains interactive nodes for:

- **Kafka** — Ingest
- **Flink** — Process
- **Iceberg** — Serve

Pipeline connections are displayed using animated edges to make the data flow easier to understand.

### Interactive Node Details

Kafka, Flink, and Iceberg nodes can be selected to inspect individual pipeline components.

The node details panel displays:

- Service name
- Pipeline stage
- Service health
- Current service metric
- Processing or ingestion information
- Service latency

This allows individual pipeline components to be inspected directly from the visualisation.

### Dynamic Pipeline Health

The dashboard can represent multiple pipeline states:

- Healthy
- Degraded
- Offline
- Unavailable

The interface updates service and pipeline status based on information received from the backend.

### Loading and Error Handling

The frontend handles situations where backend information is loading or unavailable.

For example:

```text
Loading IceStream...
```

or:

```text
Pipeline unavailable
```

This prevents the dashboard from displaying misleading operational information when the backend cannot be reached.

### Navigation

The sidebar provides navigation for monitoring views such as:

- Dashboard
- Pipeline
- Data Quality
- Alerts
- Pipeline Nodes
- Lakehouse
- Metrics
- Issues
- Settings

---

## Backend Integration

Frontend API communication is handled through:

```text
src/api/pipelineApi.js
```

The frontend keeps backend requests separate from the UI components.

The data flow follows:

```text
IceStream Backend API
        ↓
pipelineApi.js
        ↓
     App.jsx
        ↓
React Components
        ↓
    Dashboard
```

This keeps the API and presentation layers separated and makes the frontend easier to maintain.

---

## Technology Stack

| Technology | Purpose |
| --- | --- |
| React | Frontend UI |
| Vite | Development and build tooling |
| React Flow / XYFlow | Interactive pipeline visualisation |
| Lucide React | Dashboard icons |
| JavaScript | Frontend logic |
| CSS | Dashboard styling |
| Fetch API | Backend communication |

---

## Project Structure

```text
frontend/
│
├── public/
│   ├── favicon.svg
│   └── icons.svg
│
├── src/
│   ├── api/
│   │   └── pipelineApi.js
│   │
│   ├── assets/
│   │   ├── hero.png
│   │   ├── react.svg
│   │   └── vite.svg
│   │
│   ├── App.jsx
│   ├── App.css
│   ├── index.css
│   └── main.jsx
│
├── .env
├── .gitignore
├── eslint.config.js
├── index.html
├── package.json
├── package-lock.json
├── vite.config.js
└── README.md
```

---

## Environment Configuration

Create a `.env` file inside the `frontend` directory:

```text
IceStream/
│
├── frontend/
│   ├── .env
│   ├── src/
│   └── package.json
│
└── ...
```

Add the backend API URL:

```env
VITE_API_BASE_URL=http://127.0.0.1:8000
```

The frontend uses this environment variable to communicate with the IceStream backend API.

> Do not commit the `.env` file if it contains environment-specific or sensitive configuration.

---

## Installation

### 1. Clone the repository

```bash
git clone <repository-url>
```

Move into the frontend directory:

```bash
cd IceStream/frontend
```

### 2. Install dependencies

```bash
npm install
```

---

## Running the Frontend

Start the Vite development server:

```bash
npm run dev
```

The frontend will normally be available at:

```text
http://localhost:5173
```

---

## Running with the Backend

The IceStream backend should also be running to display live pipeline information.

From the **IceStream project root**, start the backend with:

```bash
uvicorn backend.app:app --reload
```

The backend runs at:

```text
http://127.0.0.1:8000
```

The local frontend/backend flow is:

```text
Backend
http://127.0.0.1:8000
        ↓
pipelineApi.js
        ↓
Frontend
http://localhost:5173
```

---

## Production Build

Before committing frontend changes, verify that the application builds successfully.

From the `frontend` directory:

```bash
npm run build
```

The production build is generated inside:

```text
frontend/dist/
```

---

## Linting

Run ESLint with:

```bash
npm run lint
```

Resolve relevant lint errors before creating a pull request.

---

## Dashboard Architecture

```text
                  IceStream Dashboard
                         │
        ┌────────────────┼────────────────┐
        │                │                │
      Kafka            Flink           Iceberg
     INGEST            PROCESS           SERVE
        │                │                │
        └────────────────┼────────────────┘
                         │
                  Pipeline Metrics
                         │
        ┌────────────────┼────────────────┐
        │                │                │
 Active Pipeline       Latency          Alerts
```

---

## Design

The frontend uses a **dark infrastructure observability design** inspired by modern monitoring dashboards.

The interface includes:

- Dark dashboard layout
- Fixed monitoring sidebar
- Infrastructure status indicators
- Interactive pipeline nodes
- Animated pipeline connections
- Metric cards
- Health indicators
- Alert cards
- Responsive layouts
- Service detail panels

The design focuses on making streaming infrastructure easy to monitor and understand.

---

## IceStream Pipeline

The overall IceStream architecture is:

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
 IceStream Frontend
```

The frontend acts as the monitoring and visualisation layer for the streaming pipeline.

---

## Development Guidelines

When working on the frontend:

1. Keep API communication inside `src/api/pipelineApi.js`.
2. Avoid hardcoding operational metrics when backend data is available.
3. Keep reusable UI logic separated into components or functions.
4. Maintain the existing observability dashboard design.
5. Handle loading, offline, and error states.
6. Test changes using `npm run dev`.
7. Run `npm run lint` before committing.
8. Run `npm run build` before creating a pull request.

---

## Future Improvements

Possible future frontend improvements include:

- Historical pipeline charts
- Real-time throughput graphs
- Detailed incident inspection
- Data quality rule visualisation
- Iceberg snapshot history
- Pipeline node drill-down views
- Configurable alert thresholds
- WebSocket-based live metric updates
- Advanced monitoring filters
- Improved mobile navigation