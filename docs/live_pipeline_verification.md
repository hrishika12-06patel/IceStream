# IceStream Live Pipeline Verification Guide

This document provides exact, sequential PowerShell commands for running, inspecting, verifying, and stopping the live **Transaction Generator → Apache Kafka → Apache Flink → Apache Iceberg → Record Readback** pipeline.

---

## Infrastructure Requirements & Prerequisites

> [!IMPORTANT]  
> **Prerequisites Checklist**:
> 1. **Docker Desktop**: Docker Desktop must be installed, running, and configured with Linux containers enabled.
> 2. **Python Environment**: Python 3.10+ with repository dependencies installed (`pip install -r requirements.txt`).
> 3. **Java**: Java 11 / 17 runtime for Flink execution.

If Docker is not running or not installed (`docker: command not found`), live execution cannot start. Run unit tests (`python -m pytest -v`) for offline verification.

---

## Pipeline Architecture

```text
Host Producer (stream_transactions.py)
        │ (localhost:9092)
        ▼
   icestream-kafka (apache/kafka:4.0.0)
        │ (kafka:9092)
        ▼
   icestream-jobmanager & icestream-taskmanager (flink:2.3.0-scala_2.12)
        │ (JSON Parsing -> Schema Validation -> total_amount Calculation -> ProcessingMetrics)
        ▼
   Apache Iceberg Storage Layer (icestream.transactions)
        │
        ▼
   Local Iceberg Warehouse (./warehouse Parquet & Metadata files)
```

---

## Exact Step-by-Step PowerShell Verification Guide

Execute the following PowerShell commands in order from the repository root (`c:\Axlero\IceStream`).

### Step 1: Start Infrastructure Containers

Starts Kafka broker, Flink JobManager, and Flink TaskManager in detached mode:

```powershell
docker compose up -d
```

---

### Step 2: Verify Container Health & Status

Confirms all containers are running and healthy:

```powershell
docker compose ps
```

Expected output should show `icestream-kafka`, `icestream-jobmanager`, and `icestream-taskmanager` with status `Up`.

---

### Step 3: Start Flink Transaction Processing Job

Launches the Flink streaming processor which initializes the `icestream.transactions` Iceberg table and consumes Kafka messages:

```powershell
python flink/transaction_processor.py
```

---

### Step 4: Start Transaction Streaming Generator (Separate Terminal)

Streams controlled e-commerce transactions to Kafka (e.g. rate of 10 tx/sec, sending 50 transactions):

```powershell
python generator/stream_transactions.py -r 10 -m 50
```

---

### Step 5: Inspect Kafka & Flink Container Logs

Inspect live streaming and job execution logs from the containers:

```powershell
# View Kafka broker logs
docker compose logs -f kafka

# View Flink JobManager logs
docker compose logs -f jobmanager

# View Flink TaskManager logs
docker compose logs -f taskmanager
```

---

### Step 6: Verify Records Written & Read Back from Apache Iceberg

Run a Python script to inspect and query the stored transactions table directly from the Iceberg catalog:

```powershell
python -c "
from pyiceberg.catalog.sql import SqlCatalog
from iceberg.iceberg_config import IcebergConfig
import os

cfg = IcebergConfig.from_env()
db_path = os.path.abspath(os.path.join(cfg.warehouse, 'catalog.db'))
catalog = SqlCatalog(cfg.catalog, **{'uri': f'sqlite:///{db_path}', 'warehouse': os.path.abspath(cfg.warehouse)})

table = catalog.load_table(cfg.full_table_name)
print('Successfully loaded Iceberg Table:', table.identifier)
print('Current Snapshot:', table.current_snapshot())
print('Schema Fields:')
for field in table.schema().fields:
    print(f'  - {field.name}: {field.field_type}')
"
```

---

### Step 7: Stop Infrastructure Containers

Shuts down containers and cleans up volume networks:

```powershell
docker compose down -v
```

---

## Offline Verification (When Docker is Unavailable)

If Docker is not available on the current machine, verify all offline logic, transaction parsing, schema validation, metrics, and local catalog writes by executing the complete test suite:

```powershell
python -m pytest -v
```
