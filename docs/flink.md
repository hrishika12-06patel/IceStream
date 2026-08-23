# Flink Setup

## Overview

Apache Flink is the stream-processing layer of IceStream.

It will process the transaction stream received from Kafka and will
later perform data-quality processing before the data is written to
the Iceberg storage layer.

The intended IceStream architecture is:

Transaction Generator
        ↓
Kafka Producer
        ↓
Kafka
        ↓
transactions topic
        ↓
Flink
        ↓
Data Quality Processing
        ↓
Iceberg

At the current stage, the local Flink cluster has been set up.
The Kafka-to-Flink processing job will be implemented in a later stage.

---

## Local Flink Architecture

The local Flink environment consists of:

- JobManager
- TaskManager

The JobManager manages the Flink cluster and coordinates jobs.

The TaskManager executes the processing tasks assigned by the
JobManager.

The local architecture is:

JobManager
     ↓
TaskManager

---

## Docker Configuration

Flink is run using Docker Compose.

The project uses:

- Flink JobManager
- Flink TaskManager

The Flink Web UI is exposed on:

`localhost:8081`

---

## Flink Configuration

The JobManager address is:

`jobmanager`

This allows the TaskManager container to connect to the JobManager
using the Docker Compose service name.

The local TaskManager is configured with:

`2` task slots.

---

## Starting Flink

From the IceStream project root, run:

```bash
docker compose up -d

##Check the running containers:

docker compose ps

The Flink services should include:

icestream-jobmanager
icestream-taskmanager
Checking Flink

Open the Flink Web UI:

http://localhost:8081

The dashboard can be used to verify that the Flink cluster is running
and that the TaskManager is connected.

Viewing Logs

To view JobManager logs:

docker compose logs jobmanager

To view TaskManager logs:

docker compose logs taskmanager

To follow TaskManager logs:

docker compose logs -f taskmanager
Stopping Flink

To stop the IceStream containers:

docker compose down

This stops the Kafka and Flink containers.

Current Status

Currently implemented:

Kafka local environment
transactions Kafka topic
Flink JobManager
Flink TaskManager
Flink Web UI
Local Kafka and Flink infrastructure

Not implemented yet:

Kafka-to-Flink streaming job
Flink transaction processing
Flink data-quality validation
Flink-to-Iceberg integration

These will be implemented in later stages.