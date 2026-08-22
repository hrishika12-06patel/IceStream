# Kafka Setup

## Overview

Apache Kafka is used in IceStream as the streaming layer for incoming
e-commerce transaction data.

The intended data flow is:

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
Iceberg

For the current stage of the project, Kafka is being set up as the
streaming foundation. Flink and Iceberg integration will be added in
later stages.

---

## Local Kafka Configuration

| Configuration | Value |
|---|---|
| Kafka Broker | `localhost:9092` |
| Topic | `transactions` |
| Partitions | `3` |
| Replication Factor | `1` |
| Mode | KRaft |

Kafka is configured as a single local broker for development.

---

## Starting Kafka

From the IceStream project root, run:

```bash
docker compose up -d