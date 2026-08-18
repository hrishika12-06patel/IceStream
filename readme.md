# IceStream

## Real-Time Lakehouse Observability

IceStream is a real-time data quality and observability system
designed for streaming e-commerce transaction data.

The system monitors incoming data, detects data-quality issues,
and provides visibility into the health of the data pipeline.

## Problem

Traditional batch ETL pipelines can take hours to identify
data-quality problems such as NULL values or schema changes.

IceStream aims to detect these problems while data is being
processed in real time.

## Planned Architecture

```text
Python Transaction Generator
            ↓
          Kafka
            ↓
          Flink
            ↓
         Iceberg
            ↓
   Data Quality Monitoring
            ↓
       React Flow UI