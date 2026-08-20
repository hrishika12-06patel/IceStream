# IceStream Transaction Generator

## Overview

The IceStream transaction generator simulates high-volume e-commerce transactions for future streaming workloads.

It supports:

- Configurable transaction count
- Configurable transactions-per-second rate
- Finite generation
- Batch generation
- Continuous generation
- Performance measurement

Kafka integration is **not included** in the current generator.

## File

```text
generator/
└── transaction_generator.py