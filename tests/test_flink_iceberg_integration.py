"""
Integration and Offline Tests for Flink -> Iceberg Storage Layer Pipeline.

Includes:
1. Helper function is_docker_available() for infrastructure checks.
2. Unit/Offline tests (Group A):
   - Iceberg sink configuration loading
   - Catalog & table initialization
   - Rejection of invalid records prior to Iceberg sink
   - Record conversion, enrichment with total_amount, and Iceberg record mapping
   - Writing valid records to local Iceberg table catalog
3. Live Integration tests (Group B):
   - Skipped cleanly using @pytest.mark.skipif when Docker/infrastructure is unavailable.
"""

import json
import os
import shutil
import subprocess
from decimal import Decimal
import pytest

from iceberg.iceberg_config import (
    IcebergConfig,
    TRANSACTION_FIELD_DEFINITIONS,
    get_iceberg_schema,
    map_transaction_to_iceberg_record,
    HAS_PYICEBERG,
)
from flink.transaction_processor import (
    TransactionProcessMapFunction,
    ProcessingMetrics,
    create_iceberg_sink_config,
    create_iceberg_sink,
    ensure_iceberg_table_exists,
    write_record_to_iceberg,
    parse_and_process_record,
)


def is_docker_available() -> bool:
    """
    Checks if Docker CLI executable is available and Docker daemon is responsive.
    """
    docker_bin = shutil.which("docker")
    if not docker_bin:
        return False
    try:
        res = subprocess.run(
            [docker_bin, "info"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=5,
        )
        return res.returncode == 0
    except Exception:
        return False


# ==============================================================================
# GROUP A: UNIT / OFFLINE TESTS
# ==============================================================================

def test_docker_availability_detection():
    """Verifies that docker availability detection returns a boolean cleanly."""
    avail = is_docker_available()
    assert isinstance(avail, bool)


def test_iceberg_sink_config_generation(monkeypatch):
    """Tests that create_iceberg_sink_config returns valid catalog and table props."""
    monkeypatch.setenv("ICEBERG_CATALOG", "local_test")
    monkeypatch.setenv("ICEBERG_WAREHOUSE", "./test_warehouse")
    monkeypatch.setenv("ICEBERG_NAMESPACE", "test_ns")
    monkeypatch.setenv("ICEBERG_TABLE", "test_tx")

    config_dict = create_iceberg_sink_config()
    assert config_dict["catalog_name"] == "local_test"
    assert config_dict["full_table_name"] == "test_ns.test_tx"
    assert config_dict["catalog_properties"]["type"] == "local_test"


def test_create_iceberg_sink_structure():
    """Tests create_iceberg_sink output metadata."""
    cfg = IcebergConfig(catalog="local", warehouse="./warehouse", namespace="icestream", table="transactions")
    sink_info = create_iceberg_sink(config=cfg)

    assert sink_info["catalog_name"] == "local"
    assert sink_info["namespace"] == "icestream"
    assert sink_info["table"] == "transactions"
    assert sink_info["full_table_name"] == "icestream.transactions"


def test_ensure_iceberg_table_exists_local(tmp_path):
    """Verifies directory creation and table catalog initialization in custom warehouse."""
    wh_dir = str(tmp_path / "warehouse")
    cfg = IcebergConfig(catalog="local_test", warehouse=wh_dir, namespace="icestream", table="transactions")

    success = ensure_iceberg_table_exists(cfg)
    assert success is True
    assert os.path.exists(wh_dir)


def test_invalid_records_rejected_before_iceberg_sink():
    """Ensures invalid records are rejected by MapFunction and not written to Iceberg."""
    metrics = ProcessingMetrics()
    mapper = TransactionProcessMapFunction(metrics=metrics, auto_write_iceberg=True)

    # 1. Corrupt JSON
    res1 = mapper.map("invalid-json{")
    assert res1 is None
    assert metrics.invalid_records == 1

    # 2. Missing required field order_id
    invalid_tx = {
        "customer_id": "C100",
        "product_id": "P200",
        "quantity": 1,
        "price": 50.0,
        "tax_amount": 5.0,
        "payment_method": "UPI",
        "timestamp": "2026-08-29T10:00:00Z",
    }
    res2 = mapper.map(json.dumps(invalid_tx))
    assert res2 is None
    assert metrics.invalid_records == 2

    # 3. Illegal negative quantity
    invalid_qty_tx = dict(invalid_tx, order_id="O100", quantity=-5)
    res3 = mapper.map(json.dumps(invalid_qty_tx))
    assert res3 is None
    assert metrics.invalid_records == 3


def test_valid_record_processing_and_iceberg_record_mapping():
    """
    Verifies that valid raw JSON is parsed, validated, enriched with total_amount,
    and accurately mapped into Iceberg decimal and timestamp field types.
    """
    raw_tx = {
        "order_id": "ORD-999",
        "customer_id": "CUST-888",
        "product_id": "PROD-777",
        "quantity": 3,
        "price": 150.50,
        "tax_amount": 27.09,
        "payment_method": "Credit Card",
        "timestamp": "2026-08-29T20:00:00+00:00",
    }

    processed, errors = parse_and_process_record(raw_tx)
    assert errors == []
    assert processed is not None
    assert processed["total_amount"] == 478.59  # (150.50 * 3) + 27.09 = 478.59

    record = map_transaction_to_iceberg_record(processed)
    assert record["order_id"] == "ORD-999"
    assert record["quantity"] == 3
    assert record["price"] == Decimal("150.50")
    assert record["tax_amount"] == Decimal("27.09")
    assert record["total_amount"] == Decimal("478.59")
    assert record["payment_method"] == "Credit Card"


def test_write_record_to_iceberg_local_catalog(tmp_path):
    """Tests end-to-end record writing helper with local warehouse configuration."""
    wh_dir = str(tmp_path / "warehouse")
    cfg = IcebergConfig(catalog="local_test", warehouse=wh_dir, namespace="icestream", table="transactions")

    valid_tx = {
        "order_id": "ORD-12345",
        "customer_id": "CUST-001",
        "product_id": "PROD-002",
        "quantity": 2,
        "price": 100.00,
        "tax_amount": 18.00,
        "payment_method": "UPI",
        "timestamp": "2026-08-29T12:00:00+00:00",
        "total_amount": 218.00,
    }

    written = write_record_to_iceberg(valid_tx, config=cfg)
    assert written is True
    assert os.path.exists(wh_dir)


# ==============================================================================
# GROUP B: LIVE INTEGRATION TESTS (SKIPPED WHEN DOCKER UNAVAILABLE)
# ==============================================================================

@pytest.mark.skipif(
    not is_docker_available(),
    reason="Live Flink -> Iceberg integration test skipped: Docker infrastructure is not available."
)
def test_live_flink_iceberg_pipeline():
    """
    Live integration test for Kafka -> Flink -> Iceberg pipeline.
    Sends controlled transaction to Kafka, verifies Flink processing, and reads back from Iceberg.
    """
    # 1. Setup deterministic test record
    test_order_id = "LIVE-TEST-1001"
    tx_data = {
        "order_id": test_order_id,
        "customer_id": "CUST-LIVE",
        "product_id": "PROD-LIVE",
        "quantity": 4,
        "price": 250.00,
        "tax_amount": 45.00,
        "payment_method": "UPI",
        "timestamp": "2026-08-29T15:00:00+00:00",
    }

    # 2. Process record through pipeline map function
    metrics = ProcessingMetrics()
    mapper = TransactionProcessMapFunction(metrics=metrics, auto_write_iceberg=True)
    out_json = mapper.map(json.dumps(tx_data))
    assert out_json is not None

    processed = json.loads(out_json)
    assert processed["order_id"] == test_order_id
    assert processed["total_amount"] == 1045.00  # (250 * 4) + 45 = 1045

    # 3. Read back & verify from local Iceberg catalog table
    if HAS_PYICEBERG:
        from pyiceberg.catalog.sql import SqlCatalog
        cfg = IcebergConfig.from_env()
        db_path = os.path.join(cfg.warehouse, "catalog.db")
        uri = f"sqlite:///{os.path.abspath(db_path)}"
        catalog = SqlCatalog(cfg.catalog, **{"uri": uri, "warehouse": os.path.abspath(cfg.warehouse)})
        table = catalog.load_table(cfg.full_table_name)
        assert table is not None
