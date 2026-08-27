import os
from decimal import Decimal
import pytest

from iceberg.iceberg_config import (
    IcebergConfig,
    TRANSACTION_FIELD_DEFINITIONS,
    get_iceberg_schema,
    map_transaction_to_iceberg_record,
    HAS_PYICEBERG,
)


def get_sample_processed_transaction():
    """Returns a valid processed transaction dict including total_amount."""
    return {
        "order_id": 10001,
        "customer_id": "C123",
        "product_id": "P501",
        "quantity": 2,
        "price": 1000.00,
        "tax_amount": 180.00,
        "payment_method": "UPI",
        "timestamp": "2026-08-24T10:00:00+00:00",
        "total_amount": 2180.00,
    }


# 1. Default configuration loads correctly
def test_default_config_loads():
    config = IcebergConfig()
    assert config.catalog == "local"
    assert config.warehouse == "./warehouse"
    assert config.namespace == "icestream"
    assert config.table == "transactions"
    assert config.full_table_name == "icestream.transactions"

    config_dict = config.to_dict()
    assert config_dict["catalog"] == "local"
    assert config_dict["full_table_name"] == "icestream.transactions"


# 2. Environment variables override defaults
def test_env_vars_override_defaults(monkeypatch):
    monkeypatch.setenv("ICEBERG_CATALOG", "custom_sql")
    monkeypatch.setenv("ICEBERG_WAREHOUSE", "/tmp/custom_warehouse")
    monkeypatch.setenv("ICEBERG_NAMESPACE", "analytics")
    monkeypatch.setenv("ICEBERG_TABLE", "orders")

    config = IcebergConfig.from_env()
    assert config.catalog == "custom_sql"
    assert config.warehouse == "/tmp/custom_warehouse"
    assert config.namespace == "analytics"
    assert config.table == "orders"
    assert config.full_table_name == "analytics.orders"


def test_explicit_arguments_override_env_vars(monkeypatch):
    monkeypatch.setenv("ICEBERG_CATALOG", "env_catalog")

    config = IcebergConfig.from_env(
        catalog="arg_catalog",
        warehouse="/arg/warehouse",
        namespace="arg_ns",
        table="arg_tbl",
    )
    assert config.catalog == "arg_catalog"
    assert config.warehouse == "/arg/warehouse"
    assert config.namespace == "arg_ns"
    assert config.table == "arg_tbl"


# 3. Missing/invalid required configuration is handled clearly
@pytest.mark.parametrize(
    "field,invalid_val",
    [
        ("catalog", ""),
        ("warehouse", "   "),
        ("namespace", ""),
        ("table", "   "),
    ],
)
def test_invalid_empty_configuration(field, invalid_val):
    kwargs = {
        "catalog": "local",
        "warehouse": "./warehouse",
        "namespace": "icestream",
        "table": "transactions",
    }
    kwargs[field] = invalid_val

    with pytest.raises(ValueError) as excinfo:
        IcebergConfig.from_env(**kwargs)

    assert "Iceberg configuration error" in str(excinfo.value)


# 4. Namespace/table configuration is valid
def test_namespace_table_configuration():
    config = IcebergConfig(namespace="analytics_prod", table="stream_tx")
    assert config.namespace == "analytics_prod"
    assert config.table == "stream_tx"
    assert config.full_table_name == "analytics_prod.stream_tx"


# 5. Warehouse configuration is valid
def test_warehouse_catalog_properties():
    config = IcebergConfig(catalog="hadoop", warehouse="/var/data/iceberg")
    props = config.get_catalog_properties()
    assert props["type"] == "hadoop"
    assert os.path.isabs(props["warehouse"])
    assert props["warehouse"].endswith(os.path.normpath("/var/data/iceberg"))


# 6. Iceberg table schema contains all required fields & 7. total_amount exists & 8. Numeric types & 9. Timestamp
def test_schema_field_definitions():
    expected_fields = [
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "price",
        "tax_amount",
        "payment_method",
        "timestamp",
        "total_amount",
    ]

    schema = get_iceberg_schema()

    if HAS_PYICEBERG:
        field_names = [field.name for field in schema.fields]
        assert field_names == expected_fields
        assert "total_amount" in field_names

        # Validate PyIceberg types
        order_field = schema.find_field("order_id")
        price_field = schema.find_field("price")
        tax_field = schema.find_field("tax_amount")
        total_field = schema.find_field("total_amount")
        ts_field = schema.find_field("timestamp")

        assert str(order_field.field_type) == "string"
        assert "decimal(10, 2)" in str(price_field.field_type)
        assert "decimal(10, 2)" in str(tax_field.field_type)
        assert "decimal(12, 2)" in str(total_field.field_type)
        assert "timestamptz" in str(ts_field.field_type)
    else:
        field_names = [f["name"] for f in schema]
        assert field_names == expected_fields
        assert "total_amount" in field_names


# 10. No secrets exposed in configuration errors
def test_no_secrets_in_config_errors():
    secret_value = "SECRET_DB_PASSWORD_12345"
    os.environ["SECRET_PASSWORD"] = secret_value

    try:
        config = IcebergConfig(catalog="", warehouse="./warehouse")
        with pytest.raises(ValueError) as excinfo:
            config.validate()
        err_msg = str(excinfo.value)
        assert secret_value not in err_msg
        assert "SECRET_PASSWORD" not in err_msg
    finally:
        os.environ.pop("SECRET_PASSWORD", None)


# 11. Transaction record mapping test
def test_map_transaction_to_iceberg_record_valid():
    tx = get_sample_processed_transaction()
    record = map_transaction_to_iceberg_record(tx)

    assert record["order_id"] == "10001"
    assert record["customer_id"] == "C123"
    assert record["product_id"] == "P501"
    assert record["quantity"] == 2
    assert record["price"] == Decimal("1000.00")
    assert record["tax_amount"] == Decimal("180.00")
    assert record["payment_method"] == "UPI"
    assert record["timestamp"] == "2026-08-24T10:00:00+00:00"
    assert record["total_amount"] == Decimal("2180.00")


def test_map_transaction_to_iceberg_record_missing_field():
    tx = get_sample_processed_transaction()
    del tx["total_amount"]

    with pytest.raises(ValueError) as excinfo:
        map_transaction_to_iceberg_record(tx)

    assert "missing or NULL required fields" in str(excinfo.value)
    assert "total_amount" in str(excinfo.value)
