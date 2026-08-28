"""
IceStream Apache Iceberg Storage Layer Configuration.

Centralizes configuration, schema definitions, catalog properties,
and data mapping logic for Apache Iceberg integration with IceStream.
"""

import os
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple, Union

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        DecimalType,
        IntegerType,
        NestedField,
        StringType,
        TimestamptzType,
    )
    HAS_PYICEBERG = True
except ImportError:
    HAS_PYICEBERG = False


# Logical field definitions for Iceberg transaction table
TRANSACTION_FIELD_DEFINITIONS: List[Dict[str, Any]] = [
    {"id": 1, "name": "order_id", "type": "string", "required": True, "doc": "Unique order identifier"},
    {"id": 2, "name": "customer_id", "type": "string", "required": True, "doc": "Customer identifier"},
    {"id": 3, "name": "product_id", "type": "string", "required": True, "doc": "Product identifier"},
    {"id": 4, "name": "quantity", "type": "int", "required": True, "doc": "Purchased product quantity"},
    {"id": 5, "name": "price", "type": "decimal(10, 2)", "required": True, "doc": "Unit price of product"},
    {"id": 6, "name": "tax_amount", "type": "decimal(10, 2)", "required": True, "doc": "Tax amount for order"},
    {"id": 7, "name": "payment_method", "type": "string", "required": True, "doc": "Payment method used"},
    {"id": 8, "name": "timestamp", "type": "timestamptz", "required": True, "doc": "Transaction UTC timestamp"},
    {"id": 9, "name": "total_amount", "type": "decimal(12, 2)", "required": True, "doc": "Flink-calculated total order amount"},
]


@dataclass
class IcebergConfig:
    """
    Centralized Apache Iceberg Configuration.

    Attributes:
        catalog: Catalog type or identifier (default: "local").
        warehouse: Warehouse file path or URI (default: "./warehouse").
        namespace: Database/namespace name (default: "icestream").
        table: Iceberg table name (default: "transactions").
    """

    catalog: str = "local"
    warehouse: str = "./warehouse"
    namespace: str = "icestream"
    table: str = "transactions"

    @classmethod
    def from_env(
        cls,
        catalog: Optional[str] = None,
        warehouse: Optional[str] = None,
        namespace: Optional[str] = None,
        table: Optional[str] = None,
    ) -> "IcebergConfig":
        """
        Loads Iceberg configuration from environment variables with optional parameter overrides.

        Env Vars:
            ICEBERG_CATALOG
            ICEBERG_WAREHOUSE
            ICEBERG_NAMESPACE
            ICEBERG_TABLE
        """
        env_catalog = os.getenv("ICEBERG_CATALOG", "local")
        env_warehouse = os.getenv("ICEBERG_WAREHOUSE", "./warehouse")
        env_namespace = os.getenv("ICEBERG_NAMESPACE", "icestream")
        env_table = os.getenv("ICEBERG_TABLE", "transactions")

        config = cls(
            catalog=str(catalog if catalog is not None else env_catalog).strip(),
            warehouse=str(warehouse if warehouse is not None else env_warehouse).strip(),
            namespace=str(namespace if namespace is not None else env_namespace).strip(),
            table=str(table if table is not None else env_table).strip(),
        )
        config.validate()
        return config

    def validate(self) -> None:
        """
        Validates configuration fields. Raises ValueError if mandatory settings are empty.
        Ensures error messages do not expose sensitive values.
        """
        if not self.catalog:
            raise ValueError("Iceberg configuration error: ICEBERG_CATALOG cannot be empty.")
        if not self.warehouse:
            raise ValueError("Iceberg configuration error: ICEBERG_WAREHOUSE cannot be empty.")
        if not self.namespace:
            raise ValueError("Iceberg configuration error: ICEBERG_NAMESPACE cannot be empty.")
        if not self.table:
            raise ValueError("Iceberg configuration error: ICEBERG_TABLE cannot be empty.")

    @property
    def full_table_name(self) -> str:
        """Returns fully qualified table identifier (namespace.table)."""
        return f"{self.namespace}.{self.table}"

    def to_dict(self) -> Dict[str, str]:
        """Converts configuration to a dictionary representation."""
        return {
            "catalog": self.catalog,
            "warehouse": self.warehouse,
            "namespace": self.namespace,
            "table": self.table,
            "full_table_name": self.full_table_name,
        }

    def get_catalog_properties(self) -> Dict[str, str]:
        """
        Returns catalog properties suitable for PyIceberg or Flink catalog initialization.
        """
        return {
            "type": self.catalog,
            "warehouse": os.path.abspath(self.warehouse),
        }


def get_iceberg_schema() -> Any:
    """
    Constructs and returns the PyIceberg Schema for IceStream transactions.

    Returns:
        pyiceberg.schema.Schema object if pyiceberg is available,
        or a list of field dictionaries if pyiceberg is unavailable.
    """
    if not HAS_PYICEBERG:
        return TRANSACTION_FIELD_DEFINITIONS

    return Schema(
        NestedField(field_id=1, name="order_id", field_type=StringType(), required=True, doc="Unique order identifier"),
        NestedField(field_id=2, name="customer_id", field_type=StringType(), required=True, doc="Customer identifier"),
        NestedField(field_id=3, name="product_id", field_type=StringType(), required=True, doc="Product identifier"),
        NestedField(field_id=4, name="quantity", field_type=IntegerType(), required=True, doc="Purchased product quantity"),
        NestedField(field_id=5, name="price", field_type=DecimalType(10, 2), required=True, doc="Unit price of product"),
        NestedField(field_id=6, name="tax_amount", field_type=DecimalType(10, 2), required=True, doc="Tax amount for order"),
        NestedField(field_id=7, name="payment_method", field_type=StringType(), required=True, doc="Payment method used"),
        NestedField(field_id=8, name="timestamp", field_type=TimestamptzType(), required=True, doc="Transaction UTC timestamp"),
        NestedField(field_id=9, name="total_amount", field_type=DecimalType(12, 2), required=True, doc="Flink-calculated total order amount"),
    )


def map_transaction_to_iceberg_record(transaction: Dict[str, Any]) -> Dict[str, Any]:
    """
    Maps a Flink-processed transaction dictionary into a type-compatible Iceberg record dict.

    Args:
        transaction: Processed transaction dict containing all 8 original fields + total_amount.

    Returns:
        Mapped record dict with numeric/decimal types properly formatted for Iceberg storage.
    """
    required_keys = [
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

    missing = [k for k in required_keys if k not in transaction or transaction[k] is None]
    if missing:
        raise ValueError(f"Cannot map transaction to Iceberg: missing or NULL required fields: {missing}")

    return {
        "order_id": str(transaction["order_id"]),
        "customer_id": str(transaction["customer_id"]),
        "product_id": str(transaction["product_id"]),
        "quantity": int(transaction["quantity"]),
        "price": Decimal(str(transaction["price"])).quantize(Decimal("0.01")),
        "tax_amount": Decimal(str(transaction["tax_amount"])).quantize(Decimal("0.01")),
        "payment_method": str(transaction["payment_method"]),
        "timestamp": str(transaction["timestamp"]),
        "total_amount": Decimal(str(transaction["total_amount"])).quantize(Decimal("0.01")),
    }
