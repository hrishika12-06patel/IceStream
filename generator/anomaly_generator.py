import argparse
import json
import random
import sys
from typing import Dict, Any, List, Optional, Tuple, Generator

# Support running directly or as module
try:
    from generator.transaction_generator import (
        generate_transaction,
        PRODUCTS,
        PAYMENT_METHODS,
    )
except ImportError:
    from transaction_generator import (
        generate_transaction,
        PRODUCTS,
        PAYMENT_METHODS,
    )


ANOMALY_TYPES = [
    "null",
    "schema",
    "negative-price",
    "invalid-numeric",
    "invalid-enum",
    "duplicate-id",
    "all",
]

NULLABLE_FIELDS = [
    "customer_id",
    "product_id",
    "payment_method",
    "price",
    "quantity",
    "tax_amount",
    "timestamp",
]

INVALID_PAYMENT_METHODS = ["UnknownMethod", "Crypto", "Bitcoin", "InvalidPay"]


def inject_null_anomaly(tx: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
    """
    Deliberately replaces one or more required transaction fields with None (null).
    """
    corrupted = dict(tx)
    field = random.choice(NULLABLE_FIELDS)
    corrupted[field] = None
    return corrupted, f"null_{field}"


def inject_schema_anomaly(tx: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
    """
    Deliberately alters the structure/schema of the transaction:
    - Renames expected field
    - Adds unexpected extra field
    - Removes expected field
    - Alters data type of field
    """
    corrupted = dict(tx)
    schema_change = random.choice(
        ["rename_field", "add_field", "remove_field", "change_type"]
    )

    if schema_change == "rename_field":
        field_to_rename = random.choice(["customer_id", "product_id"])
        new_name = "customer" if field_to_rename == "customer_id" else "product"
        val = corrupted.pop(field_to_rename)
        corrupted[new_name] = val
        detail = f"schema_rename_{field_to_rename}_to_{new_name}"

    elif schema_change == "add_field":
        corrupted["unexpected_field"] = "extra_unstructured_data"
        detail = "schema_add_unexpected_field"

    elif schema_change == "remove_field":
        field_to_remove = random.choice(["timestamp", "payment_method", "customer_id"])
        corrupted.pop(field_to_remove, None)
        detail = f"schema_remove_{field_to_remove}"

    else:  # change_type
        if "price" in corrupted and isinstance(corrupted["price"], (int, float)):
            corrupted["price"] = str(corrupted["price"])
            detail = "schema_type_price_to_string"
        elif "quantity" in corrupted and isinstance(corrupted["quantity"], int):
            corrupted["quantity"] = str(corrupted["quantity"])
            detail = "schema_type_quantity_to_string"
        else:
            corrupted["unexpected_extra"] = 123
            detail = "schema_type_extra"

    return corrupted, detail


def inject_invalid_numeric_anomaly(tx: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
    """
    Injects invalid numeric values such as negative price, negative quantity, or negative tax.
    """
    corrupted = dict(tx)
    numeric_field = random.choice(["price", "quantity", "tax_amount"])

    if numeric_field == "price":
        corrupted["price"] = -round(random.uniform(10.0, 500.0), 2)
        detail = "invalid_numeric_negative_price"
    elif numeric_field == "quantity":
        corrupted["quantity"] = -random.randint(1, 5)
        detail = "invalid_numeric_negative_quantity"
    else:
        corrupted["tax_amount"] = -round(random.uniform(5.0, 50.0), 2)
        detail = "invalid_numeric_negative_tax"

    return corrupted, detail


def inject_invalid_enum_anomaly(tx: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
    """
    Injects values outside the expected categorical / enum set.
    """
    corrupted = dict(tx)
    target = random.choice(["payment_method", "product_id"])

    if target == "payment_method":
        corrupted["payment_method"] = random.choice(INVALID_PAYMENT_METHODS)
        detail = f"invalid_enum_payment_{corrupted['payment_method']}"
    else:
        corrupted["product_id"] = "P999_INVALID"
        detail = "invalid_enum_product_P999_INVALID"

    return corrupted, detail


def inject_duplicate_id_anomaly(
    tx: Dict[str, Any], generated_ids: List[int]
) -> Tuple[Dict[str, Any], str]:
    """
    Deliberately reuses an existing order ID to simulate duplicate transactions.
    """
    corrupted = dict(tx)
    if generated_ids:
        duplicate_id = random.choice(generated_ids)
        corrupted["order_id"] = duplicate_id
        detail = f"duplicate_order_id_{duplicate_id}"
    else:
        corrupted["order_id"] = tx["order_id"] - 1
        detail = f"duplicate_order_id_{corrupted['order_id']}"

    return corrupted, detail


def apply_anomaly(
    tx: Dict[str, Any], anomaly_type: str, generated_ids: List[int]
) -> Tuple[Dict[str, Any], str, str]:
    """
    Applies the specified anomaly transformation to a transaction dictionary.

    Returns:
        Tuple of (anomalous_tx, category, detail)
    """
    selected_type = anomaly_type
    if selected_type == "all":
        selected_type = random.choice(
            [
                "null",
                "schema",
                "negative-price",
                "invalid-numeric",
                "invalid-enum",
                "duplicate-id",
            ]
        )

    if selected_type == "null":
        corrupted, detail = inject_null_anomaly(tx)
        cat = "null"
    elif selected_type == "schema":
        corrupted, detail = inject_schema_anomaly(tx)
        cat = "schema"
    elif selected_type == "negative-price":
        corrupted = dict(tx)
        corrupted["price"] = -round(random.uniform(10.0, 500.0), 2)
        cat = "negative-price"
        detail = "negative_price"
    elif selected_type == "invalid-numeric":
        corrupted, detail = inject_invalid_numeric_anomaly(tx)
        cat = "invalid-numeric"
    elif selected_type == "invalid-enum":
        corrupted, detail = inject_invalid_enum_anomaly(tx)
        cat = "invalid-enum"
    elif selected_type == "duplicate-id":
        corrupted, detail = inject_duplicate_id_anomaly(tx, generated_ids)
        cat = "duplicate-id"
    else:
        corrupted, detail = inject_null_anomaly(tx)
        cat = "null"

    return corrupted, cat, detail


def generate_anomalous_transactions(
    count: int,
    anomaly_type: str,
    anomaly_rate: float = 0.05,
    anomaly_count: Optional[int] = None,
    seed: Optional[int] = None,
    start_order_id: int = 10001,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Generates count transactions where a controlled subset are injected with anomalies.

    Args:
        count: Total number of transactions.
        anomaly_type: Target anomaly category or 'all'.
        anomaly_rate: Target fraction of anomalous transactions (e.g. 0.05 = 5%).
        anomaly_count: Explicit count of anomalous transactions (overrides rate if set).
        seed: Random seed for reproducible generation.
        start_order_id: Starting Order ID.

    Returns:
        Tuple of (list_of_transactions, summary_statistics)
    """
    if count <= 0:
        raise ValueError("Count must be greater than 0.")
    if anomaly_type not in ANOMALY_TYPES:
        raise ValueError(
            f"Invalid anomaly type '{anomaly_type}'. Allowed: {ANOMALY_TYPES}"
        )
    if anomaly_rate < 0.0 or anomaly_rate > 1.0:
        raise ValueError("Anomaly rate must be between 0.0 and 1.0.")
    if anomaly_count is not None and (anomaly_count < 0 or anomaly_count > count):
        raise ValueError("Anomaly count must be between 0 and total count.")

    if seed is not None:
        random.seed(seed)

    anomalous_indices = set()
    if anomaly_count is not None:
        anomalous_indices = set(random.sample(range(count), anomaly_count))
    else:
        for i in range(count):
            if random.random() < anomaly_rate:
                anomalous_indices.add(i)

    transactions: List[Dict[str, Any]] = []
    generated_ids: List[int] = []
    anomaly_breakdown: Dict[str, int] = {}
    actual_anomalous_count = 0

    for i in range(count):
        order_id = start_order_id + i
        valid_tx = generate_transaction(order_id)

        if i in anomalous_indices:
            corrupted_tx, cat, _detail = apply_anomaly(
                valid_tx, anomaly_type, generated_ids
            )
            transactions.append(corrupted_tx)
            actual_anomalous_count += 1
            anomaly_breakdown[cat] = anomaly_breakdown.get(cat, 0) + 1
        else:
            transactions.append(valid_tx)

        generated_ids.append(order_id)

    achieved_rate = (actual_anomalous_count / count) * 100.0 if count > 0 else 0.0

    summary = {
        "total_transactions": count,
        "anomalous_transactions": actual_anomalous_count,
        "achieved_rate_pct": achieved_rate,
        "target_anomaly_type": anomaly_type,
        "anomaly_breakdown": anomaly_breakdown,
        "seed": seed,
    }

    return transactions, summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Controlled Data Anomaly Injector for IceStream Transactions"
    )
    parser.add_argument(
        "-a",
        "--anomaly",
        type=str,
        required=True,
        choices=ANOMALY_TYPES,
        help="Type of anomaly to inject: null, schema, negative-price, invalid-numeric, invalid-enum, duplicate-id, or all",
    )
    parser.add_argument(
        "-c",
        "--count",
        type=int,
        default=1000,
        help="Total number of transactions to generate (default: 1000)",
    )
    parser.add_argument(
        "-r",
        "--anomaly-rate",
        type=float,
        default=0.05,
        help="Fraction of transactions to inject with anomalies (default: 0.05 for 5%%)",
    )
    parser.add_argument(
        "--anomaly-count",
        type=int,
        default=None,
        help="Explicit number of anomalous transactions (overrides --anomaly-rate)",
    )
    parser.add_argument(
        "-s",
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducible anomaly generation",
    )
    parser.add_argument(
        "--start-order-id",
        type=int,
        default=10001,
        help="Starting Order ID (default: 10001)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print all generated transactions as JSON",
    )

    args = parser.parse_args()

    # Validation
    if args.count <= 0:
        parser.error("Count must be greater than 0.")
    if args.anomaly_rate < 0.0 or args.anomaly_rate > 1.0:
        parser.error("Anomaly rate must be between 0.0 and 1.0.")
    if args.anomaly_count is not None and (
        args.anomaly_count < 0 or args.anomaly_count > args.count
    ):
        parser.error("Anomaly count must be between 0 and total count.")

    transactions, summary = generate_anomalous_transactions(
        count=args.count,
        anomaly_type=args.anomaly,
        anomaly_rate=args.anomaly_rate,
        anomaly_count=args.anomaly_count,
        seed=args.seed,
        start_order_id=args.start_order_id,
    )

    if args.verbose:
        print(json.dumps(transactions, indent=2))
        print()

    # Display concise summary
    print(f"Generated transactions: {summary['total_transactions']:,}")
    print(f"Anomalous transactions: {summary['anomalous_transactions']:,}")
    print(f"Anomaly type: {summary['target_anomaly_type']}")
    if args.anomaly_count is not None:
        print(f"Explicit anomaly count: {args.anomaly_count}")
    else:
        print(
            f"Anomaly rate: {summary['achieved_rate_pct']:.1f}% (target: {args.anomaly_rate * 100:.1f}%)"
        )

    if summary["target_anomaly_type"] == "all" and summary["anomaly_breakdown"]:
        print("Anomaly type breakdown:")
        for cat, num in summary["anomaly_breakdown"].items():
            print(f"  - {cat}: {num}")

    if summary["seed"] is not None:
        print(f"Random seed: {summary['seed']}")


if __name__ == "__main__":
    main()
