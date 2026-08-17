import random
import json
from datetime import datetime, timezone


PRODUCTS = [
    "P501",
    "P502",
    "P503",
    "P504",
    "P505",
    "P506",
    "P507",
    "P508",
    "P509",
    "P510"
]

PAYMENT_METHODS = [
    "Credit Card",
    "Debit Card",
    "UPI",
    "Net Banking"
]


def generate_transaction(order_id):
    quantity = random.randint(1, 5)
    price = round(random.uniform(100, 5000), 2)
    tax_amount = round(price * quantity * 0.18, 2)

    transaction = {
        "order_id": order_id,
        "customer_id": f"C{random.randint(101, 999)}",
        "product_id": random.choice(PRODUCTS),
        "quantity": quantity,
        "price": price,
        "tax_amount": tax_amount,
        "payment_method": random.choice(PAYMENT_METHODS),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    return transaction


if __name__ == "__main__":
    transactions = []

    for order_id in range(10001, 10011):
        transaction = generate_transaction(order_id)
        transactions.append(transaction)

    print(json.dumps(transactions, indent=2))