import uuid
import random

from datetime import datetime

from common.customer_registry import REGISTERED_CUSTOMERS
from common.product_registry import PRODUCTS

# ==========================================================
# ORDER SEQUENCE
# ==========================================================

ORDER_SEQUENCE = 100000

# ==========================================================
# VALID ORDER
# ==========================================================

def generate_valid_order():

    global ORDER_SEQUENCE

    if len(REGISTERED_CUSTOMERS) == 0:
        return None

    customer_id = random.choice(
        REGISTERED_CUSTOMERS
    )

    product = random.choice(
        PRODUCTS
    )

    quantity = random.randint(
        1,
        5
    )

    unit_price = product["unit_price"]

    payload = {

        "event_id": str(uuid.uuid4()),

        "event_type": "order_created",

        "event_time": datetime.utcnow().isoformat(),

        "order_id": f"ord_{ORDER_SEQUENCE}",

        "customer_id": customer_id,

        "product_id": product["product_id"],

        "quantity": quantity,

        "unit_price": unit_price,

        "total_amount": round(
            quantity * unit_price,
            2
        )
    }

    ORDER_SEQUENCE += 1

    return payload


# ==========================================================
# INVALID ORDER
# ==========================================================

def generate_invalid_order():

    order = generate_valid_order()

    if order is None:
        return None

    anomaly = random.choice(
        [
            "null_order_id",
            "null_customer_id",
            "null_product_id",
            "negative_quantity",
            "negative_price",
            "wrong_total",
            "bad_event_type"
        ]
    )

    if anomaly == "null_order_id":

        order["order_id"] = None

    elif anomaly == "null_customer_id":

        order["customer_id"] = None

    elif anomaly == "null_product_id":

        order["product_id"] = None

    elif anomaly == "negative_quantity":

        order["quantity"] = -1

        order["total_amount"] = round(
            order["quantity"] * order["unit_price"],
            2
        )

    elif anomaly == "negative_price":

        order["unit_price"] = -100

        order["total_amount"] = round(
            order["quantity"] * order["unit_price"],
            2
        )

    elif anomaly == "wrong_total":

        order["total_amount"] = 999999

    elif anomaly == "bad_event_type":

        order["event_type"] = "invalid_order"

    return order


# ==========================================================
# MAIN
# ==========================================================

def generate_order():

    if len(REGISTERED_CUSTOMERS) == 0:
        return None

    probability = random.random()

    if probability <= 0.90:

        return generate_valid_order()

    return generate_invalid_order()