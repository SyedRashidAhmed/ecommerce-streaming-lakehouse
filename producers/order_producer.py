import uuid
import random

from datetime import datetime


def generate_order():

    quantity = random.randint(1, 5)

    unit_price = round(
        random.uniform(10, 500),
        2
    )

    return {

        "event_id": str(uuid.uuid4()),

        "event_type": "order_created",

        "event_time": datetime.utcnow().isoformat(),

        "order_id": f"ord_{random.randint(10000,99999)}",

        "customer_id": f"cust_{random.randint(1000,9999)}",

        "product_id": f"prod_{random.randint(1,100)}",

        "quantity": quantity,

        "unit_price": unit_price,

        "total_amount": quantity * unit_price

    }