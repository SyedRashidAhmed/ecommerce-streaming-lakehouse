import uuid

from datetime import datetime

from common.product_registry import PRODUCTS

PRODUCT_INDEX = 0

def generate_product():

    global PRODUCT_INDEX

    if PRODUCT_INDEX >= len(PRODUCTS):
        return None

    product = PRODUCTS[PRODUCT_INDEX]

    PRODUCT_INDEX += 1

    return {

        "event_id": str(uuid.uuid4()),

        "event_type": "product_created",

        "event_time": datetime.utcnow().isoformat(),

        "product_id": product["product_id"],

        "product_name": product["product_name"],

        "category": product["category"],

        "subcategory": product["subcategory"],

        "brand": product["brand"],

        "unit_price": product["unit_price"]
    }