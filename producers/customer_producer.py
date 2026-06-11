import uuid
import random

from datetime import datetime
from faker import Faker

from common.customer_registry import REGISTERED_CUSTOMERS

fake = Faker()

# ==========================================================
# CUSTOMER MASTER DATA
# ==========================================================

CUSTOMERS = []

for i in range(1000, 10000):

    CUSTOMERS.append(
        {
            "customer_id": f"cust_{i}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "country": fake.country(),
            "city": fake.city()
        }
    )

AVAILABLE_CUSTOMERS = CUSTOMERS.copy()

# ==========================================================
# VALID CUSTOMER
# ==========================================================

def generate_valid_customer():

    if len(AVAILABLE_CUSTOMERS) == 0:
        return None

    customer = random.choice(
        AVAILABLE_CUSTOMERS
    )

    AVAILABLE_CUSTOMERS.remove(
        customer
    )

    REGISTERED_CUSTOMERS.append(
        customer["customer_id"]
    )

    return {

        "event_id": str(uuid.uuid4()),

        "event_type": "customer_registered",

        "event_time": datetime.utcnow().isoformat(),

        "customer_id": customer["customer_id"],

        "first_name": customer["first_name"],

        "last_name": customer["last_name"],

        "email": customer["email"],

        "country": customer["country"],

        "city": customer["city"]
    }

# ==========================================================
# INVALID CUSTOMER
# ==========================================================

def generate_invalid_customer():

    customer = random.choice(
        CUSTOMERS
    )

    payload = {

        "event_id": str(uuid.uuid4()),

        "event_type": "customer_registered",

        "event_time": datetime.utcnow().isoformat(),

        "customer_id": customer["customer_id"],

        "first_name": customer["first_name"],

        "last_name": customer["last_name"],

        "email": customer["email"],

        "country": customer["country"],

        "city": customer["city"]
    }

    anomaly = random.choice(
        [
            "null_customer_id",
            "null_email",
            "bad_email",
            "null_first_name",
            "null_last_name",
            "null_country",
            "null_city",
            "duplicate_customer"
        ]
    )

    if anomaly == "null_customer_id":

        payload["customer_id"] = None

    elif anomaly == "null_email":

        payload["email"] = None

    elif anomaly == "bad_email":

        payload["email"] = "invalid_email"

    elif anomaly == "null_first_name":

        payload["first_name"] = None

    elif anomaly == "null_last_name":

        payload["last_name"] = None

    elif anomaly == "null_country":

        payload["country"] = None

    elif anomaly == "null_city":

        payload["city"] = None

    return payload

# ==========================================================
# MAIN
# ==========================================================

def generate_customer():

    probability = random.random()

    if probability <= 0.90:

        customer = generate_valid_customer()

        if customer is not None:
            return customer

    return generate_invalid_customer()