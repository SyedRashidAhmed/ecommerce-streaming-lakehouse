import uuid
import random

from datetime import datetime

from faker import Faker

fake = Faker()


def generate_customer():

    return {

        "event_id": str(uuid.uuid4()),

        "event_type": "customer_registered",

        "event_time": datetime.utcnow().isoformat(),

        "customer_id": f"cust_{random.randint(1000,9999)}",

        "first_name": fake.first_name(),

        "last_name": fake.last_name(),

        "email": fake.email(),

        "country": fake.country(),

        "city": fake.city()

    }