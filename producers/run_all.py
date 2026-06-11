import asyncio

from customer_producer import generate_customer
from order_producer import generate_order
from product_producer import generate_product

from common.kafka_config import get_producer

from common.constants import (
    CUSTOMER_TOPIC,
    ORDER_TOPIC,
    PRODUCT_TOPIC
)


import time

while True:
    try:
        producer = get_producer()
        print("Connected to Kafka")
        break

    except Exception as e:
        print(f"Waiting for Kafka: {e}")
        time.sleep(5)



async def customer_loop():

    while True:

        customer = generate_customer()

        customer_key = (
        customer["customer_id"]
        if customer["customer_id"]
        else "invalid_customer"
        )
        
        producer.send(
            CUSTOMER_TOPIC,
            key=customer_key.encode(),
            value=customer
        )

        producer.flush()

        await asyncio.sleep(2)


async def order_loop():

    while True:

        order = generate_order()

        order_key = (
            order["customer_id"]
            if order["customer_id"]
            else "invalid_order"
        )

        producer.send(
            ORDER_TOPIC,
            key=order_key.encode(),
            value=order
        )

        await asyncio.sleep(1)

async def bootstrap_products():

    while True:

        product = generate_product()

        if product is None:
            break

        producer.send(
            PRODUCT_TOPIC,
            key=product["product_id"].encode(),
            value=product
        )

        await asyncio.sleep(0.05)


async def main():

    await bootstrap_products()

    await asyncio.gather(
        customer_loop(),
        order_loop()
    )


if __name__ == "__main__":
    asyncio.run(main())