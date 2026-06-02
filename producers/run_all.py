import asyncio

from customer_producer import generate_customer
from order_producer import generate_order

from common.kafka_config import get_producer

from common.constants import (
    CUSTOMER_TOPIC,
    ORDER_TOPIC
)

producer = get_producer()


async def customer_loop():

    while True:

        customer = generate_customer()

        producer.send(
            CUSTOMER_TOPIC,
            key=customer["customer_id"].encode(),
            value=customer
        )

        await asyncio.sleep(2)


async def order_loop():

    while True:

        order = generate_order()

        producer.send(
            ORDER_TOPIC,
            key=order["customer_id"].encode(),
            value=order
        )

        await asyncio.sleep(1)


async def main():

    await asyncio.gather(
        customer_loop(),
        order_loop()
    )


if __name__ == "__main__":
    asyncio.run(main())