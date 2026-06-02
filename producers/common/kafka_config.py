import json

from kafka import KafkaProducer

from common.constants import KAFKA_BROKER


def get_producer():

    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=50,
        compression_type="snappy"
    )