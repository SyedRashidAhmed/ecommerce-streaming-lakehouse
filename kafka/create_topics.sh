#!/bin/bash

kafka-topics --create --topic customer_events --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
kafka-topics --create --topic order_events  --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
kafka-topics --create --topic dead_letter_events  --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
