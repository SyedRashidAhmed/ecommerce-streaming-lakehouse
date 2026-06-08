#!/bin/bash

kafka-topics --create --topic customer_events --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
kafka-topics --create --topic order_events  --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
kafka-topics --create --topic dead_letter_events  --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
kafka-topics --create --topic bronze_raw_customer_events --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
kafka-topics --create --topic bronze_raw_order_events --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
kafka-topics --create --topic silver_customer_events --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
kafka-topics --create --topic silver_order_events --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
kafka-topics --create --topic silver_customer_events_dlq --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
kafka-topics --create --topic silver_order_events_dlq --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092