#!/bin/sh
bin/kafka-topics.sh --bootstrap-server b1:9092,b2:9092 --create --topic orders
bin/kafka-features.sh --bootstrap-server b1:9092 describe
bin/kafka-server-start.sh config/server.properties
bin/kafka-console-producer.sh --bootstrap-server b1:9092 --topic orders --command-config client.properties
