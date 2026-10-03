#!/bin/sh
bin/kafka-topics.sh --zookeeper zk:2181 --create --topic orders
bin/kafka-topics.sh --bootstrap-server "b1:9092 b2:9092" --list
bin/kafka-run-class.sh kafka.tools.JmxTool --object-name 'kafka.server:*'
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/kraft/server.properties
