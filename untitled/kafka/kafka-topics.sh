#!/bin/bash

# 创建订单主题，设置4个分区，2个副本
bin/kafka-topics.sh --create --topic ride-sharing-orders --bootstrap-server localhost:9092 --partitions 4 --replication-factor 2

# 创建处理后主题，设置4个分区，2个副本
bin/kafka-topics.sh --create --topic processed-orders --bootstrap-server localhost:9092 --partitions 4 --replication-factor 2

# 查看主题配置
bin/kafka-topics.sh --describe --topic ride-sharing-orders --bootstrap-server localhost:9092
bin/kafka-topics.sh --describe --topic processed-orders --bootstrap-server localhost:9092