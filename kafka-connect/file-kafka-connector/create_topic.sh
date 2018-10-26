kafka-topics --create --topic events --partitions 6 --replication-factor 1 --zookeeper localhost:2181 --config compression.type=snappy
 