** LAUNCH ZOOKEEPER **
./bin/zookeeper-server-start.sh ./config/zookeeper.properties

** LAUNCH KAFKA SERVER **
./bin/kafka-server-start.sh ./config/server.properties

** CREATE TOPIC: "rates" **
./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic rates

** INSTALL PYTHON DEPENDENCY **
pip install kafka-python
