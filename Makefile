.PHONY: all
all: test-all test

setup:
	lein deps
	docker-compose up -d
	sleep 10
	docker exec -it ziggurat_kafka /opt/bitnami/kafka/bin/kafka-topics.sh --create --topic topic --partitions 3 --replication-factor 1 --zookeeper ziggurat_zookeeper

test-all: setup
	lein test-all
	docker-compose down

test: setup
	lein test
	docker-compose down
