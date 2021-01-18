.PHONY: all
all: test

topic="topic"
another_test_topic="another-test-topic"

setup:
	docker-compose down
	lein deps
	docker-compose up -d
	sleep 10
	docker exec ziggurat_kafka /opt/bitnami/kafka/bin/kafka-topics.sh --create --topic $(topic) --partitions 3 --replication-factor 1 --zookeeper ziggurat_zookeeper
	docker exec ziggurat_kafka /opt/bitnami/kafka/bin/kafka-topics.sh --create --topic $(another_test_topic) --partitions 3 --replication-factor 1 --zookeeper ziggurat_zookeeper

test: setup
	ZIGGURAT_STREAM_ROUTER_DEFAULT_ORIGIN_TOPIC=$(topic) lein test
	docker-compose down

coverage: setup
	lein code-coverage
	docker-compose down
