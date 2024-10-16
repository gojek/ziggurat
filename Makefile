.PHONY: all
all: test

topic="topic"
another_test_topic="another-test-topic"

setup:
	docker compose down
	lein deps
	docker compose up -d
	sleep 10
	docker exec ziggurat_kafka /opt/bitnami/kafka/bin/kafka-topics.sh --create --topic $(topic) --partitions 3 --replication-factor 1 --zookeeper ziggurat_zookeeper
	docker exec ziggurat_kafka /opt/bitnami/kafka/bin/kafka-topics.sh --create --topic $(another_test_topic) --partitions 3 --replication-factor 1 --zookeeper ziggurat_zookeeper

test: setup
	TESTING_TYPE=local lein test
	docker compose down

setup-cluster:
	rm -rf /tmp/ziggurat_kafka_cluster_data
	docker compose -f docker compose-cluster.yml -p ziggurat down
	lein deps
	docker compose -f docker compose-cluster.yml -p ziggurat up -d
	sleep 30
# 	Sleeping for 30s to allow the cluster to come up
	docker exec ziggurat_kafka1_1 kafka-topics --create --topic $(topic) --partitions 3 --replication-factor 3 --if-not-exists --zookeeper ziggurat_zookeeper_1
	docker exec ziggurat_kafka1_1 kafka-topics --create --topic $(another_test_topic) --partitions 3 --replication-factor 3 --if-not-exists --zookeeper ziggurat_zookeeper_1

test-cluster: setup-cluster
	TESTING_TYPE=cluster lein test
	docker compose -f docker compose-cluster.yml down
	rm -rf /tmp/ziggurat_kafka_cluster_data

coverage: setup
	lein code-coverage
	docker compose down

proto:
	protoc -I=resources --java_out=test/ resources/proto/example.proto
	protoc -I=resources --java_out=test/ resources/proto/person.proto
