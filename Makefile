KAFKA_TOPICS = topic another-test-topic
KAFKA_BROKERS = kafka1:9095 kafka2:9096 kafka3:9097
ADMIN_CONFIG = /etc/kafka/secrets/config-admin.properties
KAFKA_CONTAINER = ziggurat_kafka1_1

.PHONY: all

# Main target to setup the entire cluster
setup-cluster: down up wait-for-kafka create-scram-credentials create-topics setup-acls

# Bring down all containers and clean volumes
down:
	@echo "Bringing down all containers..."
	docker-compose -f docker-compose-cluster.yml down -v

# Start all containers
up:
	@echo "Starting all containers..."
	docker-compose -f docker-compose-cluster.yml up -d

# Wait for Kafka to be ready
wait-for-kafka:
	@echo "Waiting for Kafka to be ready..."
	@sleep 30

# Restart everything
restart: down up wait-for-kafka

# Create SCRAM credentials for admin user
create-scram-credentials:
	@echo "Creating SCRAM credentials for admin user..."
	@docker exec $(KAFKA_CONTAINER) kafka-configs \
		--alter \
		--zookeeper zookeeper:2181 \
		--add-config 'SCRAM-SHA-256=[password=admin]' \
		--entity-type users \
		--entity-name admin

# Create all required topics
create-topics:
	@for topic in $(KAFKA_TOPICS); do \
		echo "Creating topic: $$topic"; \
		docker exec $(KAFKA_CONTAINER) kafka-topics \
			--create \
			--zookeeper zookeeper:2181 \
			--if-not-exists \
			--topic $$topic \
			--partitions 3 \
			--replication-factor 3; \
	done

# Setup ACLs for admin user on all brokers
setup-acls:
	@for broker in $(KAFKA_BROKERS); do \
		case $$broker in \
			kafka1:9095) \
				container="ziggurat_kafka1_1" ;; \
			kafka2:9096) \
				container="ziggurat_kafka2_1" ;; \
			kafka3:9097) \
				container="ziggurat_kafka3_1" ;; \
		esac; \
		for topic in $(KAFKA_TOPICS); do \
			echo "Setting up ACLs for topic: $$topic on broker: $$broker using container: $$container"; \
			docker exec $$container kafka-acls \
				--bootstrap-server $$broker \
				--command-config $(ADMIN_CONFIG) \
				--add \
				--allow-principal User:admin \
				--operation All \
				--topic $$topic; \
		done \
	done

# Clean up topics (can be used during development)
clean-topics:
	@for topic in $(KAFKA_TOPICS); do \
		echo "Deleting topic: $$topic"; \
		docker exec $(KAFKA_CONTAINER) kafka-topics --bootstrap-server kafka1:9095 \
			--delete \
			--topic $$topic; \
	done

# Show logs
logs:
	docker-compose -f docker-compose-cluster.yml logs -f

test-cluster: setup-cluster
	TESTING_TYPE=cluster lein test
	docker-compose -f docker-compose-cluster.yml down
	rm -rf /tmp/ziggurat_kafka_cluster_data

coverage: setup-cluster
	lein code-coverage
	docker-compose down

proto:
	protoc -I=resources --java_out=test/ resources/proto/example.proto
	protoc -I=resources --java_out=test/ resources/proto/person.proto