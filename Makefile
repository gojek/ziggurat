KAFKA_TOPICS = topic1 topic2
KAFKA_BROKERS = kafka1:9095 kafka2:9096 kafka3:9097
ADMIN_CONFIG = /etc/kafka/secrets/config-admin.properties
KAFKA_CONTAINER = ziggurat-kafka1-1

.PHONY: setup-cluster create-scram-credentials create-topics setup-acls down up clean restart

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
	@docker exec $(KAFKA_CONTAINER) kafka-configs --bootstrap-server kafka1:9095 \
		--alter \
		--add-config 'SCRAM-SHA-256=[password=admin]' \
		--entity-type users \
		--entity-name admin

# Create all required topics
create-topics:
	@for topic in $(KAFKA_TOPICS); do \
		echo "Creating topic: $$topic"; \
		docker exec $(KAFKA_CONTAINER) kafka-topics --bootstrap-server kafka1:9095 \
			--create \
			--if-not-exists \
			--topic $$topic \
			--partitions 3 \
			--replication-factor 3; \
	done

# Setup ACLs for admin user
setup-acls:
	@for topic in $(KAFKA_TOPICS); do \
		echo "Setting up ACLs for topic: $$topic"; \
		docker exec $(KAFKA_CONTAINER) kafka-acls --bootstrap-server kafka1:9095 \
			--add \
			--allow-principal User:admin \
			--operation All \
			--topic $$topic; \
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
