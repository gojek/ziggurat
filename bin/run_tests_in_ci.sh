#!/usr/bin/env bash

set -ex

lein clean
lein deps
mv -fv resources/config.test.{ci.edn,edn}
docker-compose up -d
sleep 15
docker exec -it ziggurat_kafka /opt/bitnami/kafka/bin/kafka-topics.sh --create --topic topic --partitions 3 --replication-factor 1 --zookeeper ziggurat_zookeeper
lein test-all
docker-compose down