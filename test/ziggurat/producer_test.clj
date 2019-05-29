(ns ziggurat.producer-test
  (:require [clojure.test :refer :all]
            [flatland.protobuf.core :as proto]
            [ziggurat.streams :refer [start-streams stop-streams]]
            [ziggurat.fixtures :as fix]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.producer :as producer]))

(use-fixtures :once fix/mount-only-config-and-producer)

(deftest properties-map-is-generated-correctly
  (is (= "localhost:9092" (.get (producer/producer-config-properties) "bootstrap.servers")))
  (is (= "all" (.get (producer/producer-config-properties) "acks")))
  (is (= 5 (.get (producer/producer-config-properties) "retries")))
  (is (= 5 (.get (producer/producer-config-properties) "max.in.flight.requests.per.connection")))
  (is (= "org.apache.kafka.common.serialization.StringSerializer" (.get (producer/producer-config-properties) "key.serializer")))
  (is (= "org.apache.kafka.common.serialization.StringSerializer" (.get (producer/producer-config-properties) "value.serializer")))
  (is (false? (.get (producer/producer-config-properties) "enable.idempotence"))))

(deftest send-data-with-topic-and-value-test
  (let [topic "test-topic"
        data "Hello World!!!"]
    (let [future (producer/send topic data)]
      (Thread/sleep 1000)
      (is (-> future .get .partition (>= 0))))))


