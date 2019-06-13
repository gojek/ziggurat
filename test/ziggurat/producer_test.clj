(ns ziggurat.producer-test
  (:require [clojure.test :refer :all]
            [ziggurat.streams :refer [start-streams stop-streams]]
            [ziggurat.fixtures :as fix :refer [*producer-properties* *consumer-properties* *embedded-kafka-cluster*]]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.producer :refer [producer-properties-map send kafka-producers]]
            [clojure.test.check.generators :as gen])
  (:import (org.apache.kafka.streams.integration.utils IntegrationTestUtils)
           (org.apache.kafka.clients.producer KafkaProducer)))

(use-fixtures :once fix/mount-only-config-and-producer)

(defn stream-router-config-without-producer [])
(:stream-router {:default {:application-id       "test"
                           :bootstrap-servers    "localhost:9092"
                           :stream-threads-count [1 :int]
                           :origin-topic         "topic"
                           :proto-class          "flatland.protobuf.test.Example$Photo"
                           :channels             {:channel-1 {:worker-count [10 :int]
                                                              :retry        {:count   [5 :int]
                                                                             :enabled [true :bool]}}}}})

(deftest send-data-with-topic-and-value-test
  (with-redefs
   [kafka-producers (hash-map :default (KafkaProducer. *producer-properties*))]
    (let [topic (gen/generate gen/string-alphanumeric)
          key "message"
          value "Hello World!!"]
      (.createTopic *embedded-kafka-cluster* topic)
      (send :default topic key value)
      (let [result (IntegrationTestUtils/waitUntilMinKeyValueRecordsReceived *consumer-properties* topic  1 1000)]
        (is (= value (.value (first result))))))))

(deftest send-data-with-topic-key-partition-and-value-test
  (with-redefs
   [kafka-producers (hash-map :default (KafkaProducer. *producer-properties*))]
    (let [topic (gen/generate gen/string-alphanumeric)
          key "message"
          value "Hello World!!"
          partition (int 0)]
      (.createTopic *embedded-kafka-cluster* topic)
      (send :default topic partition key value)
      (let [result (IntegrationTestUtils/waitUntilMinKeyValueRecordsReceived *consumer-properties* topic  1 1000)]
        (is (= value (.value (first result))))))))

(deftest send-throws-exception-when-no-producers-are-configured
  (with-redefs
   [kafka-producers {}]
    (let [topic "test-topic"
          key "message"
          value "Hello World!! from non-existant Kafka Producers"]
      (is (not-empty (try (send :default topic key value)
                          (catch Exception e (ex-data e))))))))

(deftest producer-properties-map-is-empty-if-no-producers-configured
  ; Here ziggurat-config has been substituted with a custom map which
  ; does not have any valid producer configs.
  (with-redefs
   [ziggurat-config stream-router-config-without-producer]
    (is (empty? (producer-properties-map)))))

(deftest producer-properties-map-is-not-empty-if-producers-are-configured
  ; Here the config is read from config.test.edn which contains
  ; valid producer configs.
  (is (seq (producer-properties-map))))



