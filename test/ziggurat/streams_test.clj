(ns ziggurat.streams-test
  (:require [clojure.test :refer :all]
            [flatland.protobuf.core :as proto]
            [ziggurat.streams :refer [start-streams stop-streams]])
  (:import [com.google.protobuf ByteString]
           [flatland.protobuf.test Example$Photo]
           [java.util Properties]
           [kafka.utils MockTime]
           [org.apache.kafka.clients.producer ProducerConfig]
           [org.apache.kafka.common.serialization BytesSerializer]
           [org.apache.kafka.streams KeyValue]
           [org.apache.kafka.streams.integration.utils EmbeddedKafkaCluster IntegrationTestUtils]))

(def proto-log-type (proto/protodef Example$Photo))

(def config-map {:stream-router {:vehicle {:application-id       "test"
                                           :bootstrap-servers    "localhost:9092"
                                           :stream-threads-count 1
                                           :proto-class          "flatland.protobuf.test.Example$Photo"}}})

(def message {:id   7
              :path "/photos/h2k3j4h9h23"})

(defn create-photo []
  (proto/protobuf-dump proto-log-type message))

(defn mapped-fn [message]
  :success)

(deftest start-streams-test
  (let [message-received-count (atom 0)]
    (with-redefs [mapped-fn (fn [message-from-kafka]
                              (when (= message message-from-kafka)
                                (swap! message-received-count inc))
                              :success)]
      (let [topic "topic"
            cluster (doto (EmbeddedKafkaCluster. 1) (.start))
            bootstrap-serves (.bootstrapServers cluster)
            times 6
            kvs (repeat times (KeyValue/pair (create-photo) (create-photo)))
            props (doto (Properties.)
                    (.put ProducerConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap-serves)
                    (.put ProducerConfig/ACKS_CONFIG "all")
                    (.put ProducerConfig/RETRIES_CONFIG (int 0))
                    (.put ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.ByteArraySerializer")
                    (.put ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.ByteArraySerializer"))
            _ (.createTopic cluster topic)
            streams (start-streams {:vehicle {:handler-fn mapped-fn}} (-> config-map
                                                                          (assoc-in [:stream-router :vehicle :bootstrap-servers] bootstrap-serves)
                                                                          (assoc-in [:stream-router :vehicle :origin-topic] topic)))]
        (Thread/sleep 20000)                                ;;waiting for streams to start
        (IntegrationTestUtils/produceKeyValuesSynchronously topic kvs props (MockTime.))
        (Thread/sleep 10000)                                ;;wating for streams to consume messages
        (stop-streams streams)
        (is (= times @message-received-count))))))
