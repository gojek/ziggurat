(ns ziggurat.streams-test
  (:require [clojure.test :refer :all]
            [flatland.protobuf.core :as proto]
            [ziggurat.streams :refer [start-streams stop-streams]]
            [ziggurat.fixtures :as fix]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.middleware.default :as default-middleware]
            [ziggurat.middleware.json :as json-middleware]
            [ziggurat.tracer :refer [tracer]])
  (:import [flatland.protobuf.test Example$Photo]
           [java.util Properties]
           [kafka.utils MockTime]
           [org.apache.kafka.clients.producer ProducerConfig]
           [org.apache.kafka.streams KeyValue]
           [org.apache.kafka.streams.integration.utils IntegrationTestUtils]
           [io.opentracing.tag Tags]))

(use-fixtures :once fix/mount-config-with-tracer)

(defn props []
  (doto (Properties.)
    (.put ProducerConfig/BOOTSTRAP_SERVERS_CONFIG (get-in (ziggurat-config) [:stream-router :default :bootstrap-servers]))
    (.put ProducerConfig/ACKS_CONFIG "all")
    (.put ProducerConfig/RETRIES_CONFIG (int 0))
    (.put ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.ByteArraySerializer")
    (.put ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.ByteArraySerializer")))

(defn props-with-string-serializer []
  (doto (props)
    (.put ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer")
    (.put ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer")))

(def message {:id   7
              :path "/photos/h2k3j4h9h23"})

(def proto-class Example$Photo)

(defn create-photo []
  (proto/protobuf-dump (proto/protodef proto-class) message))

(def message-key-value (KeyValue/pair (create-photo) (create-photo)))

(def json-message {:foo "bar"})

(def string-key-value-message (KeyValue/pair "key" "{\"foo\":\"bar\"}"))

(defn mapped-fn [_]
  :success)

(defn rand-application-id []
  (str "test" "-" (rand-int 999999999)))

(deftest start-streams-with-since-test
  (let [message-received-count (atom 0)]
    (with-redefs [mapped-fn (fn [message-from-kafka]
                              (when (= message message-from-kafka)
                                (swap! message-received-count inc))
                              :success)]
      (let [times 6
            oldest-processed-message-in-s 10
            changelog-topic-replication-factor 1
            kvs (repeat times message-key-value)
            handler-fn (default-middleware/protobuf->hash mapped-fn proto-class :default)
            streams (start-streams {:default {:handler-fn handler-fn}}
                                   (-> (ziggurat-config)
                                       (assoc-in [:stream-router :default :application-id] (rand-application-id))
                                       (assoc-in [:stream-router :default :oldest-processed-message-in-s] oldest-processed-message-in-s)
                                       (assoc-in [:stream-router :default :changelog-topic-replication-factor] changelog-topic-replication-factor)))]
        (Thread/sleep 10000)                                ;;waiting for streams to start
        (IntegrationTestUtils/produceKeyValuesSynchronously (get-in (ziggurat-config) [:stream-router :default :origin-topic])
                                                            kvs
                                                            (props)
                                                            (MockTime. (- (System/currentTimeMillis) (* 1000 oldest-processed-message-in-s)) (System/nanoTime)))
        (Thread/sleep 5000)                                 ;;wating for streams to consume messages
        (stop-streams streams)
        (is (= 0 @message-received-count))))))

(deftest start-streams-test
  (let [message-received-count (atom 0)]
    (with-redefs [mapped-fn (fn [message-from-kafka]
                              (when (= message message-from-kafka)
                                (swap! message-received-count inc))
                              :success)]
      (let [times 6
            changelog-topic-replication-factor 1
            kvs (repeat times message-key-value)
            handler-fn (default-middleware/protobuf->hash mapped-fn proto-class :default)
            streams (start-streams {:default {:handler-fn handler-fn}}
                                   (-> (ziggurat-config)
                                       (assoc-in [:stream-router :default :application-id] (rand-application-id))
                                       (assoc-in [:stream-router :default :changelog-topic-replication-factor] changelog-topic-replication-factor)))]
        (Thread/sleep 10000)                                ;;waiting for streams to start
        (IntegrationTestUtils/produceKeyValuesSynchronously (get-in (ziggurat-config) [:stream-router :default :origin-topic])
                                                            kvs
                                                            (props)
                                                            (MockTime.))
        (Thread/sleep 5000)                                 ;;wating for streams to consume messages
        (stop-streams streams)
        (is (= times @message-received-count))))))

(deftest start-streams-test-with-string-serde
  (let [message-received-count (atom 0)]
    (with-redefs [mapped-fn (fn [message-from-kafka]
                              (when (= json-message message-from-kafka)
                                (swap! message-received-count inc))
                              :success)]
      (let [times 6
            changelog-topic-replication-factor 1
            kvs (repeat times string-key-value-message)
            handler-fn (json-middleware/parse-json mapped-fn :using-string-serde)
            streams (start-streams {:using-string-serde {:handler-fn handler-fn}}
                                   (-> (ziggurat-config)
                                       (assoc-in [:stream-router :using-string-serde :application-id] (rand-application-id))
                                       (assoc-in [:stream-router :using-string-serde :changelog-topic-replication-factor] changelog-topic-replication-factor)))]
        (Thread/sleep 10000)                                ;;waiting for streams to start
        (IntegrationTestUtils/produceKeyValuesSynchronously (get-in (ziggurat-config) [:stream-router :using-string-serde :origin-topic])
                                                            kvs
                                                            (props-with-string-serializer)
                                                            (MockTime.))
        (Thread/sleep 5000)                                 ;;wating for streams to consume messages
        (stop-streams streams)
        (is (= times @message-received-count))))))

(deftest start-streams-test-with-tracer
  (let [message-received-count (atom 0)]
    (with-redefs [mapped-fn (fn [message-from-kafka]
                              (when (= message message-from-kafka)
                                (swap! message-received-count inc))
                              :success)]
      (let [times 1
            changelog-topic-replication-factor 1
            kvs (repeat times message-key-value)
            handler-fn (default-middleware/protobuf->hash mapped-fn proto-class :default)
            streams (start-streams {:default {:handler-fn handler-fn}}
                                   (-> (ziggurat-config)
                                       (assoc-in [:stream-router :default :application-id] (rand-application-id))
                                       (assoc-in [:stream-router :default :changelog-topic-replication-factor] changelog-topic-replication-factor)))]
        (Thread/sleep 10000)                                ;;waiting for streams to start
        (IntegrationTestUtils/produceKeyValuesSynchronously (get-in (ziggurat-config) [:stream-router :default :origin-topic])
                                                            kvs
                                                            (props)
                                                            (MockTime.))
        (Thread/sleep 5000)                                 ;;wating for streams to consume messages
        (stop-streams streams)
        (is (= times @message-received-count))
        (let [finished-spans (.finishedSpans tracer)
              tags (-> finished-spans
                       (.get 1)
                       (.tags))]
          (is (= 2 (.size finished-spans)))                 ;;2 spans - one from the TracingKafkaClientSupplier and one for the actual handler function
          (is (= "Message-Handler" (-> finished-spans
                                       (.get 1)
                                       (.operationName))))
          (is (= {(.getKey Tags/SPAN_KIND) Tags/SPAN_KIND_CONSUMER, (.getKey Tags/COMPONENT) "ziggurat"} tags)))))))

(deftest start-streams-test-when-tracer-is-not-configured
  (let [message-received-count (atom 0)]
    (with-redefs [mapped-fn (fn [message-from-kafka]
                              (when (= message message-from-kafka)
                                (swap! message-received-count inc))
                              :success)]
      (let [times 1
            changelog-topic-replication-factor 1
            kvs (repeat times message-key-value)
            handler-fn (default-middleware/protobuf->hash mapped-fn proto-class :default)
            streams (start-streams {:default {:handler-fn handler-fn}}
                                   (-> (ziggurat-config)
                                       (assoc-in [:stream-router :default :application-id] (rand-application-id))
                                       (assoc-in [:stream-router :default :changelog-topic-replication-factor] changelog-topic-replication-factor)
                                       (dissoc :tracer)))]
        (Thread/sleep 10000)                                ;;waiting for streams to start
        (IntegrationTestUtils/produceKeyValuesSynchronously (get-in (ziggurat-config) [:stream-router :default :origin-topic])
                                                            kvs
                                                            (props)
                                                            (MockTime.))
        (Thread/sleep 5000)                                 ;;wating for streams to consume messages
        (stop-streams streams)
        (is (= times @message-received-count))))))
