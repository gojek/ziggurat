(ns ziggurat.streams-test
  (:require [clojure.test :refer [deftest is join-fixtures testing use-fixtures]]
            [protobuf.core :as proto]
            [mount.core :as mount]
            [ziggurat.streams :refer [start-streams stop-streams stop-stream]]
            [ziggurat.fixtures :as fix]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.middleware.default :as default-middleware]
            [ziggurat.middleware.stream-joins :as stream-joins-middleware]
            [ziggurat.middleware.json :as json-middleware]
            [ziggurat.tracer :refer [tracer]]
            [ziggurat.messaging.producer :as producer]
            [ziggurat.config :as config])
  (:import [flatland.protobuf.test Example$Photo]
           [java.util Properties]
           [kafka.utils MockTime]
           [org.apache.kafka.clients.producer ProducerConfig]
           [org.apache.kafka.streams KeyValue]
           [org.apache.kafka.streams.integration.utils IntegrationTestUtils]
           [io.opentracing.tag Tags]))

(use-fixtures :once (join-fixtures [fix/mount-config-with-tracer
                                    fix/mount-metrics]))

(defn- props []
  (doto (Properties.)
    (.put ProducerConfig/BOOTSTRAP_SERVERS_CONFIG (get-in (ziggurat-config) [:stream-router :default :bootstrap-servers]))
    (.put ProducerConfig/ACKS_CONFIG "all")
    (.put ProducerConfig/RETRIES_CONFIG (int 0))
    (.put ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.ByteArraySerializer")
    (.put ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.ByteArraySerializer")))

(defn- props-with-string-serializer []
  (doto (props)
    (.put ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer")
    (.put ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer")))

(def message {:id   7
              :path "/photos/h2k3j4h9h23"})

(def proto-class Example$Photo)

(defn create-photo []
  (proto/->bytes (proto/create proto-class message)))

(def message-key-value (KeyValue/pair (create-photo) (create-photo)))

(def json-message {:foo "bar"})
(def changelog-topic-replication-factor 1)

(def string-key-value-message (KeyValue/pair "key" "{\"foo\":\"bar\"}"))

(defn- get-mapped-fn
  ([message-received-count]
   (get-mapped-fn message-received-count message))
  ([message-received-count expected-message]
   (fn [message-from-kafka]
     (when (= expected-message message-from-kafka)
       (swap! message-received-count inc))
     :success)))

(defn- rand-application-id []
  (str "test" "-" (rand-int 999999999)))

(deftest start-streams-with-since-test
  (let [message-received-count        (atom 0)
        mapped-fn                     (get-mapped-fn message-received-count)
        times                         6
        oldest-processed-message-in-s 10
        kvs                           (repeat times message-key-value)
        handler-fn                    (default-middleware/protobuf->hash mapped-fn proto-class :default)
        streams                       (start-streams {:default {:handler-fn handler-fn}}
                                                     (-> (ziggurat-config)
                                                         (assoc-in [:stream-router :default :application-id] (rand-application-id))
                                                         (assoc-in [:stream-router :default :oldest-processed-message-in-s] oldest-processed-message-in-s)
                                                         (assoc-in [:stream-router :default :changelog-topic-replication-factor] changelog-topic-replication-factor)))]
    (Thread/sleep 10000)                                    ;;waiting for streams to start
    (IntegrationTestUtils/produceKeyValuesSynchronously (get-in (ziggurat-config) [:stream-router :default :origin-topic])
                                                        kvs
                                                        (props)
                                                        (MockTime. (- (System/currentTimeMillis) (* 1000 oldest-processed-message-in-s)) (System/nanoTime)))
    (Thread/sleep 5000)                                     ;;wating for streams to consume messages
    (stop-streams streams)
    (is (= 0 @message-received-count))))

(deftest start-streams-test
  (let [message-received-count (atom 0)
        mapped-fn              (get-mapped-fn message-received-count)
        times                  6
        kvs                    (repeat times message-key-value)
        handler-fn             (default-middleware/protobuf->hash mapped-fn proto-class :default)
        streams                (start-streams {:default {:handler-fn handler-fn}}
                                              (-> (ziggurat-config)
                                                  (assoc-in [:stream-router :default :application-id] (rand-application-id))
                                                  (assoc-in [:stream-router :default :changelog-topic-replication-factor] changelog-topic-replication-factor)))]
    (Thread/sleep 10000)                                    ;;waiting for streams to start
    (IntegrationTestUtils/produceKeyValuesSynchronously (get-in (ziggurat-config) [:stream-router :default :origin-topic])
                                                        kvs
                                                        (props)
                                                        (MockTime.))
    (Thread/sleep 5000)                                     ;;wating for streams to consume messages
    (stop-streams streams)
    (is (= times @message-received-count))))

(deftest stop-stream-test
  (let [message-received-count (atom 0)
        mapped-fn              (get-mapped-fn message-received-count)
        times                  6
        kvs                    (repeat times message-key-value)
        handler-fn             (default-middleware/protobuf->hash mapped-fn proto-class :default)
        _                      (mount/start)
        streams                (start-streams {:default {:handler-fn handler-fn}}
                                              (-> (ziggurat-config)
                                                  (assoc-in [:stream-router :default :application-id] (rand-application-id))
                                                  (assoc-in [:stream-router :default :changelog-topic-replication-factor] changelog-topic-replication-factor)))]
    (Thread/sleep 10000)                                    ;;waiting for streams to start
    (IntegrationTestUtils/produceKeyValuesSynchronously (get-in (ziggurat-config) [:stream-router :default :origin-topic])
                                                        kvs
                                                        (props)
                                                        (MockTime.))
    (Thread/sleep 10000)                                     ;;wating for streams to consume messages
    (stop-stream :default)
    (is (= times @message-received-count))))

(deftest stop-duplicate-stream-test
  (let [message-received-count (atom 0)
        mapped-fn              (get-mapped-fn message-received-count)
        times                  6
        kvs                    (repeat times message-key-value)
        handler-fn             (default-middleware/protobuf->hash mapped-fn proto-class :default)
        _                      (mount/start)
        streams                (start-streams {:default {:handler-fn handler-fn}}
                                              (-> (ziggurat-config)
                                                  (assoc-in [:stream-router :default :application-id] (rand-application-id))
                                                  (assoc-in [:stream-router :default :changelog-topic-replication-factor] changelog-topic-replication-factor)))]
    (Thread/sleep 10000)                                    ;;waiting for streams to start
    (IntegrationTestUtils/produceKeyValuesSynchronously (get-in (ziggurat-config) [:stream-router :default :origin-topic])
                                                        kvs
                                                        (props)
                                                        (MockTime.))
    (Thread/sleep 10000)                                     ;;wating for streams to consume messages
    (stop-stream :default)
    (stop-stream :default)                                   ;;attempting to close same stream again
    (is (= times @message-received-count))))

(deftest stop-invalid-stream-test
  (let [message-received-count (atom 0)
        mapped-fn              (get-mapped-fn message-received-count)
        times                  6
        kvs                    (repeat times message-key-value)
        handler-fn             (default-middleware/protobuf->hash mapped-fn proto-class :default)
        _                      (mount/start)
        streams                (start-streams {:default {:handler-fn handler-fn}}
                                              (-> (ziggurat-config)
                                                  (assoc-in [:stream-router :default :application-id] (rand-application-id))
                                                  (assoc-in [:stream-router :default :changelog-topic-replication-factor] changelog-topic-replication-factor)))]
    (Thread/sleep 10000)                                    ;;waiting for streams to start
    (IntegrationTestUtils/produceKeyValuesSynchronously (get-in (ziggurat-config) [:stream-router :default :origin-topic])
                                                        kvs
                                                        (props)
                                                        (MockTime.))
    (Thread/sleep 10000)                                     ;;wating for streams to consume messages
    (stop-stream :invalid-topic-entity)
    (is (= times @message-received-count))))

(deftest start-stream-joins-test
  (testing "stream joins using inner join"
    (let [orig-config (ziggurat-config)]
      (with-redefs [config/ziggurat-config (fn [] (-> orig-config
                                                      (assoc-in [:alpha-features :stream-joins] true)))]
        (let [message-received-count (atom 0)
              mapped-fn              (get-mapped-fn message-received-count {:topic message :another-test-topic message})
              times                  1
              kvs                    (repeat times message-key-value)
              handler-fn             (stream-joins-middleware/protobuf->hash mapped-fn proto-class :default)
              streams                (start-streams {:default {:handler-fn handler-fn}}
                                                    (-> (ziggurat-config)
                                                        (assoc-in [:stream-router :default :consumer-type] :stream-joins)
                                                        (assoc-in [:stream-router :default :input-topics] {:topic {:name "topic"} :another-test-topic {:name "another-test-topic"}})
                                                        (assoc-in [:stream-router :default :join-cfg] {:topic-and-another-test-topic {:join-window-ms 5000 :join-type :inner}})
                                                        (assoc-in [:stream-router :default :application-id] (rand-application-id))
                                                        (assoc-in [:stream-router :default :changelog-topic-replication-factor] changelog-topic-replication-factor)))]
          (Thread/sleep 10000)                              ;;waiting for streams to start
          (IntegrationTestUtils/produceKeyValuesSynchronously "topic"
                                                              kvs
                                                              (props)
                                                              (MockTime.))
          (IntegrationTestUtils/produceKeyValuesSynchronously "another-test-topic"
                                                              kvs
                                                              (props)
                                                              (MockTime.))
          (Thread/sleep 5000)                               ;;wating for streams to consume messages
          (stop-streams streams)
          (is (= times @message-received-count))))))
  (testing "stream joins using left join"
    (let [orig-config (ziggurat-config)]
      (with-redefs [config/ziggurat-config (fn [] (-> orig-config
                                                      (assoc-in [:alpha-features :stream-joins] true)))]
        (let [message-received-count (atom 0)
              mapped-fn              (get-mapped-fn message-received-count {:topic message :another-test-topic message})
              times                  1
              kvs                    (repeat times message-key-value)
              handler-fn             (stream-joins-middleware/protobuf->hash mapped-fn proto-class :default)
              streams                (start-streams {:default {:handler-fn handler-fn}}
                                                    (-> (ziggurat-config)
                                                        (assoc-in [:stream-router :default :consumer-type] :stream-joins)
                                                        (assoc-in [:stream-router :default :input-topics] {:topic {:name "topic"} :another-test-topic {:name "another-test-topic"}})
                                                        (assoc-in [:stream-router :default :join-cfg] {:topic-and-another-test-topic {:join-window-ms 5000 :join-type :left}})
                                                        (assoc-in [:stream-router :default :application-id] (rand-application-id))
                                                        (assoc-in [:stream-router :default :changelog-topic-replication-factor] changelog-topic-replication-factor)))]
          (Thread/sleep 10000)                              ;;waiting for streams to start
          (IntegrationTestUtils/produceKeyValuesSynchronously "topic"
                                                              kvs
                                                              (props)
                                                              (MockTime.))
          (IntegrationTestUtils/produceKeyValuesSynchronously "another-test-topic"
                                                              kvs
                                                              (props)
                                                              (MockTime.))
          (Thread/sleep 5000)                               ;;wating for streams to consume messages
          (stop-streams streams)
          (is (= times @message-received-count))))))
  (testing "stream joins using outer join"
    (let [orig-config (ziggurat-config)]
      (with-redefs [config/ziggurat-config (fn [] (-> orig-config
                                                      (assoc-in [:alpha-features :stream-joins] true)))]
        (let [message-received-count (atom 0)
              mapped-fn              (get-mapped-fn message-received-count {:topic message :another-test-topic message})
              times                  1
              kvs                    (repeat times message-key-value)
              handler-fn             (stream-joins-middleware/protobuf->hash mapped-fn proto-class :default)
              streams                (start-streams {:default {:handler-fn handler-fn}}
                                                    (-> (ziggurat-config)
                                                        (assoc-in [:stream-router :default :consumer-type] :stream-joins)
                                                        (assoc-in [:stream-router :default :input-topics] {:topic {:name "topic"} :another-test-topic {:name "another-test-topic"}})
                                                        (assoc-in [:stream-router :default :join-cfg] {:topic-and-another-test-topic {:join-window-ms 5000 :join-type :outer}})
                                                        (assoc-in [:stream-router :default :application-id] (rand-application-id))
                                                        (assoc-in [:stream-router :default :changelog-topic-replication-factor] changelog-topic-replication-factor)))]
          (Thread/sleep 10000)                              ;;waiting for streams to start
          (IntegrationTestUtils/produceKeyValuesSynchronously "topic"
                                                              kvs
                                                              (props)
                                                              (MockTime.))
          (IntegrationTestUtils/produceKeyValuesSynchronously "another-test-topic"
                                                              kvs
                                                              (props)
                                                              (MockTime.))
          (Thread/sleep 5000)                               ;;wating for streams to consume messages
          (stop-streams streams)
          (is (= times @message-received-count))))))
  (testing "stream-joins should not start if :alpha-features for stream-joins is `false`"
    (let [original-config (ziggurat-config)]
      (with-redefs [ziggurat-config (fn [] (-> original-config
                                               (assoc-in [:alpha-features :stream-joins] false)))]
        (let [handler-fn (constantly nil)
              streams                (start-streams {:default {:handler-fn handler-fn}}
                                                    (-> (ziggurat-config)
                                                        (assoc-in [:stream-router :default :consumer-type] :stream-joins)
                                                        (assoc-in [:stream-router :default :input-topics] {:topic {:name "topic"} :another-test-topic {:name "another-test-topic"}})
                                                        (assoc-in [:stream-router :default :join-cfg] {:topic-and-another-test-topic {:join-window-ms 5000 :join-type :outer}})
                                                        (assoc-in [:stream-router :default :application-id] (rand-application-id))
                                                        (assoc-in [:stream-router :default :changelog-topic-replication-factor] changelog-topic-replication-factor)))]
          (is (empty? streams)))))))

(deftest start-streams-test-with-string-serde
  (let [message-received-count (atom 0)
        mapped-fn              (get-mapped-fn message-received-count json-message)
        times                  6
        kvs                    (repeat times string-key-value-message)
        handler-fn             (json-middleware/parse-json mapped-fn :using-string-serde)
        streams                (start-streams {:using-string-serde {:handler-fn handler-fn}}
                                              (-> (ziggurat-config)
                                                  (assoc-in [:stream-router :using-string-serde :application-id] (rand-application-id))
                                                  (assoc-in [:stream-router :using-string-serde :changelog-topic-replication-factor] changelog-topic-replication-factor)))]
    (Thread/sleep 10000)                                    ;;waiting for streams to start
    (IntegrationTestUtils/produceKeyValuesSynchronously (get-in (ziggurat-config) [:stream-router :using-string-serde :origin-topic])
                                                        kvs
                                                        (props-with-string-serializer)
                                                        (MockTime.))
    (Thread/sleep 5000)                                     ;;wating for streams to consume messages
    (stop-streams streams)
    (is (= times @message-received-count))))

(deftest start-streams-test-with-tracer
  (let [message-received-count (atom 0)
        mapped-fn              (get-mapped-fn message-received-count)
        times                  1
        kvs                    (repeat times message-key-value)
        handler-fn             (default-middleware/protobuf->hash mapped-fn proto-class :default)
        streams                (start-streams {:default {:handler-fn handler-fn}}
                                              (-> (ziggurat-config)
                                                  (assoc-in [:stream-router :default :application-id] (rand-application-id))
                                                  (assoc-in [:stream-router :default :changelog-topic-replication-factor] changelog-topic-replication-factor)))]
    (Thread/sleep 10000)                                    ;;waiting for streams to start
    (IntegrationTestUtils/produceKeyValuesSynchronously (get-in (ziggurat-config) [:stream-router :default :origin-topic])
                                                        kvs
                                                        (props)
                                                        (MockTime.))
    (Thread/sleep 5000)                                     ;;wating for streams to consume messages
    (stop-streams streams)
    (is (= times @message-received-count))
    (let [finished-spans (.finishedSpans tracer)
          tags           (-> finished-spans
                             (.get 1)
                             (.tags))]
      (is (= 2 (.size finished-spans)))                     ;;2 spans - one from the TracingKafkaClientSupplier and one for the actual handler function
      (is (= "Message-Handler" (-> finished-spans
                                   (.get 1)
                                   (.operationName))))
      (is (= {(.getKey Tags/SPAN_KIND) Tags/SPAN_KIND_CONSUMER, (.getKey Tags/COMPONENT) "ziggurat"} tags)))))

(deftest start-streams-test-when-tracer-is-not-configured
  (let [message-received-count (atom 0)
        mapped-fn              (get-mapped-fn message-received-count)
        times                  1
        kvs                    (repeat times message-key-value)
        handler-fn             (default-middleware/protobuf->hash mapped-fn proto-class :default)
        streams                (start-streams {:default {:handler-fn handler-fn}}
                                              (-> (ziggurat-config)
                                                  (assoc-in [:stream-router :default :application-id] (rand-application-id))
                                                  (assoc-in [:stream-router :default :changelog-topic-replication-factor] changelog-topic-replication-factor)
                                                  (dissoc :tracer)))]
    (Thread/sleep 10000)                                    ;;waiting for streams to start
    (IntegrationTestUtils/produceKeyValuesSynchronously (get-in (ziggurat-config) [:stream-router :default :origin-topic])
                                                        kvs
                                                        (props)
                                                        (MockTime.))
    (Thread/sleep 5000)                                     ;;wating for streams to consume messages
    (stop-streams streams)
    (is (= times @message-received-count))))

(deftest stops-streams-when-exception-is-raised-from-mapper-fn-test
  (let [stop-streams-called? (atom false)
        mapped-fn            (fn [message]
                               :retry)
        times                1
        kvs                  (repeat times message-key-value)
        streams              (start-streams {:default {:handler-fn mapped-fn}}
                                            (-> (ziggurat-config)
                                                (assoc-in [:stream-router :default :application-id] (rand-application-id))
                                                (assoc-in [:stream-router :default :changelog-topic-replication-factor] changelog-topic-replication-factor)
                                                (dissoc :tracer)))]
    (with-redefs [producer/retry                (fn [message-payload]
                                                  (throw (ex-info "Streams test: rabbit retry error" {:type :rabbitmq-publish-failure
                                                                                                      :e    (Exception. "Custom Error")})))
                  ziggurat.streams/stop-streams (fn [kafka-stream]
                                                  (reset! stop-streams-called? true))]
      (Thread/sleep 10000)                                  ;;waiting for streams to start
      (IntegrationTestUtils/produceKeyValuesSynchronously (get-in (ziggurat-config) [:stream-router :default :origin-topic])
                                                          kvs
                                                          (props)
                                                          (MockTime.))
      (Thread/sleep 5000)                                   ;;wating for streams to consume messages
      (is @stop-streams-called?))
    (stop-streams streams)))
