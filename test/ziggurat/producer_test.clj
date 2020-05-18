(ns ziggurat.producer-test
  (:require [clojure.string :refer [blank?]]
            [clojure.test :refer :all]
            [clojure.test.check.generators :as gen]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.fixtures :as fix :refer [*producer-properties* *consumer-properties*]]
            [ziggurat.producer :refer [producer-properties-map send kafka-producers
                                       property->fn -send producer-properties]]
            [ziggurat.streams :refer [start-streams stop-streams]]
            [ziggurat.tracer :refer [tracer]])
  (:import (org.apache.kafka.clients.producer KafkaProducer ProducerRecord ProducerConfig)
           [org.apache.kafka.streams.integration.utils IntegrationTestUtils]
           [io.opentracing.contrib.kafka TracingKafkaProducer]))

(use-fixtures :once fix/mount-producer-with-config-and-tracer)

(def valid-config {:key-serializer-class   "org.apache.kafka.common.serialization.StringSerializer"
                   :value-serializer-class "org.apache.kafka.common.serialization.StringSerializer"
                   :bootstrap-servers      "localhost:8000"})

(defn stream-router-config-without-producer [])
(:stream-router {:default {:application-id       "test"
                           :bootstrap-servers    "localhost:9092"
                           :stream-threads-count [1 :int]
                           :origin-topic         "topic"
                           :channels             {:channel-1 {:worker-count [10 :int]
                                                              :retry        {:count   [5 :int]
                                                                             :enabled [true :bool]}}}}})

(deftest send-data-with-topic-and-value-test
  (with-redefs [kafka-producers (hash-map :default (KafkaProducer. *producer-properties*))]
    (let [alphanum-gen (gen/such-that #(not (blank? %)) gen/string-alphanumeric)
          topic        (gen/generate alphanum-gen 10)
          key          "message"
          value        "Hello World!!"]
      (send :default topic key value)
      (let [result (IntegrationTestUtils/waitUntilMinKeyValueRecordsReceived *consumer-properties* topic 1 2000)]
        (is (= value (.value (first result))))))))

(deftest send-data-with-topic-key-partition-and-value-test
  (with-redefs [kafka-producers (hash-map :default (KafkaProducer. *producer-properties*))]
    (let [alphanum-gen (gen/such-that #(not (blank? %)) gen/string-alphanumeric)
          topic        (gen/generate alphanum-gen 10)
          key          "message"
          value        "Hello World!!"
          partition    (int 0)]
      (send :default topic partition key value)
      (let [result (IntegrationTestUtils/waitUntilMinKeyValueRecordsReceived *consumer-properties* topic 1 2000)]
        (is (= value (.value (first result))))))))

(deftest send-throws-exception-when-no-producers-are-configured
  (with-redefs [kafka-producers {}]
    (let [topic "test-topic"
          key   "message"
          value "Hello World!! from non-existant Kafka Producers"]
      (is (not-empty (try (send :default topic key value)
                          (catch Exception e (ex-data e))))))))

(deftest producer-properties-map-is-empty-if-no-producers-configured
  ; Here ziggurat-config has been substituted with a custom map which
  ; does not have any valid producer configs.
  (with-redefs [ziggurat-config stream-router-config-without-producer]
    (is (empty? (producer-properties-map)))))

(deftest producer-properties-map-is-not-empty-if-producers-are-configured
  ; Here the config is read from config.test.edn which contains
  ; valid producer configs.
  (is (seq (producer-properties-map))))

(deftest send-data-with-tracer-enabled
  (with-redefs [kafka-producers (hash-map :default (TracingKafkaProducer. (KafkaProducer. *producer-properties*) tracer))]
    (let [alphanum-gen (gen/such-that #(not (blank? %)) gen/string-alphanumeric)
          topic        (gen/generate alphanum-gen 10)
          key          "message"
          value        "Hello World!!"]
      (.reset tracer)
      (send :default topic key value)
      (let [result (IntegrationTestUtils/waitUntilMinKeyValueRecordsReceived *consumer-properties* topic 1 2000)
            finished-spans (.finishedSpans tracer)]
        (is (= value (.value (first result))))
        (is (= 1 (.size finished-spans)))
        (is (= (str "To_" topic) (-> finished-spans
                                     (.get 0)
                                     (.operationName))))))))

(deftest java-send-test
  (let [stream-config-key           ":entity"
        expected-stream-config-key  (keyword (subs stream-config-key 1))
        topic                       "topic"
        key                         "key"
        value                       "value"
        partition                   1
        send-called?                (atom false)
        send-with-partition-called? (atom false)]
    (testing "it calls send with the correct parameters i.e. config-key(keyword), topic(string), key, value"
      (with-redefs [send (fn [actual-stream-config-key actual-topic actual-key actual-value]
                           (if (and (= actual-stream-config-key expected-stream-config-key)
                                    (= actual-key key)
                                    (= actual-topic topic)
                                    (= actual-value value))
                             (reset! send-called? true)))]
        (-send stream-config-key topic key value)
        (is (true? @send-called?))))
    (testing "it calls send with the correct parameters i.e. config-key(keyword), topic(string), partition, key, value"
      (with-redefs [send (fn [actual-stream-config-key actual-topic actual-partition actual-key actual-value]
                           (if (and (= actual-stream-config-key expected-stream-config-key)
                                    (= actual-key key)
                                    (= actual-partition partition)
                                    (= actual-topic topic)
                                    (= actual-value value))
                             (reset! send-with-partition-called? true)))]
        (-send stream-config-key topic partition key value)
        (is (true? @send-with-partition-called?))))))

(deftest producer-properties-test
  (testing "with correct config"
    (let [valid-config (assoc valid-config :linger-ms "1")
          props        (producer-properties valid-config)]
      (is (= (.getProperty props ProducerConfig/LINGER_MS_CONFIG)
             "1"))
      (is (= (.getProperty props ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG)
             (:key-serializer-class valid-config)))
      (is (= (.getProperty props ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG)
             (:value-serializer-class valid-config)))
      (is (= (.getProperty props ProducerConfig/BOOTSTRAP_SERVERS_CONFIG)
             (:bootstrap-servers valid-config)))))

  (testing "with incorrect config"
    (let [valid-config (assoc valid-config :linger-ms-foo "1")]
      (is (thrown? java.lang.RuntimeException (producer-properties valid-config))))
    (let [valid-config (update  valid-config :key-serializer-class (constantly  "java.time.Clock"))]
      (is (thrown? java.lang.RuntimeException (producer-properties valid-config))))
    (let [valid-config (update  valid-config :key-serializer-class (constantly  "java.foo.Bar"))]
      (is (thrown? java.lang.RuntimeException (producer-properties valid-config))))
    (let [valid-config (dissoc valid-config :bootstrap-servers)]
      (is (thrown? java.lang.RuntimeException (producer-properties valid-config))))))

(deftest property->fn-test
  (testing "should return the producer property for a given config"
    (let [expected-properties #{"send.buffer.bytes"
                                "metrics.sample.window.ms"
                                "receive.buffer.bytes"
                                "client.dns.lookup"
                                "reconnect.backoff.ms"
                                "transactional.id"
                                "interceptor.classes"
                                "bootstrap.servers"
                                "request.timeout.ms"
                                "connections.max.idle.ms"
                                "metrics.num.samples"
                                "retry.backoff.ms"
                                "linger.ms"
                                "enable.idempotence"
                                "client.id"
                                "metadata.max.age.ms"
                                "max.block.ms"
                                "value.serializer"
                                "retries"
                                "key.serializer"
                                "reconnect.backoff.max.ms"
                                "metrics.recording.level"
                                "batch.size"
                                "delivery.timeout.ms"
                                "buffer.memory"
                                "max.in.flight.requests.per.connection"
                                "partitioner.class"
                                "acks"
                                "max.request.size"
                                "transaction.timeout.ms"
                                "compression.type"
                                "metric.reporters"}
          configs             [:key-serializer-class
                               :value-serializer-class
                               :retries
                               :bootstrap-servers
                               :metadata-max-age
                               :reconnect-backoff-ms
                               :client-id
                               :metrics-num-samples
                               :transaction-timeout
                               :retry-backoff-ms
                               :receive-buffer
                               :partitioner-class
                               :max-block-ms
                               :metric-reporter-classes
                               :compression-type
                               :max-request-size
                               :delivery-timeout-ms
                               :metrics-sample-window-ms
                               :request-timeout-ms
                               :buffer-memory
                               :interceptor-classes
                               :linger-ms
                               :connections-max-idle-ms
                               :acks
                               :enable-idempotence
                               :metrics-recording-level
                               :transactional-id
                               :reconnect-backoff-max-ms
                               :client-dns-lookup
                               :max-in-flight-requests-per-connection
                               :send-buffer
                               :batch-size]
          response            (set (for [config configs]
                                     (eval (property->fn config))))]
      (is (= response
             expected-properties)))))

(deftest backward-compatibility-of-producer-configs
  (testing "should allow key-serializer, value-serializer and retries-config attributes of kafka producer"
    (let [expected-properties #{"value.serializer"
                                "retries"
                                "key.serializer"}
          configs             [:key-serializer
                               :value-serializer
                               :retries-config]
          response            (set (for [config configs]
                                     (eval (property->fn config))))]
      (is (= response
             expected-properties)))))
