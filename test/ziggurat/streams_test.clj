(ns ziggurat.streams-test
  (:require [clojure.java.shell :refer [sh]]
            [clojure.test :refer [deftest is join-fixtures testing use-fixtures]]
            [mount.core :as mount]
            [protobuf.core :as proto]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.fixtures :as fix]
            [ziggurat.middleware.default :as default-middleware]
            [ziggurat.middleware.json :as json-middleware]
            [ziggurat.middleware.stream-joins :as stream-joins-middleware]
            [ziggurat.streams :refer [add-stream-thread get-stream-thread-count remove-stream-thread start-stream start-streams stop-stream stop-streams]]
            [ziggurat.streams :refer [handle-uncaught-exception start-stream start-streams stop-stream stop-streams]])
  (:import (com.gojek.test.proto Example$Photo)
           (java.net URI)
           (java.nio.file Files Paths)
           (java.nio.file.attribute FileAttribute)
           (java.util Properties)
           (kafka.server KafkaConfig KafkaServer)
           (org.apache.kafka.clients CommonClientConfigs)
           (org.apache.kafka.clients.producer ProducerConfig)
           (org.apache.kafka.common.config SaslConfigs)
           (org.apache.kafka.common.utils MockTime)
           (org.apache.kafka.streams KafkaStreams$State)
           (org.apache.kafka.streams KeyValue)
           (org.apache.kafka.streams.errors StreamsUncaughtExceptionHandler$StreamThreadExceptionResponse)
           (org.apache.kafka.streams.integration.utils IntegrationTestUtils)
           (scala Some)))

(use-fixtures :once (join-fixtures [fix/mount-config-with-tracer
                                    fix/silence-logging
                                    fix/mount-metrics]))

(def truststore-path "/tmp/truststore/kafka.server.truststore.jks")
(def saas-config-path "test/ziggurat/kafka_server_jaas.conf")
(def truststore-password "testpassword")

(defn- start-mount
  []
  (mount/start-with-states [[#'ziggurat.messaging.consumer/consumers {:start (constantly nil)
                                                                      :stop  (constantly nil)}]]))

(defn- props []
  (doto (Properties.)
    (.put ProducerConfig/BOOTSTRAP_SERVERS_CONFIG (get-in (ziggurat-config) [:stream-router :default :bootstrap-servers]))
    (.put ProducerConfig/ACKS_CONFIG "all")
    (.put ProducerConfig/RETRIES_CONFIG (int 0))
    (.put ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.ByteArraySerializer")
    (.put ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.ByteArraySerializer")))

(defn- props-with-sasl-config []
  (doto (props)
    (.put ProducerConfig/BOOTSTRAP_SERVERS_CONFIG "localhost:9093")
    (.put CommonClientConfigs/SECURITY_PROTOCOL_CONFIG "SASL_SSL")
    (.put SaslConfigs/SASL_MECHANISM "OAUTHBEARER")
    (.put SaslConfigs/SASL_LOGIN_CALLBACK_HANDLER_CLASS "org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerValidatorCallbackHandler")
    (.put props SaslConfigs/SASL_JAAS_CONFIG
          "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;")
    (.put props "ssl.truststore.location" truststore-path)
    (.put props "ssl.truststore.password" truststore-password)
    (.put props "ssl.endpoint.identification.algorithm" "https")))

(defn- props-with-string-serializer []
  (doto (props)
    (.put ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer")
    (.put ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer")))

(defn create-truststore []
  "Generates a new truststore and self-signed certificate for testing."
  (let [some-path (Paths/get (URI. (str "file:///" truststore-path)))
        dir-path  (.getParent some-path)]
    ;; Ensure directory exists
    (Files/createDirectories dir-path (into-array FileAttribute []))
    ;; Generate truststore with keytool
    (sh "keytool" "-genkeypair"
        "-alias" "test-cert"
        "-keyalg" "RSA"
        "-keystore" truststore-path
        "-storepass" truststore-password
        "-validity" "365"
        "-keysize" "2048"
        "-dname" "CN=localhost, OU=Test, O=Ziggurat, L=City, S=State, C=US")))

(defn cleanup-truststore []
  "Deletes the generated truststore after the test."
  (let [path (Paths/get truststore-path nil)]
    (Files/deleteIfExists path)))

(defn create-broker-config [port log-dir]
  "Manually create a Kafka broker configuration."
  (let [props (Properties.)]
    (.put props "broker.id" "0")
    (.put props "log.dirs" (.toString log-dir))
    (.put props "listeners" (str "SASL_SSL://localhost:" port))
    (.put props "advertised.listeners" (str "SASL_SSL://localhost:" port))
    (.put props "listener.security.protocol.map" "SASL_SSL:SASL_SSL")
    (.put props "inter.broker.listener.name" "SASL_SSL")
    (.put props "ssl.truststore.location" truststore-path)
    (.put props "ssl.truststore.password" truststore-password)
    (.put props "ssl.endpoint.identification.algorithm" "")
    (.put props "sasl.enabled.mechanisms" "OAUTHBEARER")
    (.put props "sasl.mechanism.inter.broker.protocol" "OAUTHBEARER")
    (.put props "zookeeper.connect" "localhost:2181")
    (.put props "sasl.jaas.config" "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;")
    props))

(defn create-kafka-config []
  "Create Kafka Streams configuration with SASL/SSL."
  (let [props (Properties.)]
    (.put props "security.protocol" "SASL_SSL")
    (.put props "ssl.truststore.location" truststore-path)
    (.put props "ssl.truststore.password" truststore-password)
    (.put props "sasl.mechanism" "OAUTHBEARER")
    (.put props "sasl.jaas.config" "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;")
    props))

(defn start-embedded-kafka [port]
  "Start an embedded Kafka broker."
  (create-truststore)
  (System/setProperty "java.security.auth.login.config" saas-config-path)
  (let [log-dir      (Files/createTempDirectory "kafka-logs" (into-array FileAttribute []))
        config       (KafkaConfig. (create-broker-config port log-dir))
        kafka-server (KafkaServer. config (MockTime.) (new Some "a") false)]
    (.startup kafka-server)
    kafka-server))

(defn stop-embedded-kafka [kafka-server]
  "Stop the embedded Kafka broker."
  (.shutdown kafka-server)
  (cleanup-truststore)
  (System/clearProperty "java.security.auth.login.config"))


(def ^:dynamic *embedded-kafka* nil)

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
     (when (and (= expected-message (:message message-from-kafka))
                (some? (:metadata message-from-kafka))
                (nil? (:retry-count message-from-kafka))
                (nil? (:topic-entity message-from-kafka))
                (nil? (:headers message-from-kafka)))
       (swap! message-received-count inc)
       :success))))

(defn- poll-to-check-if-running
  ([stream]
   (poll-to-check-if-running stream :default))
  ([stream topic-entity]
   (let [threshold 0]
     (while (and (not= (.state (get stream topic-entity)) KafkaStreams$State/RUNNING)
                 (> threshold 30))
       (Thread/sleep 2000)
       (inc threshold)))))

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
    (Thread/sleep 5000)                                     ;;waiting for streams to start
    (IntegrationTestUtils/produceKeyValuesSynchronously (get-in (ziggurat-config) [:stream-router :default :origin-topic])
                                                        kvs
                                                        (props)
                                                        (MockTime. 0 (- (System/currentTimeMillis) (* 1000 oldest-processed-message-in-s)) (System/nanoTime)))
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

(deftest start-streams-test-when-sasl-configs-are-not-provided
  (with-redefs [ziggurat.config/ssl-config  (constantly {})
                ziggurat.config/sasl-config (constantly {})]
    (let [message-received-count (atom 0)
          mapped-fn              (get-mapped-fn message-received-count)
          times                  6
          kvs                    (repeat times message-key-value)
          handler-fn             (default-middleware/protobuf->hash mapped-fn proto-class :default)
          streams                (start-streams {:default {:handler-fn handler-fn}}
                                                (-> (ziggurat-config)
                                                    (assoc-in [:stream-router :default :application-id] (rand-application-id))
                                                    (assoc-in [:stream-router :default :changelog-topic-replication-factor] changelog-topic-replication-factor)))]
      (Thread/sleep 10000)                                  ;;waiting for streams to start
      (IntegrationTestUtils/produceKeyValuesSynchronously (get-in (ziggurat-config) [:stream-router :default :origin-topic])
                                                          kvs
                                                          (props)
                                                          (MockTime.))
      (Thread/sleep 5000)                                   ;;wating for streams to consume messages
      (stop-streams streams)
      (is (= times @message-received-count)))))

(deftest start-streams-test-should-fail-with-invalid-sasl-configs
  (with-redefs [ziggurat.config/ssl-config (fn [] {:enabled                 true
                                                   :protocol                "SASL_SSL"
                                                   :mechanism               "OAUTHBEARER"
                                                   :ssl-truststore-location "/path/to/truststore.jks"
                                                   :ssl-truststore-password "some-password"
                                                   :login-callback-handler  "com.example.oauthbearer.OAuthBearerLoginCallbackHandler"
                                                   :jaas                    {:login-module "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule"}})]
    (let [message-received-count (atom 0)
          mapped-fn              (get-mapped-fn message-received-count)
          handler-fn             (default-middleware/protobuf->hash mapped-fn proto-class :default)
          streams                (try (start-streams {:default {:handler-fn handler-fn}}
                                                     (-> (ziggurat-config)
                                                         (assoc-in [:stream-router :default :application-id] (rand-application-id))
                                                         (assoc-in [:stream-router :default :changelog-topic-replication-factor] changelog-topic-replication-factor)))
                                      (catch Exception e (.getMessage e)))]
      (is (= streams "Invalid value com.example.oauthbearer.OAuthBearerLoginCallbackHandler for configuration sasl.login.callback.handler.class: Class com.example.oauthbearer.OAuthBearerLoginCallbackHandler could not be found.")))))

(deftest start-streams-test-with-valid-sasl-configs
  (binding [*embedded-kafka* (start-embedded-kafka 9093)]
    (try
      (with-redefs [ziggurat.config/ssl-config (fn [] {:enabled                true
                                                       :protocol               "SASL_SSL"
                                                       :mechanism              "OAUTHBEARER"
                                                       :login-callback-handler "org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerValidatorCallbackHandler"
                                                       :jaas                   {:login-module "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule"}})]
        (let [message-received-count (atom 0)
              mapped-fn              (get-mapped-fn message-received-count)
              times                  6
              kvs                    (repeat times message-key-value)
              handler-fn             (default-middleware/protobuf->hash mapped-fn proto-class :default)
              streams                (start-streams {:default {:handler-fn handler-fn}}
                                                    (-> (ziggurat-config)
                                                        (assoc-in [:stream-router :default :bootstrap-servers] "localhost:9093")
                                                        (assoc-in [:stream-router :default :application-id] (rand-application-id))
                                                        (assoc-in [:stream-router :default :changelog-topic-replication-factor] changelog-topic-replication-factor)))]
          (Thread/sleep 10000)
          (IntegrationTestUtils/produceKeyValuesSynchronously (get-in (ziggurat-config) [:stream-router :default :origin-topic])
                                                              kvs
                                                              (props-with-sasl-config)
                                                              (MockTime.))
          (Thread/sleep 5000)                               ;;waiting for streams to consume messages
          (stop-streams streams)
          (is (= times @message-received-count))))
      (finally (stop-embedded-kafka *embedded-kafka*)))))

(deftest stop-stream-test
  (let [message-received-count (atom 0)
        mapped-fn              (get-mapped-fn message-received-count)
        handler-fn             (default-middleware/protobuf->hash mapped-fn proto-class :default)
        _                      (start-mount)]
    (mount/start-with-states {#'ziggurat.streams/stream {:start (fn [] (start-streams {:default {:handler-fn handler-fn}}
                                                                                      (ziggurat-config)))
                                                         :stop  (fn [] (stop-streams #'ziggurat.streams/stream))}}))
  (poll-to-check-if-running ziggurat.streams/stream)
  (stop-stream :default)
  (is (not= (.state (get ziggurat.streams/stream :default)) KafkaStreams$State/RUNNING)))

(deftest start-stopped-stream-test
  (let [message-received-count (atom 0)
        mapped-fn              (get-mapped-fn message-received-count)
        handler-fn             (default-middleware/protobuf->hash mapped-fn proto-class :default)
        _                      (start-mount)]
    (mount/start-with-states {#'ziggurat.streams/stream {:start (fn [] (start-streams {:default {:handler-fn handler-fn}}
                                                                                      (ziggurat-config)))
                                                         :stop  (fn [] (stop-streams ziggurat.streams/stream))}}))
  (poll-to-check-if-running ziggurat.streams/stream)
  (stop-stream :default)
  (is (not= (.state (get ziggurat.streams/stream :default)) KafkaStreams$State/RUNNING))
  (start-stream :default)
  (poll-to-check-if-running ziggurat.streams/stream)
  (is (not= (.state (get ziggurat.streams/stream :default)) KafkaStreams$State/NOT_RUNNING)))

(deftest stop-restarted-stream-test
  (let [message-received-count (atom 0)
        mapped-fn              (get-mapped-fn message-received-count)
        handler-fn             (default-middleware/protobuf->hash mapped-fn proto-class :default)
        _                      (start-mount)]
    (mount/start-with-states {#'ziggurat.streams/stream {:start (fn [] (start-streams {:default {:handler-fn handler-fn}}
                                                                                      (ziggurat-config)))
                                                         :stop  (fn [] (stop-streams ziggurat.streams/stream))}}))
  (poll-to-check-if-running ziggurat.streams/stream)
  (stop-stream :default)
  (is (not= (.state (get ziggurat.streams/stream :default)) KafkaStreams$State/RUNNING))
  (start-stream :default)
  (poll-to-check-if-running ziggurat.streams/stream :default)
  (is (not= (.state (get ziggurat.streams/stream :default)) KafkaStreams$State/NOT_RUNNING))
  (stop-stream :default)
  (is (not= (.state (get ziggurat.streams/stream :default)) KafkaStreams$State/RUNNING)))

(deftest stop-desired-stream-only-test
  (let [message-received-count (atom 0)
        mapped-fn              (get-mapped-fn message-received-count)
        handler-fn             (default-middleware/protobuf->hash mapped-fn proto-class :default)
        _                      (start-mount)
        stream-routes          {:default            {:handler-fn handler-fn}
                                :using-string-serde {:handler-fn handler-fn}}]
    (mount/start-with-states {#'ziggurat.streams/stream {:start (fn [] (start-streams stream-routes
                                                                                      (-> (ziggurat-config)
                                                                                          (assoc-in [:stream-router :default :application-id] (rand-application-id))
                                                                                          (assoc-in [:stream-router :default :changelog-topic-replication-factor] changelog-topic-replication-factor)
                                                                                          (assoc-in [:stream-router :using-string-serde :application-id] (rand-application-id))
                                                                                          (assoc-in [:stream-router :using-string-serde :changelog-topic-replication-factor] changelog-topic-replication-factor))))
                                                         :stop  (fn [] (stop-streams ziggurat.streams/stream))}}))
  (poll-to-check-if-running ziggurat.streams/stream)
  (stop-stream :default)
  (is (not= (.state (get ziggurat.streams/stream :default)) KafkaStreams$State/RUNNING))
  (is (not= (.state (get ziggurat.streams/stream :using-string-serde)) KafkaStreams$State/NOT_RUNNING)))

(deftest stop-duplicate-stream-test
  (let [message-received-count (atom 0)
        mapped-fn              (get-mapped-fn message-received-count)
        handler-fn             (default-middleware/protobuf->hash mapped-fn proto-class :default)
        _                      (start-mount)]
    (mount/start-with-states {#'ziggurat.streams/stream {:start (fn [] (start-streams {:default {:handler-fn handler-fn}}
                                                                                      (ziggurat-config)))
                                                         :stop  (fn [] (stop-streams ziggurat.streams/stream))}}))
  (poll-to-check-if-running ziggurat.streams/stream)
  (stop-stream :default)
  (is (not= (.state (get ziggurat.streams/stream :default)) KafkaStreams$State/RUNNING))
  (stop-stream :default))

(deftest stop-invalid-stream-test
  (let [is-close-called (atom 0)
        mapped-fn       (get-mapped-fn is-close-called)
        handler-fn      (default-middleware/protobuf->hash mapped-fn proto-class :default)
        _               (start-mount)]
    (mount/start-with-states {#'ziggurat.streams/stream {:start (fn [] (start-streams {:default {:handler-fn handler-fn}}
                                                                                      (ziggurat-config)))
                                                         :stop  (fn [] (stop-streams ziggurat.streams/stream))}})
    (poll-to-check-if-running ziggurat.streams/stream)
    (with-redefs [ziggurat.streams/close-stream (fn [] (swap! is-close-called inc))]
      (stop-stream :invalid-topic-entity)
      (is (= @is-close-called 0)))))

(deftest start-invalid-stream-test
  (let [is-close-called (atom 0)
        mapped-fn       (get-mapped-fn is-close-called)
        handler-fn      (default-middleware/protobuf->hash mapped-fn proto-class :default)
        _               (start-mount)]
    (mount/start-with-states {#'ziggurat.streams/stream {:start (fn [] (start-streams {:default {:handler-fn handler-fn}}
                                                                                      (ziggurat-config)))
                                                         :stop  (fn [] (stop-streams ziggurat.streams/stream))}}))
  (let [is-close-called (atom 0)]
    (with-redefs [mount/start-with-states (fn [] (swap! is-close-called inc))]
      (start-stream :invalid-topic-entity)
      (is (= @is-close-called 0)))))

(deftest start-stream-joins-test-with-inner-join
  (testing "stream joins using inner join"
    (let [orig-config (ziggurat-config)]
      (with-redefs [ziggurat-config (fn [] (-> orig-config
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
                                                        (assoc-in [:stream-router :default :join-cfg] {:topic-and-another-test-topic {:join-window-ms 6000 :join-type :inner}})
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
          (is (= times @message-received-count)))))))

(deftest start-stream-joins-test-with-left-join
  (testing "stream joins using left join"
    (let [orig-config (ziggurat-config)]
      (with-redefs [ziggurat-config (fn [] (-> orig-config
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
                                                        (assoc-in [:stream-router :default :join-cfg] {:topic-and-another-test-topic {:join-window-ms 6000 :join-type :left}})
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
          (is (= times @message-received-count)))))))

(deftest start-stream-joins-test-with-outer-join
  (testing "stream joins using outer join"
    (let [orig-config (ziggurat-config)]
      (with-redefs [ziggurat-config (fn [] (-> orig-config
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
                                                        (assoc-in [:stream-router :default :join-cfg] {:topic-and-another-test-topic {:join-window-ms 6000 :join-type :outer}})
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
          (is (= times @message-received-count)))))))

(deftest start-stream-joins-test-alpha-features-test
  (testing "stream-joins should not start if :alpha-features for stream-joins is `false`"
    (let [original-config (ziggurat-config)]
      (with-redefs [ziggurat-config (fn [] (-> original-config
                                               (assoc-in [:alpha-features :stream-joins] false)))]
        (let [handler-fn (constantly nil)
              streams    (start-streams {:default {:handler-fn handler-fn}}
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

(deftest add-remove-stream-thread-test
  (let [message-received-count           (atom 0)
        mapped-fn                        (get-mapped-fn message-received-count)
        times                            6
        kvs                              (repeat times message-key-value)
        handler-fn                       (default-middleware/protobuf->hash mapped-fn proto-class :default)
        streams                          (start-streams {:default {:handler-fn handler-fn}}
                                                        (-> (ziggurat-config)
                                                            (assoc-in [:stream-router :default :application-id] (rand-application-id))
                                                            (assoc-in [:stream-router :default :changelog-topic-replication-factor] changelog-topic-replication-factor)))
        _                                (Thread/sleep 20000) ;;waiting for streams to start
        stream-thread-count-before-add   (get-stream-thread-count :default)
        _                                (add-stream-thread :default)
        stream-thread-count-after-add    (get-stream-thread-count :default)
        _                                (is (= stream-thread-count-after-add (+ stream-thread-count-before-add 1)))
        _                                (remove-stream-thread :default)
        stream-thread-count-after-remove (get-stream-thread-count :default)
        _                                (is (= stream-thread-count-before-add stream-thread-count-after-remove))]
    (IntegrationTestUtils/produceKeyValuesSynchronously (get-in (ziggurat-config) [:stream-router :default :origin-topic])
                                                        kvs
                                                        (props)
                                                        (MockTime.))
    (Thread/sleep 5000)                                     ;;wating for streams to consume messages
    (stop-streams streams)
    (is (= times @message-received-count))))

(deftest remove-stream-thread-to-zero-test
  (let [message-received-count           (atom 0)
        mapped-fn                        (get-mapped-fn message-received-count)
        times                            0
        kvs                              (repeat times message-key-value)
        handler-fn                       (default-middleware/protobuf->hash mapped-fn proto-class :default)
        streams                          (start-streams {:default {:handler-fn handler-fn}}
                                                        (-> (ziggurat-config)
                                                            (assoc-in [:stream-router :default :application-id] (rand-application-id))
                                                            (assoc-in [:stream-router :default :changelog-topic-replication-factor] changelog-topic-replication-factor)))
        _                                (Thread/sleep 10000) ;;waiting for streams to start
        _                                (remove-stream-thread :default)
        stream-thread-count-after-remove (get-stream-thread-count :default)
        _                                (is (= stream-thread-count-after-remove 0))]
    (IntegrationTestUtils/produceKeyValuesSynchronously (get-in (ziggurat-config) [:stream-router :default :origin-topic])
                                                        kvs
                                                        (props)
                                                        (MockTime.))
    (Thread/sleep 5000)                                     ;;wating for streams to consume messages
    (stop-streams streams)
    (is (= times @message-received-count))))

(deftest remove-stream-thread-with-timeout-to-zero-test
  (let [message-received-count           (atom 0)
        mapped-fn                        (get-mapped-fn message-received-count)
        times                            0
        kvs                              (repeat times message-key-value)
        handler-fn                       (default-middleware/protobuf->hash mapped-fn proto-class :default)
        streams                          (start-streams {:default {:handler-fn handler-fn}}
                                                        (-> (ziggurat-config)
                                                            (assoc-in [:stream-router :default :application-id] (rand-application-id))
                                                            (assoc-in [:stream-router :default :changelog-topic-replication-factor] changelog-topic-replication-factor)))
        _                                (Thread/sleep 10000) ;;waiting for streams to start
        _                                (remove-stream-thread :default 5000)
        stream-thread-count-after-remove (get-stream-thread-count :default)
        _                                (is (= stream-thread-count-after-remove 0))]
    (IntegrationTestUtils/produceKeyValuesSynchronously (get-in (ziggurat-config) [:stream-router :default :origin-topic])
                                                        kvs
                                                        (props)
                                                        (MockTime.))
    (Thread/sleep 5000)                                     ;;wating for streams to consume messages
    (stop-streams streams)
    (is (= times @message-received-count))))

(deftest handle-uncaught-exception-test
  (let [t (Throwable. "foobar")]
    (testing "should return SHUTDOWN_CLIENT"
      (let [r (handle-uncaught-exception :shutdown-client t)]
        (is (= r StreamsUncaughtExceptionHandler$StreamThreadExceptionResponse/SHUTDOWN_CLIENT))))
    (testing "should return SHUTDOWN_APPLICATION"
      (let [r (handle-uncaught-exception :shutdown-application t)]
        (is (= r StreamsUncaughtExceptionHandler$StreamThreadExceptionResponse/SHUTDOWN_APPLICATION))))
    (testing "should return REPLACE_THREAD"
      (let [r (handle-uncaught-exception :replace-thread t)]
        (is (= r StreamsUncaughtExceptionHandler$StreamThreadExceptionResponse/REPLACE_THREAD))))))
