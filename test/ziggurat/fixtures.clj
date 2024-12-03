(ns ziggurat.fixtures
  (:require [clojure.tools.logging :as log]
            [langohr.channel :as lch]
            [langohr.exchange :as le]
            [langohr.queue :as lq]
            [mount.core :as mount]
            [ziggurat.config :as config]
            [ziggurat.kafka-consumer.executor-service :refer [thread-pool]]
            [ziggurat.messaging.producer-connection :refer [producer-connection]]
            [ziggurat.messaging.consumer]
            [ziggurat.messaging.consumer-connection :refer [consumer-connection]]
            [ziggurat.messaging.channel-pool :refer [channel-pool]]
            [ziggurat.messaging.producer :as pr]
            [ziggurat.messaging.util :as util]
            [ziggurat.metrics :as metrics]
            [ziggurat.producer :as producer]
            [ziggurat.server :refer [server]])
  (:import (java.util Properties)
           (org.apache.kafka.clients.consumer ConsumerConfig)
           (org.apache.kafka.clients.producer ProducerConfig))
  (:gen-class
   :methods [^{:static true} [mountConfig [] void]
             ^{:static true} [mountProducer [] void]
             ^{:static true} [unmountAll [] void]]
   :name tech.gojek.ziggurat.internal.test.Fixtures))

(def test-config-file-name "config.test.edn")

(def ^:private bootstrap-servers
  (if (= (System/getenv "TESTING_TYPE") "local")
    "localhost:9092"
    "localhost:9092,localhost:9093,localhost:9094"))

(defn- get-default-or-cluster-config
  [m]
  (let [keys [[:ziggurat :stream-router :default :bootstrap-servers]
              [:ziggurat :stream-router :using-string-serde :bootstrap-servers]
              [:ziggurat :batch-routes :consumer-1 :bootstrap-servers]
              [:ziggurat :batch-routes :consumer-2 :bootstrap-servers]
              [:ziggurat :stream-router :default :producer :bootstrap-servers]]]
    (reduce (fn [s k]
              (assoc-in s k bootstrap-servers))
            m
            keys)))

(defn mount-config []
  (-> (mount/only [#'config/config])
      (mount/swap {#'config/config (get-default-or-cluster-config (config/config-from-env test-config-file-name))})
      (mount/start)))

(defn mount-only-config [f]
  (mount-config)
  (f)
  (mount/stop))

(defn mount-test-thread-pool [f]
  (-> (mount/only [#'thread-pool])
      (mount/start))
  (f)
  (-> (mount/only [#'thread-pool])
      (mount/stop)))

(defn mount-metrics [f]
  (mount/start (mount/only [#'metrics/metrics-reporter]))
  (f)
  (mount/stop #'metrics/metrics-reporter))

(defn mount-config-with-tracer [f]
  (mount-config)
  (f)
  (mount/stop))

(defn silence-logging
  [f]
  (with-redefs [log/log* (constantly nil)]
    (f)))

(defn- get-queue-name [queue-type]
  (:queue-name (queue-type (config/rabbitmq-config))))

(defn- get-exchange-name [exchange-type]
  (:exchange-name (exchange-type (config/rabbitmq-config))))

(defn delete-queues [stream-routes]
  (with-open [ch (lch/open producer-connection)]
    (doseq [topic-entity (keys stream-routes)]
      (let [topic-identifier (name topic-entity)
            channels         (util/get-channel-names stream-routes topic-entity)]
        (lq/delete ch (util/prefixed-queue-name topic-identifier (get-queue-name :instant)))
        (lq/delete ch (util/prefixed-queue-name topic-identifier (get-queue-name :dead-letter)))
        (lq/delete ch (pr/delay-queue-name topic-identifier (get-queue-name :delay)))
        (doseq [channel channels]
          (lq/delete ch (util/prefixed-channel-name topic-identifier channel (get-queue-name :instant)))
          (lq/delete ch (util/prefixed-channel-name topic-identifier channel (get-queue-name :dead-letter)))
          (lq/delete ch (util/prefixed-channel-name topic-identifier channel (get-queue-name :delay))))))))

(defn delete-exchanges [stream-routes]
  (with-open [ch (lch/open producer-connection)]
    (doseq [topic-entity (keys stream-routes)]
      (let [topic-identifier (name topic-entity)
            channels         (util/get-channel-names stream-routes topic-entity)]
        (le/delete ch (util/prefixed-queue-name topic-identifier (get-exchange-name :instant)))
        (le/delete ch (util/prefixed-queue-name topic-identifier (get-exchange-name :dead-letter)))
        (le/delete ch (util/prefixed-queue-name topic-identifier (get-exchange-name :delay)))
        (doseq [channel channels]
          (lq/delete ch (util/prefixed-channel-name topic-identifier channel (get-exchange-name :instant)))
          (lq/delete ch (util/prefixed-channel-name topic-identifier channel (get-exchange-name :dead-letter)))
          (lq/delete ch (util/prefixed-channel-name topic-identifier channel (get-exchange-name :delay))))))))

(defn init-rabbit-mq [f]
  (let [stream-routes {:default {:handler-fn #(constantly nil)
                                 :channel-1  #(constantly nil)}}]
    (mount-config)
    (->
     (mount/only [#'producer-connection #'consumer-connection #'channel-pool])
     (mount/with-args {:stream-routes stream-routes})
     (mount/start))
    (f)
    (mount/stop)))

(defn with-start-server* [stream-routes f]
  (mount-config)
  (->
   (mount/only [#'server])
   (mount/with-args {:stream-routes stream-routes})
   (mount/start))
  (f)
  (mount/stop))

(defmacro with-start-server [stream-routes & body]
  `(with-start-server* ~stream-routes (fn [] ~@body)))

(defmacro with-queues [stream-routes body]
  `(try
     (pr/make-queues ~stream-routes)
     ~body
     (finally
       (delete-queues ~stream-routes)
       (delete-exchanges ~stream-routes))))

(defmacro with-config [body]
  `(do (mount-config)
       ~body
       (mount/stop #'config/config)))

(defn mount-producer []
  (-> (mount/only [#'producer/kafka-producers])
      (mount/start)))

(def ^:dynamic *bootstrap-servers* nil)
(def ^:dynamic *consumer-properties* nil)
(def ^:dynamic *producer-properties* nil)

(defn mount-producer-with-config-and-tracer [f]
  (mount-config)
  (mount-producer)
  (binding [*bootstrap-servers* (get-in (config/ziggurat-config) [:stream-router :default :bootstrap-servers])]
    (binding [*consumer-properties* (doto (Properties.)
                                      (.put ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG, *bootstrap-servers*)
                                      (.put ConsumerConfig/GROUP_ID_CONFIG, "ziggurat-consumer")
                                      (.put ConsumerConfig/AUTO_OFFSET_RESET_CONFIG, "earliest")
                                      (.put ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
                                      (.put ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer"))
              *producer-properties* (doto (Properties.)
                                      (.put ProducerConfig/BOOTSTRAP_SERVERS_CONFIG *bootstrap-servers*)
                                      (.put ProducerConfig/ACKS_CONFIG "all")
                                      (.put ProducerConfig/RETRIES_CONFIG (int 0))
                                      (.put ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer")
                                      (.put ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer"))]
      (f)))
  (mount/stop))

(defn unmount-all []
  (mount/stop))

(defn -mountConfig []
  (mount-config))

(defn -mountProducer []
  (mount-producer))

(defn -unmountAll []
  (unmount-all))
