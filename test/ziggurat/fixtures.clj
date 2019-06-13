(ns ziggurat.fixtures
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [clojure.stacktrace :as st]
            [mount.core :as mount]
            [ziggurat.config :as config]
            [ziggurat.messaging.util :as util]
            [ziggurat.messaging.connection :refer [connection]]
            [ziggurat.server :refer [server]]
            [ziggurat.messaging.producer :as pr]
            [ziggurat.producer :as producer]
            [langohr.channel :as lch]
            [langohr.exchange :as le]
            [langohr.queue :as lq])
  (:import (org.apache.kafka.streams.integration.utils EmbeddedKafkaCluster)
           (java.util Properties)
           (org.apache.kafka.clients.producer ProducerConfig)
           (org.apache.kafka.clients.consumer ConsumerConfig)))

(defn mount-config []
  (-> (mount/only [#'config/config])
      (mount/swap {#'config/config (config/config-from-env "config.test.edn")})
      (mount/start)))

(defn mount-only-config [f]
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
  (with-open [ch (lch/open connection)]
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
  (with-open [ch (lch/open connection)]
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
     (mount/only [#'connection])
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
     (catch Exception e#
       (st/print-stack-trace e#))
     (finally
       (delete-queues ~stream-routes)
       (delete-exchanges ~stream-routes))))

(defn mount-producer []
  (-> (mount/only [#'producer/kafka-producers])
      (mount/start)))

(defn construct-embedded-kafka-cluster []
  (doto (EmbeddedKafkaCluster. 1)
    (.start)))

(def ^:dynamic *embedded-kafka-cluster* nil)
(def ^:dynamic *bootstrap-servers* nil)
(def ^:dynamic *consumer-properties* nil)
(def ^:dynamic *producer-properties* nil)

(defn mount-only-config-and-producer [f]
  (do
    (mount-config)
    (mount-producer)
    (binding [*embedded-kafka-cluster* (construct-embedded-kafka-cluster)]
      (binding [*bootstrap-servers* (.bootstrapServers *embedded-kafka-cluster*)]
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
          (f)))))
  (mount/stop))