(ns ziggurat.fixtures
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [clojure.stacktrace :as st]
            [mount.core :as mount]
            [ziggurat.config :as config]
            [ziggurat.messaging.util :as util]
            [ziggurat.messaging.rabbitmq-wrapper :as rmqw :refer [connection]]
            [ziggurat.messaging.messaging :as messaging]
            [ziggurat.server :refer [server]]
            [ziggurat.messaging.producer :as pr]
            [ziggurat.producer :as producer]
            [langohr.channel :as lch]
            [langohr.exchange :as le]
            [langohr.queue :as lq]
            [ziggurat.tracer :as tracer])
  (:import (java.util Properties)
           (org.apache.kafka.clients.producer ProducerConfig)
           (org.apache.kafka.clients.consumer ConsumerConfig)
           (io.opentracing.mock MockTracer))
  (:gen-class
   :name tech.gojek.ziggurat.internal.test.Fixtures
   :methods [^{:static true} [mountConfig [] void]
             ^{:static true} [mountProducer [] void]
             ^{:static true} [unmountAll [] void]]))

(defn mount-config []
  (-> (mount/only [#'config/config])
      (mount/swap {#'config/config (config/config-from-env "config.test.edn")})
      (mount/start)))

(defn mount-only-config [f]
  (mount-config)
  (f)
  (mount/stop))

(defn mount-metrics [f]
  (mount/start (mount/only [#'ziggurat.metrics/statsd-reporter]))
  (f)
  (mount/stop #'ziggurat.metrics/statsd-reporter))

(defn mount-tracer []
  (with-redefs [tracer/create-tracer (fn [] (MockTracer.))]
    (-> (mount/only [#'tracer/tracer])
        (mount/start))))

(defn mount-config-with-tracer [f]
  (mount-config)
  (mount-tracer)
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
  (with-open [ch (lch/open @connection)]
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
  (with-open [ch (lch/open @connection)]
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

(defn init-messaging [f]
  (let [stream-routes {:default {:handler-fn #(constantly nil)
                                 :channel-1  #(constantly nil)}}]
    (mount-config)
    (mount-tracer)
    (mount/start)                                           ;;TODO move it to mapper_test.clj, removal of this causes mapper_test to fail
    (messaging/start-connection config/config stream-routes)
    (f)
    (messaging/stop-connection config/config stream-routes)
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

(defn mount-producer []
  (-> (mount/only [#'producer/kafka-producers])
      (mount/start)))

(def ^:dynamic *bootstrap-servers* nil)
(def ^:dynamic *consumer-properties* nil)
(def ^:dynamic *producer-properties* nil)

(defn mount-producer-with-config-and-tracer [f]
  (do
    (mount-config)
    (mount-tracer)
    (mount-producer)
    (binding [*bootstrap-servers* "localhost:9092"]
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
        (f))))
  (mount/stop))

(defn unmount-all []
  (mount/stop))

(defn -mountConfig []
  (mount-config))

(defn -mountProducer []
  (mount-producer))

(defn -unmountAll []
  (unmount-all))
