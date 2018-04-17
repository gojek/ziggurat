(ns ziggurat.streams
  (:require [clojure.tools.logging :as log]
            [flatland.protobuf.core :as proto]
            [mount.core :refer [defstate]]
            [ziggurat.sentry :refer [sentry-reporter]]
            [ziggurat.config :refer [config]]
            [ziggurat.mapper :as mpr]
            [sentry.core :as sentry]
            [ziggurat.kafka-delay :as kafka-delay]
            [mount.core :as mount])
  (:import [com.gojek.esb.booking BookingLogMessage]
           [org.apache.kafka.clients.consumer ConsumerConfig]
           [org.apache.kafka.common.serialization Serdes]
           [org.apache.kafka.streams KafkaStreams StreamsConfig]
           [org.apache.kafka.streams.kstream KStreamBuilder Predicate Reducer ValueMapper KStream]
           (java.util.regex Pattern)))

(defn properties []
  (let [stream-config (:stream-config config)]
    {StreamsConfig/APPLICATION_ID_CONFIG     (:application-id stream-config)
     StreamsConfig/BOOTSTRAP_SERVERS_CONFIG  (:bootstrap-servers stream-config)
     StreamsConfig/NUM_STREAM_THREADS_CONFIG (int (:stream-threads-count stream-config))
     StreamsConfig/KEY_SERDE_CLASS_CONFIG    (.getName (.getClass (Serdes/ByteArray)))
     StreamsConfig/VALUE_SERDE_CLASS_CONFIG  (.getName (.getClass (Serdes/ByteArray)))
     ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "latest"}))

(defn wrap-try
  [mapper-fn]
  (fn [booking]
    (try (mapper-fn booking)
         (catch Throwable e
           (log/error e "Unhandled exception")
           (sentry/report-error sentry-reporter e "Lambda Framework failed")))))

(defn log-and-report-metrics
  [message]
  (kafka-delay/calculate-and-report-kafka-delay message)
  message)

(defn value-mapper
  [f]
  (reify ValueMapper
    (apply [_ v] (f v))))

(defn map-values
  [mapper-fn ^KStream stream-builder]
  (.mapValues stream-builder (value-mapper (wrap-try mapper-fn))))

(defn topology [mapper-fn]
  (let [builder (KStreamBuilder.)
        topic-pattern (Pattern/compile (-> config :stream-config :origin-topic))]
    (->> (.stream builder topic-pattern)
         (map-values #(proto/protobuf-load (proto/protodef (java.lang.Class/forName (:proto-class config))) %))
         (map-values log-and-report-metrics)
         (map-values (mpr/mapper-func mapper-fn)))
    builder))

(defn start-stream [mapper-fn]
  (let [stream (KafkaStreams. ^KStreamBuilder (topology mapper-fn)
                              (StreamsConfig. (properties)))]
    (.start stream)
    stream))

(defn stop-stream [^KafkaStreams stream]
  (.close stream))

(defstate stream
  :start (do (log/info "Starting Kafka stream")
             (start-stream (:ziggurat.init/mapper-fn (mount/args))))
  :stop (do (log/info "Stopping Kafka stream")
            (stop-stream stream)))
