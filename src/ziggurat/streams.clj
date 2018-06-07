(ns ziggurat.streams
  (:require [clojure.tools.logging :as log]
            [flatland.protobuf.core :as proto]
            [mount.core :as mount :refer [defstate]]
            [lambda-common.metrics :as metrics]
            [ziggurat.sentry :refer [sentry-reporter]]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.mapper :as mpr]
            [ziggurat.kafka-delay :as kafka-delay])
  (:import [org.apache.kafka.clients.consumer ConsumerConfig]
           [org.apache.kafka.common.serialization Serdes]
           [org.apache.kafka.streams KafkaStreams StreamsConfig]
           [org.apache.kafka.streams.kstream KStreamBuilder ValueMapper KStream]
           [org.apache.kafka.streams.processor WallclockTimestampExtractor]
           (java.util.regex Pattern)))

(defn- properties []
  (let [stream-config (:stream-config (ziggurat-config))]
    {StreamsConfig/APPLICATION_ID_CONFIG            (:application-id stream-config)
     StreamsConfig/BOOTSTRAP_SERVERS_CONFIG         (:bootstrap-servers stream-config)
     StreamsConfig/NUM_STREAM_THREADS_CONFIG        (int (:stream-threads-count stream-config))
     StreamsConfig/KEY_SERDE_CLASS_CONFIG           (.getName (.getClass (Serdes/ByteArray)))
     StreamsConfig/VALUE_SERDE_CLASS_CONFIG         (.getName (.getClass (Serdes/ByteArray)))
     StreamsConfig/TIMESTAMP_EXTRACTOR_CLASS_CONFIG WallclockTimestampExtractor
     ConsumerConfig/AUTO_OFFSET_RESET_CONFIG        "latest"}))

(defn- log-and-report-metrics
  [message]
  (kafka-delay/calculate-and-report-kafka-delay message)
  (metrics/increment-count "message" "read")
  message)

(defn- value-mapper
  [f]
  (reify ValueMapper
    (apply [_ v] (f v))))

(defn- map-values
  [mapper-fn ^KStream stream-builder]
  (.mapValues stream-builder (value-mapper mapper-fn)))

(defn- protobuf->hash [message]
  (let [proto-klass (-> (ziggurat-config)
                        :proto-class
                        java.lang.Class/forName
                        proto/protodef)
        loaded-proto (proto/protobuf-load proto-klass message)
        proto-keys (-> proto-klass
                       proto/protobuf-schema
                       :fields
                       keys
                       loaded-proto)]
    (select-keys loaded-proto proto-keys)))

(defn- topology [mapper-fn]
  (let [builder (KStreamBuilder.)
        topic-pattern (Pattern/compile (-> (ziggurat-config) :stream-config :origin-topic))]
    (->> (.stream builder topic-pattern)
         (map-values protobuf->hash)
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
