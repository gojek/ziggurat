(ns ziggurat.streams
  (:require [clojure.tools.logging :as log]
            [flatland.protobuf.core :as proto]
            [mount.core :refer [defstate]]
            [lambda-common.metrics :as metrics]
            [ziggurat.sentry :refer [sentry-reporter]]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.mapper :as mpr]
            [sentry.core :as sentry]
            [ziggurat.kafka-delay :as kafka-delay]
            [mount.core :as mount])
  (:import [org.apache.kafka.clients.consumer ConsumerConfig]
           [org.apache.kafka.common.serialization Serdes]
           [org.apache.kafka.streams KafkaStreams StreamsConfig]
           [org.apache.kafka.streams.kstream KStreamBuilder ValueMapper KStream]
           [org.apache.kafka.streams.processor WallclockTimestampExtractor]
           (java.util.regex Pattern)))

(defn- properties [{:keys [application-id bootstrap-servers stream-threads-count]}]
  {StreamsConfig/APPLICATION_ID_CONFIG            application-id
   StreamsConfig/BOOTSTRAP_SERVERS_CONFIG         bootstrap-servers
   StreamsConfig/NUM_STREAM_THREADS_CONFIG        (int stream-threads-count)
   StreamsConfig/KEY_SERDE_CLASS_CONFIG           (.getName (.getClass (Serdes/ByteArray)))
   StreamsConfig/VALUE_SERDE_CLASS_CONFIG         (.getName (.getClass (Serdes/ByteArray)))
   StreamsConfig/TIMESTAMP_EXTRACTOR_CLASS_CONFIG WallclockTimestampExtractor
   ConsumerConfig/AUTO_OFFSET_RESET_CONFIG        "latest"})

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

(defn- topology [mapper-fn {:keys [origin-topic proto-class]}]
  (let [builder (KStreamBuilder.)
        topic-pattern (Pattern/compile origin-topic)]
    (->> (.stream builder topic-pattern)
         (map-values #(proto/protobuf-load (proto/protodef (java.lang.Class/forName proto-class)) %))
         (map-values log-and-report-metrics)
         (map-values (mpr/mapper-func mapper-fn)))
    builder))

(defn- start-stream* [mapper-fn stream-config]
  (KafkaStreams. ^KStreamBuilder (topology mapper-fn stream-config)
                 (StreamsConfig. (properties stream-config))))

(defn start-streams [{:keys [stream-routes mapper-fn]}]
  (let [zig-conf (ziggurat-config)]
    (if (nil? stream-routes)
      (let [stream-config (-> zig-conf
                              :stream-config
                              (assoc :proto-class (:proto-class zig-conf)))
            stream (start-stream* mapper-fn stream-config)]
        (.start stream)
        [stream])
      (reduce (fn [streams route]
                (let [topic-entity (first (keys route))
                      stream-config (get-in zig-conf [:stream-router-configs topic-entity])
                      mapper-fn (get-in route [topic-entity :main-fn])
                      stream (start-stream* mapper-fn stream-config)]
                  (.start stream)
                  (conj streams stream)))
              []
              stream-routes))))

(defn stop-streams [streams]
  (doseq [stream streams]
    (.close ^KStream stream)))

(defstate stream
  :start (do (log/info "Starting Kafka stream")
             (start-streams (:ziggurat.init/stream-args (mount/args))))
  :stop (do (log/info "Stopping Kafka stream")
            (stop-streams stream)))
