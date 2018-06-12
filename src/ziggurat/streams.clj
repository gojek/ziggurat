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

(defn- properties [{:keys [application-id bootstrap-servers stream-threads-count]}]
  {StreamsConfig/APPLICATION_ID_CONFIG            application-id
   StreamsConfig/BOOTSTRAP_SERVERS_CONFIG         bootstrap-servers
   StreamsConfig/NUM_STREAM_THREADS_CONFIG        (int stream-threads-count)
   StreamsConfig/KEY_SERDE_CLASS_CONFIG           (.getName (.getClass (Serdes/ByteArray)))
   StreamsConfig/VALUE_SERDE_CLASS_CONFIG         (.getName (.getClass (Serdes/ByteArray)))
   StreamsConfig/TIMESTAMP_EXTRACTOR_CLASS_CONFIG WallclockTimestampExtractor
   ConsumerConfig/AUTO_OFFSET_RESET_CONFIG        "latest"})

(defn- get-metric-namespace [default topic]
  (if (nil? topic)
    default
    (str (name topic) "." default)))

(defn- log-and-report-metrics
  [topic message]
  (let [message-read-metric-namespace (get-metric-namespace "message" topic)
        message-delay-metric-namespace (get-metric-namespace "message-received-delay-histogram" topic)]
    (kafka-delay/calculate-and-report-kafka-delay message-delay-metric-namespace message)
    (metrics/increment-count message-read-metric-namespace "read"))
  message)

(defn- value-mapper
  [f]
  (reify ValueMapper
    (apply [_ v] (f v))))

(defn- map-values
  [mapper-fn ^KStream stream-builder]
  (.mapValues stream-builder (value-mapper mapper-fn)))

(defn- protobuf->hash [message]
  (try
    (let [proto-klass (-> (ziggurat-config)
                          :proto-class
                          java.lang.Class/forName
                          proto/protodef)
          loaded-proto (proto/protobuf-load proto-klass message)
          proto-keys (-> proto-klass
                         proto/protobuf-schema
                         :fields
                         keys)]
      (select-keys loaded-proto proto-keys))
    (catch Throwable e
      (metrics/increment-count "message-parsing" "failed")
      nil)))

(defn- topology [mapper-fn {:keys [origin-topic proto-class]} topic-entity]
  (let [builder (KStreamBuilder.)
        topic-pattern (Pattern/compile origin-topic)]
    (->> (.stream builder topic-pattern)
         (map-values protobuf->hash)
         (map-values #(log-and-report-metrics topic-entity %))
         (map-values #((mpr/mapper-func mapper-fn) % topic-entity)))
    builder))

(defn- start-stream* [mapper-fn stream-config topic-entity]
  (KafkaStreams. ^KStreamBuilder (topology mapper-fn stream-config topic-entity)
                 (StreamsConfig. (properties stream-config))))

(defn start-streams [{:keys [stream-routes mapper-fn]}]
  (let [zig-conf (ziggurat-config)]
    (if (nil? stream-routes)
      (let [stream-config (-> zig-conf
                              :stream-config
                              (assoc :proto-class (:proto-class zig-conf)))
            stream (start-stream* mapper-fn stream-config nil)]
        (.start stream)
        [stream])
      (reduce (fn [streams route]
                (let [topic-entity  (first (keys route))
                      stream-config (get-in zig-conf [:stream-router-configs topic-entity])
                      mapper-fn (get-in route [topic-entity :handler-fn])
                      stream (start-stream* mapper-fn stream-config (name topic-entity))]
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
