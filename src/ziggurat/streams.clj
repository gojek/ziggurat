(ns ziggurat.streams
  (:require [clojure.tools.logging :as log]
            [flatland.protobuf.core :as proto]
            [mount.core :as mount :refer [defstate]]
            [lambda-common.metrics :as metrics]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.mapper :as mpr]
            [ziggurat.kafka-delay :as kafka-delay])
  (:import [org.apache.kafka.clients.consumer ConsumerConfig]
           [org.apache.kafka.common.serialization Serdes]
           [org.apache.kafka.streams KafkaStreams StreamsConfig]
           [org.apache.kafka.streams.state Stores]
           [org.apache.kafka.streams.kstream KStreamBuilder ValueMapper KStream TransformerSupplier]
           [org.apache.kafka.streams.processor WallclockTimestampExtractor]
           [java.util.regex Pattern]
           [org.apache.kafka.streams.processor StateStoreSupplier]
           [java.util HashMap]))

(defn- properties [{:keys [application-id bootstrap-servers stream-threads-count]}]
  {StreamsConfig/APPLICATION_ID_CONFIG                    application-id
   StreamsConfig/BOOTSTRAP_SERVERS_CONFIG                 bootstrap-servers
   StreamsConfig/NUM_STREAM_THREADS_CONFIG                (int stream-threads-count)
   StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG           (.getName (.getClass (Serdes/ByteArray)))
   StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG         (.getName (.getClass (Serdes/ByteArray)))
   StreamsConfig/DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG WallclockTimestampExtractor
   ConsumerConfig/AUTO_OFFSET_RESET_CONFIG                "latest"})

(defn- get-metric-namespace [default topic]
  (str (name topic) "." default))

(defn- log-and-report-metrics [topic-entity message]
  (let [message-read-metric-namespace (get-metric-namespace "message" topic-entity)]
    (metrics/increment-count message-read-metric-namespace "read"))
  message)

(defn state-store-supplier []
  (-> (Stores/create "state-store")
      (.withKeys (Serdes/ByteArray))
      (.withValues (Serdes/ByteArray))
      (.inMemory)
      (.build)))

(defn- value-mapper [f]
  (reify ValueMapper
    (apply [_ v] (f v))))

(defn- map-values [mapper-fn ^KStream stream-builder]
  (.mapValues stream-builder (value-mapper mapper-fn)))

(defn- transformer-supplier [metric-namespace]
  (reify TransformerSupplier
    (get [_] (kafka-delay/create-transformer metric-namespace))))

(defn- transform-values [topic-entity stream-builder]
  (let [metric-namespace (get-metric-namespace "message-received-delay-histogram" topic-entity)]
    (.transform stream-builder (transformer-supplier metric-namespace) (into-array [(.name (state-store-supplier))]))))

(defn- protobuf->hash [message proto-class]
  (try
    (let [proto-klass  (-> proto-class
                           java.lang.Class/forName
                           proto/protodef)
          loaded-proto (proto/protobuf-load proto-klass message)
          proto-keys   (-> proto-klass
                           proto/protobuf-schema
                           :fields
                           keys)]
      (select-keys loaded-proto proto-keys))
    (catch Throwable e
      (metrics/increment-count "message-parsing" "failed")
      nil)))

(defn- topology [handler-fn {:keys [origin-topic proto-class]} topic-entity]
  (let [builder       (KStreamBuilder.)
        topic-pattern (Pattern/compile origin-topic)]
    (.addStateStore builder (state-store-supplier) nil)
    (->> (.stream builder topic-pattern)
         (transform-values topic-entity)
         (map-values #(protobuf->hash % proto-class))
         (map-values #(log-and-report-metrics topic-entity %))
         (map-values #((mpr/mapper-func handler-fn) % topic-entity)))
    builder))

(defn- start-stream* [handler-fn stream-config topic-entity]
  (KafkaStreams. ^KStreamBuilder (topology handler-fn stream-config topic-entity)
                 (StreamsConfig. (properties stream-config))))

(defn start-streams [stream-routes]
  (let [zig-conf (ziggurat-config)]
    (reduce-kv (fn [streams topic-entity topic-handler]
                 (let [stream-config (get-in zig-conf [:stream-router topic-entity])
                       handler-fn    (get-in topic-handler [:handler-fn])
                       stream        (start-stream* handler-fn stream-config (name topic-entity))]
                   (.start stream)
                   (conj streams stream)))
               []
               stream-routes)))

(defn stop-streams [streams]
  (doseq [stream streams]
    (.close ^KStream stream)))

(defstate stream
  :start (do (log/info "Starting Kafka stream")
             (start-streams (:ziggurat.init/stream-routes (mount/args))))
  :stop (do (log/info "Stopping Kafka stream")
            (stop-streams stream)))
