(ns ziggurat.streams
  (:require [clojure.tools.logging :as log]
            [flatland.protobuf.core :as proto]
            [mount.core :as mount :refer [defstate]]
            [ziggurat.metrics :as metrics]
            [ziggurat.config :refer [ziggurat-config]]
            [sentry-clj.async :as sentry]
            [ziggurat.mapper :as mpr]
            [ziggurat.channel :as chl]
            [ziggurat.kafka-delay :as kafka-delay]
            [ziggurat.sentry :refer [sentry-reporter]])
  (:import [java.util.regex Pattern]
           [java.util Properties]
           [org.apache.kafka.clients.consumer ConsumerConfig]
           [org.apache.kafka.common.serialization Serdes]
           [org.apache.kafka.streams KafkaStreams StreamsConfig StreamsBuilder Topology]
           [org.apache.kafka.streams.kstream ValueMapper TransformerSupplier]
           [org.apache.kafka.streams.state.internals KeyValueStoreBuilder RocksDbKeyValueBytesStoreSupplier]
           [org.apache.kafka.common.utils SystemTime]
           [ziggurat.kafka_delay IngestionTimeExtractor]))

(defn- properties [{:keys [application-id bootstrap-servers stream-threads-count auto-offset-reset-config buffered-records-per-partition commit-interval-ms]}]
  (if-not (contains? #{"latest" "earliest" nil} auto-offset-reset-config)
    (throw (ex-info "Stream offset can only be latest or earliest" {:offset auto-offset-reset-config})))
  (doto (Properties.)
    (.put StreamsConfig/APPLICATION_ID_CONFIG application-id)
    (.put StreamsConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap-servers)
    (.put StreamsConfig/NUM_STREAM_THREADS_CONFIG (int stream-threads-count))
    (.put StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG (.getName (.getClass (Serdes/ByteArray))))
    (.put StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG (.getName (.getClass (Serdes/ByteArray))))
    (.put StreamsConfig/DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG IngestionTimeExtractor)
    (.put StreamsConfig/BUFFERED_RECORDS_PER_PARTITION_CONFIG (int (or buffered-records-per-partition 10000)))
    (.put StreamsConfig/COMMIT_INTERVAL_MS_CONFIG (int (or commit-interval-ms 15000)))
    (.put ConsumerConfig/AUTO_OFFSET_RESET_CONFIG (or auto-offset-reset-config "latest"))))

(defn- get-metric-namespace [default topic]
  (str (name topic) "." default))

(defn- log-and-report-metrics [topic-entity message]
  (let [message-read-metric-namespace (get-metric-namespace "message" (name topic-entity))]
    (metrics/increment-count message-read-metric-namespace "read"))
  message)

(defn store-supplier-builder []
  (KeyValueStoreBuilder. (RocksDbKeyValueBytesStoreSupplier. "state-store")
                         (Serdes/ByteArray)
                         (Serdes/ByteArray)
                         (SystemTime.)))

(defn- value-mapper [f]
  (reify ValueMapper
    (apply [_ v] (f v))))

(defn- map-values [mapper-fn stream-builder]
  (.mapValues stream-builder (value-mapper mapper-fn)))

(defn- transformer-supplier [metric-namespace process-message-since-in-s]
  (reify TransformerSupplier
    (get [_] (kafka-delay/create-transformer metric-namespace process-message-since-in-s))))

(defn- transform-values [topic-entity skip-before-time stream-builder]
  (let [metric-namespace (get-metric-namespace "message-received-delay-histogram" topic-entity)]
    (.transform stream-builder (transformer-supplier metric-namespace skip-before-time) (into-array [(.name (store-supplier-builder))]))))

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
      (sentry/report-error sentry-reporter e (str "Couldn't parse the message with proto - " proto-class))
      (metrics/increment-count "message-parsing" "failed")
      nil)))

(defn- topology [handler-fn {:keys [origin-topic proto-class process-message-since-in-s]} topic-entity channels]
  (let [builder           (StreamsBuilder.)
        topic-entity-name (name topic-entity)
        topic-pattern     (Pattern/compile origin-topic)]
    (.addStateStore builder (store-supplier-builder))
    (->> (.stream builder topic-pattern)
         (transform-values topic-entity-name (or process-message-since-in-s 3600))
         (map-values #(protobuf->hash % proto-class))
         (map-values #(log-and-report-metrics topic-entity-name %))
         (map-values #((mpr/mapper-func handler-fn topic-entity channels) %)))
    (.build builder)))

(defn- start-stream* [handler-fn stream-config topic-entity channels]
  (KafkaStreams. ^Topology (topology handler-fn stream-config topic-entity channels)
                 ^Properties (properties stream-config)))

(defn start-streams
  ([stream-routes]
   (start-streams stream-routes (ziggurat-config)))
  ([stream-routes stream-configs]
   (reduce (fn [streams stream]
             (let [topic-entity (first stream)
                   topic-handler-fn (-> stream second :handler-fn)
                   channels (chl/get-keys-for-topic stream-routes topic-entity)
                   stream-config (get-in stream-configs [:stream-router topic-entity])
                   stream (start-stream* topic-handler-fn stream-config topic-entity channels)]
               (.start stream)
               (conj streams stream)))
           []
           stream-routes)))

(defn stop-streams [streams]
  (doseq [stream streams]
    (.close stream)))

(defstate stream
  :start (do (log/info "Starting Kafka stream")
             (start-streams (:stream-routes (mount/args)) (ziggurat-config)))
  :stop (do (log/info "Stopping Kafka stream")
            (stop-streams stream)))