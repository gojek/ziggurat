(ns ziggurat.streams
  (:require [clojure.tools.logging :as log]
            [flatland.protobuf.core :as proto]
            [mount.core :as mount :refer [defstate]]
            [sentry-clj.async :as sentry]
            [ziggurat.channel :as chl]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.mapper :as mpr]
            [ziggurat.metrics :as metrics]
            [ziggurat.sentry :refer [sentry-reporter]]
            [ziggurat.timestamp-transformer :as transformer]
            [ziggurat.util.map :as umap])
  (:import [java.util Properties]
           [java.util.regex Pattern]
           [org.apache.kafka.clients.consumer ConsumerConfig]
           [org.apache.kafka.common.serialization Serdes]
           [org.apache.kafka.common.utils SystemTime]
           [org.apache.kafka.streams KafkaStreams StreamsConfig StreamsBuilder Topology]
           [org.apache.kafka.streams.kstream ValueMapper TransformerSupplier]
           [org.apache.kafka.streams.state.internals KeyValueStoreBuilder RocksDbKeyValueBytesStoreSupplier]
           [ziggurat.timestamp_transformer IngestionTimeExtractor]))

(def default-config-for-stream
  {:buffered-records-per-partition     10000
   :commit-interval-ms                 15000
   :auto-offset-reset-config           "latest"
   :oldest-processed-message-in-s      604800
   :changelog-topic-replication-factor 3})

(defn- set-upgrade-from-config
  "Populates the upgrade.from config in kafka streams required for upgrading kafka-streams version from 1 to 2. If the
  value is non-nil it sets the config (the value validation is done in the kafka streams code), to unset the value the
  config needs to be set as nil "
  [properties upgrade-from-config]
  (if (some? upgrade-from-config)
    (.put properties StreamsConfig/UPGRADE_FROM_CONFIG upgrade-from-config)))

(defn- validate-auto-offset-reset-config
  [auto-offset-reset-config]
  (if-not (contains? #{"latest" "earliest" nil} auto-offset-reset-config)
    (throw (ex-info "Stream offset can only be latest or earliest" {:offset auto-offset-reset-config}))))

(defn- properties [{:keys [application-id
                           bootstrap-servers
                           stream-threads-count
                           auto-offset-reset-config
                           buffered-records-per-partition
                           commit-interval-ms
                           upgrade-from
                           changelog-topic-replication-factor]}]
  (validate-auto-offset-reset-config auto-offset-reset-config)
  (doto (Properties.)
    (.put StreamsConfig/APPLICATION_ID_CONFIG application-id)
    (.put StreamsConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap-servers)
    (.put StreamsConfig/NUM_STREAM_THREADS_CONFIG (int stream-threads-count))
    (.put StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG (.getName (.getClass (Serdes/ByteArray))))
    (.put StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG (.getName (.getClass (Serdes/ByteArray))))
    (.put StreamsConfig/DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG IngestionTimeExtractor)
    (.put StreamsConfig/BUFFERED_RECORDS_PER_PARTITION_CONFIG (int buffered-records-per-partition))
    (.put StreamsConfig/COMMIT_INTERVAL_MS_CONFIG commit-interval-ms)
    (.put StreamsConfig/REPLICATION_FACTOR_CONFIG (int changelog-topic-replication-factor))
    (.put ConsumerConfig/AUTO_OFFSET_RESET_CONFIG auto-offset-reset-config)
    (set-upgrade-from-config upgrade-from)))

(defn- log-and-report-metrics [topic-entity message]
  (let [service-name       (:app-name (ziggurat-config))
        topic-entity-name  (name topic-entity)
        additional-tags    {:actor service-name :topic_name topic-entity-name}
        default-namespace  "message"
        metric-namespaces  [service-name topic-entity-name default-namespace]
        default-namespaces [default-namespace]
        metric             "read"
        multi-namespaces   [metric-namespaces default-namespaces]]
    (metrics/multi-ns-increment-count multi-namespaces metric additional-tags))
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

(defn- transformer-supplier
  [metric-namespaces oldest-processed-message-in-s additional-tags]
  (reify TransformerSupplier
    (get [_] (transformer/create metric-namespaces oldest-processed-message-in-s additional-tags))))

(defn- transform-values [topic-entity-name oldest-processed-message-in-s stream-builder]
  (let [service-name      (:app-name (ziggurat-config))
        metric-namespaces [service-name topic-entity-name "message-received-delay-histogram"]
        additional-tags   {:actor service-name :topic_name topic-entity-name}]
    (.transform stream-builder (transformer-supplier metric-namespaces oldest-processed-message-in-s additional-tags) (into-array [(.name (store-supplier-builder))]))))

(defn- protobuf->hash [message proto-class topic-entity-name]
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
      (let [service-name      (:app-name (ziggurat-config))
            additional-tags   {:actor service-name :topic_name topic-entity-name}
            default-namespace "message-parsing"
            metric-namespaces [service-name default-namespace]
            multi-namespaces  [metric-namespaces [default-namespace]]]
        (sentry/report-error sentry-reporter e (str "Couldn't parse the message with proto - " proto-class))
        (metrics/multi-ns-increment-count multi-namespaces "failed" additional-tags)
        nil))))

(defn- topology [handler-fn {:keys [origin-topic proto-class oldest-processed-message-in-s]} topic-entity channels]
  (let [builder           (StreamsBuilder.)
        topic-entity-name (name topic-entity)
        topic-pattern     (Pattern/compile origin-topic)]
    (.addStateStore builder (store-supplier-builder))
    (->> (.stream builder topic-pattern)
         (transform-values topic-entity-name oldest-processed-message-in-s)
         (map-values #(protobuf->hash % proto-class topic-entity-name))
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
             (let [topic-entity     (first stream)
                   topic-handler-fn (-> stream second :handler-fn)
                   channels         (chl/get-keys-for-topic stream-routes topic-entity)
                   stream-config    (-> stream-configs
                                        (get-in [:stream-router topic-entity])
                                        (umap/deep-merge default-config-for-stream))
                   stream           (start-stream* topic-handler-fn stream-config topic-entity channels)]
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
