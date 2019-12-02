(ns ziggurat.streams
  (:require [clojure.tools.logging :as log]
            [mount.core :as mount :refer [defstate]]
            [sentry-clj.async :as sentry]
            [ziggurat.channel :as chl]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.header-transformer :as header-transformer]
            [ziggurat.mapper :refer [mapper-func ->MessagePayload]]
            [ziggurat.metrics :as metrics]
            [ziggurat.timestamp-transformer :as timestamp-transformer]
            [ziggurat.util.map :as umap]
            [ziggurat.tracer :refer [tracer]])
  (:import [java.util Properties]
           [java.util.regex Pattern]
           [org.apache.kafka.clients.consumer ConsumerConfig]
           [org.apache.kafka.common.serialization Serdes]
           [org.apache.kafka.common.utils SystemTime]
           [org.apache.kafka.streams KafkaStreams StreamsConfig StreamsBuilder Topology]
           [org.apache.kafka.streams.kstream ValueMapper TransformerSupplier ValueTransformerSupplier]
           [org.apache.kafka.streams.state.internals KeyValueStoreBuilder RocksDbKeyValueBytesStoreSupplier]
           [ziggurat.timestamp_transformer IngestionTimeExtractor]
           [io.opentracing Tracer]
           [io.opentracing.contrib.kafka.streams TracingKafkaClientSupplier]
           [io.opentracing.contrib.kafka TracingKafkaUtils]
           [io.opentracing.tag Tags]))

(def default-config-for-stream
  {:buffered-records-per-partition     10000
   :commit-interval-ms                 15000
   :auto-offset-reset-config           "latest"
   :oldest-processed-message-in-s      604800
   :changelog-topic-replication-factor 3
   :default-key-serde                  "org.apache.kafka.common.serialization.Serdes$ByteArraySerde"
   :default-value-serde                "org.apache.kafka.common.serialization.Serdes$ByteArraySerde"})

(defn- set-upgrade-from-config
  "Populates the upgrade.from config in kafka streams required for upgrading kafka-streams version from 1 to 2. If the
  value is non-nil it sets the config (the value validation is done in the kafka streams code), to unset the value the
  config needs to be set as nil "
  [properties upgrade-from-config]
  (if (some? upgrade-from-config)
    (.put properties StreamsConfig/UPGRADE_FROM_CONFIG upgrade-from-config)))

(defn- set-encoding-config
  "Populates `key-deserializer-encoding`, `value-deserializer-encoding` and `deserializer-encoding`
   in `properties` only if non-nil."
  [properties key-deserializer-encoding value-deserializer-encoding deserializer-encoding]
  (let [KEY_DESERIALIZER_ENCODING "key.deserializer.encoding"
        VALUE_DESERIALIZER_ENCODING "value.deserializer.encoding"
        DESERIALIZER_ENCODING "deserializer.encoding"]
    (if (some? key-deserializer-encoding)
      (.put properties KEY_DESERIALIZER_ENCODING key-deserializer-encoding))
    (if (some? value-deserializer-encoding)
      (.put properties VALUE_DESERIALIZER_ENCODING value-deserializer-encoding))
    (if (some? deserializer-encoding)
      (.put properties DESERIALIZER_ENCODING deserializer-encoding))))

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
                           changelog-topic-replication-factor
                           default-key-serde
                           default-value-serde
                           key-deserializer-encoding
                           value-deserializer-encoding
                           deserializer-encoding]}]
  (validate-auto-offset-reset-config auto-offset-reset-config)
  (doto (Properties.)
    (.put StreamsConfig/APPLICATION_ID_CONFIG application-id)
    (.put StreamsConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap-servers)
    (.put StreamsConfig/NUM_STREAM_THREADS_CONFIG (int stream-threads-count))
    (.put StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG default-key-serde)
    (.put StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG default-value-serde)
    (.put StreamsConfig/DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG IngestionTimeExtractor)
    (.put StreamsConfig/BUFFERED_RECORDS_PER_PARTITION_CONFIG (int buffered-records-per-partition))
    (.put StreamsConfig/COMMIT_INTERVAL_MS_CONFIG commit-interval-ms)
    (.put StreamsConfig/REPLICATION_FACTOR_CONFIG (int changelog-topic-replication-factor))
    (.put ConsumerConfig/AUTO_OFFSET_RESET_CONFIG auto-offset-reset-config)
    (set-upgrade-from-config upgrade-from)
    (set-encoding-config key-deserializer-encoding value-deserializer-encoding deserializer-encoding)))

(defn- log-and-report-metrics [topic-entity message]
  (let [topic-entity-name (name topic-entity)
        additional-tags   {:topic_name topic-entity-name}
        default-namespace "message"
        metric            "read"]
    (metrics/increment-count default-namespace metric additional-tags))
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

(defn- timestamp-transformer-supplier
  [metric-namespaces oldest-processed-message-in-s additional-tags]
  (reify TransformerSupplier
    (get [_] (timestamp-transformer/create metric-namespaces oldest-processed-message-in-s additional-tags))))

(defn- header-transformer-supplier
  []
  (reify ValueTransformerSupplier
    (get [_] (header-transformer/create))))

(defn- timestamp-transform-values [topic-entity-name oldest-processed-message-in-s stream-builder]
  (let [service-name     (:app-name (ziggurat-config))
        metric-namespaces [service-name topic-entity-name "message-received-delay-histogram"]
        additional-tags  {:topic_name topic-entity-name}]
    (.transform stream-builder (timestamp-transformer-supplier metric-namespaces oldest-processed-message-in-s additional-tags) (into-array [(.name (store-supplier-builder))]))))

(defn- header-transform-values [stream-builder]
  (.transformValues stream-builder (header-transformer-supplier) (into-array [(.name (store-supplier-builder))])))

(defn- traced-handler-fn [handler-fn channels message topic-entity]
  (let [parent-ctx (TracingKafkaUtils/extractSpanContext (:headers message) tracer)
        span (as-> tracer t
               (.buildSpan t "Message-Handler")
               (.withTag t (.getKey Tags/SPAN_KIND) Tags/SPAN_KIND_CONSUMER)
               (.withTag t (.getKey Tags/COMPONENT) "ziggurat")
               (if (nil? parent-ctx)
                 t
                 (.asChildOf t parent-ctx))
               (.start t))]
    (try
      ((mapper-func handler-fn channels) (assoc (->MessagePayload (:value message) topic-entity) :headers (:headers message)))
      (finally
        (.finish span)))))

(defn- topology [handler-fn {:keys [origin-topic oldest-processed-message-in-s]} topic-entity channels]
  (let [builder           (StreamsBuilder.)
        topic-entity-name (name topic-entity)
        topic-pattern     (Pattern/compile origin-topic)]
    (.addStateStore builder (store-supplier-builder))
    (->> (.stream builder topic-pattern)
         (timestamp-transform-values topic-entity-name oldest-processed-message-in-s)
         (header-transform-values)
         (map-values #(log-and-report-metrics topic-entity-name %))
         (map-values #(traced-handler-fn handler-fn channels % topic-entity)))
    (.build builder)))

(defn- start-stream* [handler-fn stream-config topic-entity channels]
  (KafkaStreams. ^Topology (topology handler-fn stream-config topic-entity channels)
                 ^Properties (properties stream-config)
                 (new TracingKafkaClientSupplier tracer)))

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
