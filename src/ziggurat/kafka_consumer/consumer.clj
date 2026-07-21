(ns ziggurat.kafka-consumer.consumer
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [ziggurat.config :as cfg]
            [ziggurat.util.map :as umap])
  (:import (java.util.regex Pattern)
           (org.apache.kafka.clients.consumer KafkaConsumer)))

(def default-consumer-config
  {:commit-interval-ms              15000
   :session-timeout-ms-config       60000
   :max-poll-interval-ms            300000
   :default-api-timeout-ms-config   60000
   :key-deserializer-class-config   "org.apache.kafka.common.serialization.ByteArrayDeserializer"
   :value-deserializer-class-config "org.apache.kafka.common.serialization.ByteArrayDeserializer"})

(def partition-assignment-strategy-classes
  "Friendly batch-route strategy names mapped to their fully-qualified Kafka assignor
   class names. Add new assignors here as Kafka ships them; callers may also pass a
   fully-qualified class name directly (see `resolve-partition-assignment-strategy`),
   so this map is a convenience layer, not an exhaustive allow-list."
  {:range              "org.apache.kafka.clients.consumer.RangeAssignor"
   :round-robin        "org.apache.kafka.clients.consumer.RoundRobinAssignor"
   :sticky             "org.apache.kafka.clients.consumer.StickyAssignor"
   :cooperative-sticky "org.apache.kafka.clients.consumer.CooperativeStickyAssignor"})

(defn- resolve-strategy-class
  "Resolves a single strategy to a Kafka assignor class name. Accepts a known short name
   (keyword or string, e.g. :cooperative-sticky) or an already fully-qualified class name
   (any string containing a '.'), which is returned unchanged so assignors not listed in
   `partition-assignment-strategy-classes` still work without a code change. Throws on an
   unrecognised short name so misconfiguration fails loudly at startup."
  [strategy]
  (let [strategy-key (keyword strategy)]
    (cond
      (contains? partition-assignment-strategy-classes strategy-key)
      (get partition-assignment-strategy-classes strategy-key)

      (and (string? strategy) (str/includes? strategy "."))
      strategy

      :else
      (throw (ex-info "Invalid :partition-assignment-strategy for batch route"
                      {:provided          strategy
                       :valid-short-names (vec (keys partition-assignment-strategy-classes))})))))

(defn- resolve-partition-assignment-strategy
  "Resolves the batch-route `:partition-assignment-strategy` value into the Kafka
   `partition.assignment.strategy` property string. Accepts either a single strategy or an
   ordered collection of strategies; a collection is joined with ',' to form Kafka's
   preference list, which is required for the two-phase eager -> cooperative rolling
   upgrade (phase 1: [:range :cooperative-sticky], phase 2: :cooperative-sticky).
   See doc/kafka_produce_consume.md."
  [strategy]
  (let [strategies (if (sequential? strategy) strategy [strategy])]
    (when (empty? strategies)
      (throw (ex-info "Empty :partition-assignment-strategy for batch route"
                      {:provided strategy})))
    (str/join "," (map resolve-strategy-class strategies))))

(defn- with-partition-assignment-strategy-config
  "Opt-in feature flag: when `:partition-assignment-strategy` is set on the batch route it is
   resolved to the Kafka `partition.assignment.strategy` property. When the flag is absent (or
   nil) the config is returned unchanged, so the Kafka client default (RangeAssignor / eager
   rebalancing) applies and existing consumers are unaffected."
  [consumer-config]
  (if-some [strategy (:partition-assignment-strategy consumer-config)]
    (assoc consumer-config
           :partition-assignment-strategy-config (resolve-partition-assignment-strategy strategy))
    consumer-config))

(defn- with-manual-commit-config
  "When `:manual-commit-enabled` is set on the batch route, Kafka's background auto-commit
   is turned off so offsets are committed by the consumer-handler only after a batch has
   been processed. Without the flag the config is returned unchanged, preserving the
   existing auto-commit behaviour."
  [consumer-config]
  (cond-> consumer-config
    (:manual-commit-enabled consumer-config) (assoc :enable-auto-commit false)))

(defn create-consumer
  [topic-entity consumer-group-config]
  (try
    (let [merged-consumer-group-config (-> consumer-group-config
                                           (umap/deep-merge default-consumer-config)
                                           (with-manual-commit-config)
                                           (with-partition-assignment-strategy-config))
          consumer                     (KafkaConsumer.
                                        (cfg/build-consumer-config-properties merged-consumer-group-config))
          topic-pattern                (Pattern/compile (:origin-topic merged-consumer-group-config))]
      (.subscribe consumer topic-pattern)
      consumer)
    (catch Exception e
      (log/error e "Exception received while creating Kafka Consumer for: " topic-entity))))
