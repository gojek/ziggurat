(ns ziggurat.kafka-consumer.consumer
  (:require [clojure.tools.logging :as log]
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
                                           (with-manual-commit-config))
          consumer                     (KafkaConsumer.
                                        (cfg/build-consumer-config-properties merged-consumer-group-config))
          topic-pattern                (Pattern/compile (:origin-topic merged-consumer-group-config))]
      (.subscribe consumer topic-pattern)
      consumer)
    (catch Exception e
      (log/error e "Exception received while creating Kafka Consumer for: " topic-entity))))
