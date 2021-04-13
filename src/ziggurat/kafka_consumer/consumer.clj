(ns ziggurat.kafka-consumer.consumer
  (:require [clojure.tools.logging :as log]
            [ziggurat.config :as cfg]
            [ziggurat.util.map :as umap])
  (:import (java.util.regex Pattern)
           (org.apache.kafka.clients.consumer KafkaConsumer)))

(def default-consumer-config
  {:commit-interval-ms              15000
   :session-timeout-ms-config       60000
   :default-api-timeout-ms-config   60000
   :key-deserializer-class-config   "org.apache.kafka.common.serialization.ByteArrayDeserializer"
   :value-deserializer-class-config "org.apache.kafka.common.serialization.ByteArrayDeserializer"})

(defn create-consumer
  [topic-entity consumer-group-config]
  (try
    (let [merged-consumer-group-config (umap/deep-merge consumer-group-config default-consumer-config)
          consumer                     (KafkaConsumer. (cfg/build-consumer-config-properties merged-consumer-group-config))
          topic-pattern                (Pattern/compile (:origin-topic merged-consumer-group-config))]
      (.subscribe consumer topic-pattern)
      consumer)
    (catch Exception e
      (log/error e "Exception received while creating Kafka Consumer for: " topic-entity))))
