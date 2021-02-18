(ns ziggurat.kafka-consumer.consumer
  (:require [clojure.tools.logging :as log]
            [ziggurat.kafka-consumer.consumer-handler :refer :all]
            [ziggurat.util.map :as umap])
  (:import (java.util Map Properties)
           (org.apache.kafka.clients.consumer KafkaConsumer ConsumerConfig)
           (java.util.regex Pattern)))

(def default-consumer-config
  {:commit-interval-ms 15000
   :max-poll-records 500
   :session-timeout-ms-config 60000
   :key-deserializer-class-config "org.apache.kafka.common.serialization.ByteArrayDeserializer"
   :value-deserializer-class-config "org.apache.kafka.common.serialization.ByteArrayDeserializer"})

(defn- build-consumer-properties-map
  [{:keys [bootstrap-servers
           consumer-group-id
           max-poll-records
           session-timeout-ms-config
           commit-interval-ms
           key-deserializer-class-config
           value-deserializer-class-config]}]
  (doto (Properties.)
    (.putAll {ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG        bootstrap-servers
              ConsumerConfig/GROUP_ID_CONFIG                 consumer-group-id
              ConsumerConfig/MAX_POLL_RECORDS_CONFIG         (int max-poll-records)
              ConsumerConfig/SESSION_TIMEOUT_MS_CONFIG       (int session-timeout-ms-config)
              ConsumerConfig/ENABLE_AUTO_COMMIT_CONFIG       true
              ConsumerConfig/AUTO_COMMIT_INTERVAL_MS_CONFIG  (int commit-interval-ms)
              ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG   key-deserializer-class-config
              ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG value-deserializer-class-config})))
(defn create-consumer
  [topic-entity consumer-group-config]
  (try
    (let [merged-consumer-group-config (umap/deep-merge consumer-group-config default-consumer-config)
          consumer (KafkaConsumer. ^Map (build-consumer-properties-map merged-consumer-group-config))
          topic-pattern (Pattern/compile (:origin-topic merged-consumer-group-config))]
      (.subscribe consumer topic-pattern)
      consumer)
    (catch Exception e
      (log/error e "Exception received while creating Kafka Consumer for: " topic-entity))))

