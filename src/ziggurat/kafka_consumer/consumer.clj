(ns ziggurat.kafka-consumer.consumer
  (:require [clojure.tools.logging :as log]
            [ziggurat.kafka-consumer.consumer-handler :refer :all])
  (:import (java.util Map Properties)
           (org.apache.kafka.clients.consumer KafkaConsumer ConsumerConfig)
           (java.util.regex Pattern)))

(defn- build-consumer-properties-map
  [consumer-group-config]
  (doto (Properties.)
    (.putAll {ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG        (:bootstrap-servers consumer-group-config)
              ConsumerConfig/GROUP_ID_CONFIG                 (:consumer-group-id consumer-group-config)
              ConsumerConfig/MAX_POLL_RECORDS_CONFIG         (int (or (:max-poll-records  consumer-group-config) 50))
              ConsumerConfig/SESSION_TIMEOUT_MS_CONFIG       (int (or (:session-timeout-ms-config  consumer-group-config) 60000))
              ConsumerConfig/ENABLE_AUTO_COMMIT_CONFIG       false
              ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG   "org.apache.kafka.common.serialization.ByteArrayDeserializer"
              ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.ByteArrayDeserializer"})))

(defn create-consumer
  [consumer-group-config]
  (try
    (let [consumer (KafkaConsumer. ^Map (build-consumer-properties-map consumer-group-config))
          topic-pattern (Pattern/compile (:origin-topic consumer-group-config))]
      (.subscribe consumer topic-pattern)
      consumer)
    (catch Exception e
      (log/error "Exception received while creating Kafka Consumer:" e))))

