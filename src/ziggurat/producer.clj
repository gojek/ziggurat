(ns ziggurat.producer
  "This namespace defines methods for publishing data to
   Kafka topics. The methods defined here are essentially wrapper
   around variants of `send` methods defined in
   `org.apache.kafka.clients.producer.KafkaProducer`.

   Users of Ziggurat can use methods defined here to send data
   to Kafka topics.

   Constructs an instance of `org.apache.kafka.clients.producer.KafkaProducer` using config values
   provided in `resources/config.edn`. Producer configs currently supported in Ziggurat are
   mentioned below.

   - bootstrap.servers
   - acks
   - retries
   - key.serializer
   - value.serializer
   - max.in.flight.requests.per.connection
   - enable.idempotencecd

   Please see [Producer configs](http://kafka.apache.org/documentation.html#producerconfigs)
   for a complete list of all producer configs available in Kafka."

  (:require [ziggurat.config :refer [ziggurat-config]]
            [clojure.tools.logging :as log]
            [mount.core :refer [defstate]])
  (:import (org.apache.kafka.clients.producer KafkaProducer ProducerRecord ProducerConfig)
           (java.util Properties)))

(defn producer-config [] (get-in (ziggurat-config) [:producer :default]))

(defn producer-config-properties []
  (let [bootstrap-servers (:bootstrap-servers (producer-config))
        acks (:acks (producer-config))
        key-serializer-class (:key-serializer (producer-config))
        value-serializer-class (:value-serializer (producer-config))
        enable-idempotence (:enable-idempotence (producer-config))
        retries-config (int (:retries-config (producer-config)))
        max-in-flight-requests-per-connection (int (:max-in-flight-requests-per-connection (producer-config)))]
    (doto (Properties.)
      (.put ProducerConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap-servers)
      (.put ProducerConfig/ACKS_CONFIG acks)
      (.put ProducerConfig/RETRIES_CONFIG retries-config)
      (.put ProducerConfig/ENABLE_IDEMPOTENCE_CONFIG enable-idempotence)
      (.put ProducerConfig/MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION max-in-flight-requests-per-connection)
      (.put ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG key-serializer-class)
      (.put ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG value-serializer-class))))

(defstate kafka-producer
  :start (KafkaProducer. (producer-config-properties))
  :stop (doto kafka-producer)
  (.flush)
  (.close))

(defn send
  "A wrapper around `org.apache.kafka.clients.producer.KafkaProducer#send` which enables
  the users of ziggurat to produce data to a Kafka topic using a Kafka producer.

  Please see `producer` for the list of config currently supported in ziggurat.

  E.g.
  (send \"test-topic\" \"value\")
  (send \"test-topic\" 1 \"key\" \"value\")
  "

  ([topic data]
   (let [producer-record (ProducerRecord. topic data)]
     (.send kafka-producer producer-record)))

  ([topic partition key data]
   (let [producer-record (ProducerRecord. topic (int partition) key data)]
     (.send kafka-producer producer-record))))





