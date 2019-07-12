(ns ziggurat.producer
  "This namespace defines methods for publishing data to
   Kafka topics. The methods defined here are essentially wrapper
   around variants of `send` methods defined in
   `org.apache.kafka.clients.producer.KafkaProducer`.

   At the time of initialization, an instance of
   `org.apache.kafka.clients.producer.KafkaProducer`
   is constructed using config values provided in `resources/config.edn`.

   A producer can be configured for each of the stream-routes
   in config.edn. Please see the example below

   `
     :stream-router {:default {:application-id \"test\"\n
                               :bootstrap-servers    \"localhost:9092\"\n
                               :stream-threads-count [1 :int]\n
                               :origin-topic         \"topic\"\n
                               :channels             {:channel-1 {:worker-count [10 :int]\n  :retry {:count [5 :int]\n  :enabled [true :bool]}}}\n
                               :producer             {:bootstrap-servers \"localhost:9092\"\n
                                                      :acks \"all\"\n
                                                      :retries-config  5\n
                                                      :max-in-flight-requests-per-connection 5\n
                                                      :enable-idempotence  false\n
                                                      :value-serializer  \"org.apache.kafka.common.serialization.StringSerializer\"\n
                                                      :key-serializer    \"org.apache.kafka.common.serialization.StringSerializer\"}}
   `

   Usage:

   `
      Please see `send` for publishing data via Kafka producers
   `

   These are the KafkaProducer configs currenlty supported in Ziggurat.
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
           (java.util Properties))
  (:gen-class
    :name tech.gojek.ziggurat.Producer
    :methods [^{:static true} [send [String String int Object Object] void]
              ^{:static true} [send [clojure.lang.Keyword String int String String] void]
              ^{:static true} [send [String String Object Object] void]
              ^{:static true} [send [clojure.lang.Keyword String String String] void]]))

(defn- producer-properties-from-config [{:keys [bootstrap-servers
                                                acks
                                                key-serializer
                                                value-serializer
                                                enable-idempotence
                                                retries-config
                                                max-in-flight-requests-per-connection]}]
  (doto (Properties.)
    (.put ProducerConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap-servers)
    (.put ProducerConfig/ACKS_CONFIG acks)
    (.put ProducerConfig/RETRIES_CONFIG (int retries-config))
    (.put ProducerConfig/ENABLE_IDEMPOTENCE_CONFIG enable-idempotence)
    (.put ProducerConfig/MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION (int max-in-flight-requests-per-connection))
    (.put ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG key-serializer)
    (.put ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG value-serializer)))

(defn producer-properties-map []
  (reduce (fn [producer-map [stream-config-key stream-config]]
            (let [producer-config (:producer stream-config)]
              (if (some? producer-config)
                (assoc producer-map stream-config-key (producer-properties-from-config producer-config))
                producer-map)))
          {}
          (seq (:stream-router (ziggurat-config)))))

(defstate kafka-producers
  :start (if (not-empty (producer-properties-map))
           (do (log/info "Starting Kafka producers ...")
               (reduce (fn [producers [stream-config-key properties]]
                         (do (log/debug "Constructing Kafka producer associated with [" stream-config-key "] ")
                             (assoc producers stream-config-key (KafkaProducer. properties))))
                       {}
                       (seq (producer-properties-map))))
           (log/info "No producers found. Can not initiate start."))

  :stop (if (not-empty kafka-producers)
          (do (log/info "Stopping Kafka producers ...")
              (doall (map (fn [[stream-config-key producer]]
                            (log/debug "Stopping Kafka producer associated with [" stream-config-key "]")
                            (doto producer
                              (.flush)
                              (.close)))
                          (seq kafka-producers))))
          (log/info "No producers found. Can not initiate stop.")))

(defn send
  "A wrapper around `org.apache.kafka.clients.producer.KafkaProducer#send` which enables
  the users of Ziggurat to produce data to a Kafka topic using a Kafka producer
  associated with a Kafka stream config key.

  E.g.
  For publishing data to producer defined for the
  stream router config with defined agains
   key `:default`, use send like this.

  `(send :default \"test-topic\" \"key\" \"value\")`
  `(send :default \"test-topic\" 1 \"key\" \"value\")`

  "

  ([stream-config-key topic key value]
   (send stream-config-key topic nil key value))

  ([stream-config-key topic partition key value]
   (if (some? (get kafka-producers stream-config-key))
     (let [producer-record (ProducerRecord. topic partition key value)]
       (.send (stream-config-key kafka-producers) producer-record))

     (let [error-msg (str "Can't publish data. No producers defined for stream config [" stream-config-key "]")]
       (log/error error-msg)
       (throw (ex-info error-msg {:stream-config-key stream-config-key}))))))


(defn -send
  ([stream-config-key topic key value]
   (send stream-config-key topic key value))

  ([stream-config-key topic partition key value]
   (send stream-config-key topic partition key value)))