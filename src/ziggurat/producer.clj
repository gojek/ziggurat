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

   ```
     :stream-router {:default {:application-id \"test\"\n
                               :bootstrap-servers    \"localhost:9092\"\n
                               :stream-threads-count [1 :int]\n
                               :origin-topic         \"topic\"\n
                               :channels             {:channel-1 {:worker-count [10 :int]\n
                                                                  :retry {:count [5 :int]\n
                                                                          :enabled [true :bool]}}}\n
                               :producer             {:bootstrap-servers \"localhost:9092\"\n
                                                      :acks \"all\"\n
                                                      :retries-config  5\n
                                                      :max-in-flight-requests-per-connection 5\n
                                                      :enable-idempotence  false\n
                                                      :value-serializer  \"org.apache.kafka.common.serialization.StringSerializer\"\n
                                                      :key-serializer    \"org.apache.kafka.common.serialization.StringSerializer\"}}
   ```

   Please see the documentation for `send` for publishing data via Kafka producers

   These are the KafkaProducer configs currenlty supported in Ziggurat.
   - bootstrap.servers
   - acks
   - retries
   - key.serializer
   - value.serializer
   - max.in.flight.requests.per.connection
   - enable.idempotence

   Please see [producer configs](http://kafka.apache.org/documentation.html#producerconfigs)
   for a complete list of all producer configs available in Kafka."
  (:refer-clojure :exclude [send])
  (:require [clojure.tools.logging :as log]
            [mount.core :refer [defstate]]
            [ziggurat.config :refer [build-producer-config-properties ziggurat-config]]
            [ziggurat.tracer :refer [tracer]]
            [ziggurat.util.java-util :refer [get-key]]
            [ziggurat.ssl.properties :as ssl-properties])
  (:import (io.opentracing.contrib.kafka TracingKafkaProducer)
           (org.apache.kafka.clients.producer KafkaProducer ProducerRecord))
  (:gen-class
   :methods [^{:static true} [send [String String Object Object] java.util.concurrent.Future]
             ^{:static true} [send [String String int Object Object] java.util.concurrent.Future]]
   :name tech.gojek.ziggurat.internal.Producer))

(defn producer-properties-map []
  (reduce (fn [producer-map [stream-config-key stream-config]]
            (let [producer-config (:producer stream-config)]
              (if (some? producer-config)
                (assoc producer-map stream-config-key (ssl-properties/build-ssl-properties (build-producer-config-properties producer-config)))
                producer-map)))
          {}
          (seq (:stream-router (ziggurat-config)))))

(declare kafka-producers)

(defstate kafka-producers
  :start (if (not-empty (producer-properties-map))
           (do (log/info "Starting Kafka producers ...")
               (reduce (fn [producers [stream-config-key properties]]
                         (log/debug "Constructing Kafka producer associated with [" stream-config-key "] ")
                         (let [kp  (KafkaProducer. properties)
                               tkp (TracingKafkaProducer. kp tracer)]
                           (assoc producers stream-config-key tkp)))
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
          (log/info "No producers found.n Can not initiate stop.")))

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

  ([stream-config-key topic partition key   value]
   (if (some? (get kafka-producers stream-config-key))
     (let [producer-record (ProducerRecord. topic partition key value)]
       (.send (stream-config-key kafka-producers) producer-record))

     (let [error-msg (str "Can't publish data. No producers defined for stream config [" stream-config-key "]")]
       (log/error error-msg)
       (throw (ex-info error-msg {:stream-config-key stream-config-key}))))))

(defn -send
  ([stream-config-key topic key value]
   (send (get-key stream-config-key) topic key value))

  ([stream-config-key topic partition key value]
   (send (get-key stream-config-key) topic partition key value)))
