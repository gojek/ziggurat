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
   - enable.idempotencecd

   Please see [producer configs](http://kafka.apache.org/documentation.html#producerconfigs)
   for a complete list of all producer configs available in Kafka."

  (:require [ziggurat.config :refer [ziggurat-config]]
            [clojure.tools.logging :as log]
            [mount.core :refer [defstate]]
            [camel-snake-kebab.core :as csk]
            [ziggurat.tracer :refer [tracer]]
            [ziggurat.util.java-util :refer [get-key]]
            [schema.core :as s])
  (:import (org.apache.kafka.clients.producer KafkaProducer ProducerRecord ProducerConfig)
           (java.util Properties)
           (io.opentracing.contrib.kafka TracingKafkaProducer))
  (:gen-class
   :name tech.gojek.ziggurat.internal.Producer
   :methods  [^{:static true} [send [String String Object Object] java.util.concurrent.Future]
              ^{:static true} [send [String String int Object Object] java.util.concurrent.Future]]))

(defn *implements-serializer?* [serializer-class]
  (contains? (set (.getInterfaces (Class/forName serializer-class)))
             (Class/forName "org.apache.kafka.common.serialization.Serializer")))

(def implements-serializer? (s/pred *implements-serializer?* 'implements-serializer?))

(s/defschema ProducerConfigSchema {(s/required-key :bootstrap-servers)                     s/Any
                                   (s/optional-key :key-serializer-class)                  implements-serializer?
                                   (s/optional-key :value-serializer-class)                implements-serializer?
                                   (s/optional-key :key-serializer)                        implements-serializer?
                                   (s/optional-key :value-serializer)                      implements-serializer?
                                   (s/optional-key :retries-config)                        s/Any
                                   (s/optional-key :metadata-max-age)                      s/Any
                                   (s/optional-key :reconnect-backoff-ms)                  s/Any
                                   (s/optional-key :client-id)                             s/Any
                                   (s/optional-key :metric-num-samples)                    s/Any
                                   (s/optional-key :transaction-timeout)                   s/Any
                                   (s/optional-key :retries)                               s/Any
                                   (s/optional-key :retry-backoff-ms)                      s/Any
                                   (s/optional-key :receive-buffer)                        s/Any
                                   (s/optional-key :partitioner-class)                     s/Any
                                   (s/optional-key :max-block-ms)                          s/Any
                                   (s/optional-key :metrics-reporter-classes)              s/Any
                                   (s/optional-key :compression-type)                      s/Any
                                   (s/optional-key :max-request-size)                      s/Any
                                   (s/optional-key :delivery-timeout-ms)                   s/Any
                                   (s/optional-key :metrics-sample-window-ms)              s/Any
                                   (s/optional-key :request-timeout-ms)                    s/Any
                                   (s/optional-key :buffer-memory)                         s/Any
                                   (s/optional-key :interceptor-classes)                   s/Any
                                   (s/optional-key :linger-ms)                             s/Any
                                   (s/optional-key :connections-max-idle-ms)               s/Any
                                   (s/optional-key :acks)                                  s/Any
                                   (s/optional-key :enable-idempotence)                    s/Any
                                   (s/optional-key :metrics-recording-level)               s/Any
                                   (s/optional-key :transactional-id)                      s/Any
                                   (s/optional-key :reconnect-backoff-max-ms)              s/Any
                                   (s/optional-key :client-dns-lookup)                     s/Any
                                   (s/optional-key :max-in-flight-requests-per-connection) s/Any
                                   (s/optional-key :send-buffer)                           s/Any
                                   (s/optional-key :batch-size)                            s/Any})

(def valid-configs? (partial s/validate ProducerConfigSchema))

(def explain-str (partial s/explain ProducerConfigSchema))

(defn property->fn [field-name]
  (let [raw-field-name (condp = field-name
                         :max-in-flight-requests-per-connection "org.apache.kafka.clients.producer.ProducerConfig/%s"
                         :retries-config "org.apache.kafka.clients.producer.ProducerConfig/RETRIES_CONFIG"
                         :key-serializer "org.apache.kafka.clients.producer.ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG"
                         :value-serializer "org.apache.kafka.clients.producer.ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG"
                         "org.apache.kafka.clients.producer.ProducerConfig/%s_CONFIG")]
    (->> field-name
         csk/->SCREAMING_SNAKE_CASE_STRING
         (format raw-field-name)
         (read-string))))

(defn producer-properties [config-map]
  (if (valid-configs? config-map)
    (let [props (Properties.)]
      (doseq [config-key (keys config-map)]
        (.setProperty props
                      (eval (property->fn config-key))
                      (str (get config-map config-key))))
      props)
    (throw (ex-info (explain-str config-map) config-map))))

(defn producer-properties-map []
  (reduce (fn [producer-map [stream-config-key stream-config]]
            (let [producer-config (:producer stream-config)]
              (if (some? producer-config)
                (assoc producer-map stream-config-key (producer-properties producer-config))
                producer-map)))
          {}
          (seq (:stream-router (ziggurat-config)))))

(defstate kafka-producers
  :start (if (not-empty (producer-properties-map))
           (do (log/info "Starting Kafka producers ...")
               (reduce (fn [producers [stream-config-key properties]]
                         (do (log/debug "Constructing Kafka producer associated with [" stream-config-key "] ")
                             (let [_ (println properties)
                                   kp  (KafkaProducer. properties)
                                   _   (println kp)
                                   tkp (TracingKafkaProducer. kp tracer)]
                               (assoc producers stream-config-key tkp))))
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

  ([stream-config-key topic partition key value]
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
