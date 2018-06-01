(ns ziggurat.streams-test
  (:require [clojure.test :refer :all])
  (:require [ziggurat.streams :refer [start-streams]]
            [ziggurat.config :as config]
            [clojure.tools.logging :as log]
            [ziggurat.fixtures :as fix]
            [flatland.protobuf.core :as proto]
            [clojure.string :as s]
            [ziggurat.streams :as streams]
            [ziggurat.kafka-delay :as kafka-delay]
            [lambda-common.metrics :as metrics])
  (:import [com.gojek.esb.booking BookingLogMessage BookingLogKey]
           [java.time Instant]
           [org.apache.kafka.clients.producer ProducerRecord KafkaProducer ProducerConfig]
           [java.util Properties]))

(use-fixtures :once fix/mount-only-config)

(def proto-log-type (proto/protodef BookingLogMessage))
(def proto-key-type (proto/protodef BookingLogKey))

(defn event-timestamp->proto-ts [^Instant ts]
  {:seconds (.getEpochSecond ts)
   :nanos   (.getNano ts)})

(defn- ->hash-map [proto-type message]
  (select-keys message (keys (:fields (proto/protobuf-schema proto-type)))))

(defn log->hash-map [message]
  (->hash-map proto-log-type message))

(defn key->hash-map [message]
  (->hash-map proto-key-type message))

(defn message->proto [message now]
  (let [message (log->hash-map message)]
    (try
      (proto/protobuf-dump proto-log-type
                           (-> message
                               (update :status #(s/upper-case (name %)))
                               (assoc :event-timestamp (event-timestamp->proto-ts now))))
      (catch Exception e
        (log/warn e "Failed to encode message to proto" message)))))

(defn construct-key-from-value [value now]
  (let [key (-> value
                key->hash-map
                (assoc :event-timestamp (event-timestamp->proto-ts now))
                (update :status #(s/upper-case (name %))))]
    (try
      (proto/protobuf-dump proto-key-type key)
      (catch Exception e
        (log/warn e "Failed to encode key to proto" key)))))

(defn props []
  (let [{:keys [bootstrap-servers
                acks
                enable-idempotence
                retries
                max-in-flight-requests-per-connection] :as kafka-config} (:test-kafka-producer-config config/config)]
    (doto (Properties.)
      (.setProperty ProducerConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap-servers)
      (.setProperty ProducerConfig/ACKS_CONFIG acks)
      (.setProperty ProducerConfig/RETRIES_CONFIG (str retries))
      (.setProperty ProducerConfig/MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION
                    (str max-in-flight-requests-per-connection))
      (.setProperty ProducerConfig/ENABLE_IDEMPOTENCE_CONFIG (str enable-idempotence))
      (.setProperty ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG
                    "org.apache.kafka.common.serialization.ByteArraySerializer")
      (.setProperty ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG
                    "org.apache.kafka.common.serialization.ByteArraySerializer"))))


(defn publish [message topic]
  (try
    (let [producer (KafkaProducer. (props))
          now (Instant/now)
          key (construct-key-from-value message now)
          proto-message (message->proto message now)]
      (let [future-record-metadata (.send producer (ProducerRecord. topic key proto-message))]
        (.get future-record-metadata))
      (.close producer))
    (catch Exception e
      (log/warn e "Failure in publishing message to kafka" message))))

(defn is-mapper-fn-called? [mapper-fn-called timeout]
  (cond
    @mapper-fn-called true
    (> timeout 0) (do (Thread/sleep 10000)
                      (recur mapper-fn-called (dec timeout)))
    (= timeout 0) false))

(deftest stream-test
  (testing "single stream"
    (let [delay-called-with-correct-namespace (atom false)
          message-count-called-with-correct-namespace (atom false)]
      (with-redefs [kafka-delay/calculate-and-report-kafka-delay (fn [metric-ns _]
                                                                   (if (= metric-ns "message-received-delay-histogram")
                                                                     (reset! delay-called-with-correct-namespace true)))
                    metrics/increment-count (fn [metric-ns _]
                                              (if (= metric-ns "message")
                                                (reset! message-count-called-with-correct-namespace true)))]
        (let [expected-message {:order-number "R-1"
                                :status       :created}
              mapper-fn-called (atom false)
              correct-message-recieved (atom false)
              mapper-fn (fn [actual-message]
                          (reset! mapper-fn-called true)
                          (if (and (= (:order-number expected-message) (:order-number actual-message)) (= (:status expected-message) (:status actual-message)))
                            (reset! correct-message-recieved true))
                          :success)
              streams (streams/start-streams {:mapper-fn mapper-fn})]
          (publish expected-message "booking-log")
          (is (is-mapper-fn-called? mapper-fn-called 5))
          (streams/stop-streams streams)
          (is @correct-message-recieved)
          (is @delay-called-with-correct-namespace)
          (is @message-count-called-with-correct-namespace))))))

(deftest stream-router-test
  (testing "multiple stream"
    (let [delay-called-with-correct-namespace (atom false)
          message-count-called-with-correct-namespace (atom false)]
      (with-redefs [kafka-delay/calculate-and-report-kafka-delay (fn [metric-ns _]
                                                                   (if (= metric-ns "booking.message-received-delay-histogram")
                                                                     (reset! delay-called-with-correct-namespace true)))
                    metrics/increment-count (fn [metric-ns _]
                                              (if (= metric-ns "booking.message")
                                                (reset! message-count-called-with-correct-namespace true)))]
        (let [expected-message {:order-number "R-1"
                                :status       :created}
              booking-mapper-fn-called (atom false)
              booking-correct-message-recieved (atom false)
              booking-mapper-fn (fn [actual-message]
                                  (reset! booking-mapper-fn-called true)
                                  (if (and (= (:order-number expected-message) (:order-number actual-message)) (= (:status expected-message) (:status actual-message)))
                                    (reset! booking-correct-message-recieved true))
                                  :success)
              stream-routes [{:booking {:mapper-fn booking-mapper-fn}}]
              streams (streams/start-streams {:stream-routes stream-routes})]
          (publish expected-message "booking-log")
          (is (is-mapper-fn-called? booking-mapper-fn-called 5))
          (is @booking-correct-message-recieved)
          (is @delay-called-with-correct-namespace)
          (is @message-count-called-with-correct-namespace)
          (streams/stop-streams streams))))))