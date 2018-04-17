(ns ziggurat.kafka-delay
  (:require [lambda-common.metrics :as metrics])
  (:import [java.time Instant]))

(defn- get-millis [seconds nano-seconds]
  (let [second-in-millis (* seconds 1000)
        nanos-in-millis  (/ (or nano-seconds 1000000) 1000000)]
    (+ second-in-millis nanos-in-millis)))

(defn calculate-and-report-kafka-delay [booking]
  (let [now-millis (.toEpochMilli (Instant/now))
        seconds    (-> booking :event-timestamp :seconds)
        nanos      (-> booking :event-timestamp :nanos)
        delay      (- now-millis
                      (get-millis seconds nanos))]
    (metrics/message-received-delay delay)))
