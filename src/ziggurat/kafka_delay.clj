(ns ziggurat.kafka-delay
  (:require [lambda-common.metrics :as metrics])
  (:import [java.time Instant]))

(defn- get-millis [seconds nano-seconds]
  (let [second-in-millis (* seconds 1000)
        nanos-in-millis  (/ (or nano-seconds 1000000) 1000000)]
    (+ second-in-millis nanos-in-millis)))

(defn get-current-time-in-millis []
  (.toEpochMilli (Instant/now)))

(defn calculate-and-report-kafka-delay [metric-namespace message]
  (let [now-millis (get-current-time-in-millis)
        seconds    (-> message :event-timestamp :seconds)
        nanos      (-> message :event-timestamp :nanos)
        delay      (- now-millis
                      (get-millis seconds nanos))]
    (metrics/report-time metric-namespace delay)))
