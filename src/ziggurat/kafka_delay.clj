(ns ziggurat.kafka-delay
  (:require [lambda-common.metrics :as metrics])
  (:import [java.time Instant]
           [org.apache.kafka.streams KeyValue]
           [org.apache.kafka.streams.kstream Transformer]
           [org.apache.kafka.streams.processor ProcessorContext]))

(defn- get-current-time-in-millis []
  (.toEpochMilli (Instant/now)))

(defn calculate-and-report-kafka-delay [metric-namespace record-timestamp]
  (let [now-millis (get-current-time-in-millis)
        delay      (- now-millis
                      record-timestamp)]
    (metrics/report-time metric-namespace delay)))



(deftype TimestampTransformers [^{:volatile-mutable true} processor-context metric-namespace] Transformer
  (^void init [this ^ProcessorContext context]
    (do (set! processor-context context)
        nil))
  (transform [this record-key record-value]
    (do (calculate-and-report-kafka-delay metric-namespace (.timestamp processor-context))
        (KeyValue/pair record-key record-value)))
  (punctuate [this ms] nil)
  (close [this] nil))

(defn create-transformer [metric-namespace]
  (TimestampTransformers. nil metric-namespace))