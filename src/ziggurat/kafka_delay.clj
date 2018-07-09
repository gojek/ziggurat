(ns ziggurat.kafka-delay
  (:require [lambda-common.metrics :as metrics])
  (:import [java.time Instant]
           [org.apache.kafka.streams KeyValue]
           [org.apache.kafka.streams.kstream Transformer]
           [org.apache.kafka.streams.processor ProcessorContext]))

(defn get-current-time-in-millis []
  (.toEpochMilli (Instant/now)))

(defn calculate-and-report-kafka-delay [metric-namespace recordTimestamp]
  (let [now-millis (get-current-time-in-millis)
        delay      (- now-millis
                      recordTimestamp)]
    (metrics/report-time metric-namespace delay)))



(deftype TimestampTransformers [^{:volatile-mutable true} processor-context metric-namespace] Transformer
  (^void init [this ^ProcessorContext context]
    (do (set! processor-context context)
        nil))
  (transform [this recordKey recordValue]
    (do (calculate-and-report-kafka-delay metric-namespace (.timestamp processor-context))
        (KeyValue/pair recordKey recordValue)))
  (punctuate [this ms] nil)
  (close [this] nil))

(defn create-transformer [metric-namespace]
  (TimestampTransformers. nil metric-namespace))