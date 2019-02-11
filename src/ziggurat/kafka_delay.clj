(ns ziggurat.kafka-delay
  (:require [ziggurat.metrics :as metrics])
  (:import [java.time Instant]
           [org.apache.kafka.streams KeyValue]
           [org.apache.kafka.streams.kstream Transformer]
           [org.apache.kafka.streams.processor TimestampExtractor ProcessorContext]))

(defn get-timestamp-from-record [record]
  (.timestamp record))

(defn get-current-time-in-millis []
  (.toEpochMilli (Instant/now)))

(deftype IngestionTimeExtractor [] TimestampExtractor
         (extract [_ record _]
           (let [ingestion-time (get-timestamp-from-record record)]
             (if (< ingestion-time 0)
               (get-current-time-in-millis)
               ingestion-time))))

(defn calculate-and-report-kafka-delay [metric-namespace record-timestamp]
  (let [now-millis (get-current-time-in-millis)
        delay      (- now-millis
                      record-timestamp)]
    (metrics/report-time metric-namespace delay)))

(deftype TimestampTransformers [^{:volatile-mutable true} processor-context metric-namespace] Transformer
         (^void init [_ ^ProcessorContext context]
           (do (set! processor-context context)
               nil))
         (transform [_ record-key record-value]
           (do (calculate-and-report-kafka-delay metric-namespace (.timestamp processor-context))
               (KeyValue/pair record-key record-value)))
         (punctuate [_ _] nil)
         (close [_] nil))

(defn create-transformer [metric-namespace]
  (TimestampTransformers. nil metric-namespace))