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

(defn- message-to-process? [message-timestamp process-message-since-in-s]
  (let [current-time (get-current-time-in-millis)
        allowed-time (- current-time (* 1000 process-message-since-in-s))]
    (> message-timestamp allowed-time)))

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

(deftype TimestampTransformers [^{:volatile-mutable true} processor-context metric-namespace process-message-since-in-s] Transformer
         (^void init [_ ^ProcessorContext context]
           (do (set! processor-context context)
               nil))
         (transform [_ record-key record-value]
           (let [message-time (.timestamp processor-context)]
             (when (message-to-process? message-time process-message-since-in-s)
               (calculate-and-report-kafka-delay metric-namespace message-time)
               (KeyValue/pair record-key record-value))))
         (punctuate [_ _] nil)
         (close [_] nil))

(defn create-transformer [metric-namespace process-message-since-in-s]
  (TimestampTransformers. nil metric-namespace process-message-since-in-s))